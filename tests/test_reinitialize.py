"""Forced re-initialization: minimal writes, lock-free diff, snapshot fidelity.

The forced rebuild must reconcile SQLite with the JSON catalog instead of
rewriting the whole table. The comparison runs without the write lock, and
whatever commits meanwhile (a take-down or another rebuild) must not leak
into the published result: the final switch always reflects the JSON that
this request read.
"""
from __future__ import annotations

import json
import sqlite3
import threading

import pytest

from app import database

from conftest import DEFAULT_IMAGES, _img


def _seed(catalog_paths, images, **meta):
    _, write = catalog_paths
    write(images, **meta)
    return database.initialize_database()


def _rows():
    conn = sqlite3.connect(database.config.db_path())
    conn.row_factory = sqlite3.Row
    try:
        return {
            r["sheet_no"]: dict(r)
            for r in conn.execute("SELECT * FROM images ORDER BY sheet_no")
        }
    finally:
        conn.close()


def _snapshot():
    """Everything a failed rebuild must leave untouched."""
    conn = sqlite3.connect(database.config.db_path())
    conn.row_factory = sqlite3.Row
    try:
        return {
            "images": [dict(r) for r in conn.execute("SELECT * FROM images ORDER BY id")],
            "batches": [
                dict(r)
                for r in conn.execute(
                    "SELECT * FROM withdrawal_batches ORDER BY id"
                )
            ],
            "meta": {
                r["key"]: r["value"]
                for r in conn.execute("SELECT * FROM catalog_meta")
            },
        }
    finally:
        conn.close()


def _install_write_log():
    """Record every row write to ``images`` through SQLite triggers."""
    conn = sqlite3.connect(database.config.db_path())
    try:
        conn.executescript(
            """
            DROP TABLE IF EXISTS write_log;
            CREATE TABLE write_log (op TEXT NOT NULL, sheet_no TEXT NOT NULL);
            CREATE TRIGGER log_img_ins AFTER INSERT ON images
                BEGIN INSERT INTO write_log VALUES ('insert', NEW.sheet_no); END;
            CREATE TRIGGER log_img_upd AFTER UPDATE ON images
                BEGIN INSERT INTO write_log VALUES ('update', NEW.sheet_no); END;
            CREATE TRIGGER log_img_del AFTER DELETE ON images
                BEGIN INSERT INTO write_log VALUES ('delete', OLD.sheet_no); END;
            """
        )
    finally:
        conn.close()


def _write_log():
    conn = sqlite3.connect(database.config.db_path())
    try:
        return sorted(conn.execute("SELECT op, sheet_no FROM write_log").fetchall())
    finally:
        conn.close()


def _clear_write_log():
    conn = sqlite3.connect(database.config.db_path())
    try:
        conn.execute("DELETE FROM write_log")
        conn.commit()
    finally:
        conn.close()


def _withdraw(sheet):
    return database.retry_on_locked(database.withdraw_batch, [sheet])


# ---------------------------------------------------------------------------
# Minimal writes
# ---------------------------------------------------------------------------

def test_forced_rebuild_in_sync_writes_no_image_rows(catalog_paths):
    _seed(catalog_paths, DEFAULT_IMAGES)
    _install_write_log()
    before = _rows()

    result = database.initialize_database(force=True)

    assert result["changed"] is False
    assert result["changes"] == {
        "inserted": 0,
        "updated": 0,
        "reactivated": 0,
        "deleted": 0,
        "unchanged": len(DEFAULT_IMAGES),
        "audit_cleared": 0,
    }
    assert _write_log() == []
    after = _rows()
    # Row identity is preserved: untouched rows were not delete+inserted.
    assert {s: (r["id"], r["is_active"]) for s, r in before.items()} == {
        s: (r["id"], r["is_active"]) for s, r in after.items()
    }


def test_forced_rebuild_only_writes_differences(catalog_paths):
    _seed(catalog_paths, DEFAULT_IMAGES)
    _install_write_log()
    before = _rows()

    # One withdrawal so an unchanged JSON sheet has to be reactivated.
    assert _withdraw("A")["status"] == "committed"
    _clear_write_log()

    changed = [img for img in DEFAULT_IMAGES if img["sheet_no"] != "L"]
    changed = [
        {**img, "resolution_mpp": 0.2} if img["sheet_no"] == "B" else img
        for img in changed
    ]
    changed.append(_img("M", [1000, 0, 1100, 100], 0.25))
    _, write = catalog_paths
    write(changed, crs="EPSG:32651", units="us-foot")

    result = database.initialize_database(force=True)

    assert result["changed"] is True
    assert result["changes"] == {
        "inserted": 1,       # M
        "updated": 1,        # B (resolution change)
        "reactivated": 1,    # A (withdrawn, absent from audit of JSON state)
        "deleted": 1,        # L
        "unchanged": 9,      # the other sheets, including C..K
        "audit_cleared": 1,  # committed A batch
    }
    assert _write_log() == [
        ("delete", "L"),
        ("insert", "M"),
        ("update", "A"),
        ("update", "B"),
    ]

    after = _rows()
    assert "L" not in after and "M" in after
    assert after["B"]["resolution"] == 0.2
    assert after["A"]["is_active"] == 1
    assert after["A"]["id"] == before["A"]["id"]  # reactivated in place

    meta = database.catalog_metadata()
    assert meta["crs"] == "EPSG:32651"
    assert meta["units"] == "us-foot"
    # The audit trail is cleared even though only one batch existed.
    assert database.list_batches(10) == []


def test_identical_rebuild_after_withdrawal_still_reactivates(catalog_paths):
    _seed(catalog_paths, DEFAULT_IMAGES)
    _withdraw("A")
    _withdraw("K")

    result = database.initialize_database(force=True)

    assert result["changes"]["reactivated"] == 2
    assert result["changes"]["unchanged"] == len(DEFAULT_IMAGES) - 2
    active = {r.sheet_no for r in database.list_images(active_only=True)}
    assert active == {img["sheet_no"] for img in DEFAULT_IMAGES}
    assert database.list_batches(10) == []


def test_unchanged_rebuild_clears_audit_without_rewriting_images(catalog_paths):
    _seed(catalog_paths, DEFAULT_IMAGES)
    _install_write_log()
    # A rejected batch writes an audit row but changes no image.
    with pytest.raises(database.WithdrawalValidationError):
        _withdraw("D")
    assert len(database.list_batches(10)) == 1
    _clear_write_log()

    result = database.initialize_database(force=True)

    assert result["changed"] is True
    assert result["changes"]["audit_cleared"] == 1
    assert result["changes"]["unchanged"] == len(DEFAULT_IMAGES)
    assert _write_log() == []
    assert database.list_batches(10) == []


def test_metadata_only_change_does_not_touch_images(catalog_paths):
    _, write = catalog_paths
    write(DEFAULT_IMAGES, crs="EPSG:32650", units="metre")
    _seed(catalog_paths, DEFAULT_IMAGES)
    _install_write_log()
    before = _rows()

    write(DEFAULT_IMAGES, crs="EPSG:32655", units="metre")

    result = database.initialize_database(force=True)

    assert result["changes"]["inserted"] == 0
    assert result["changes"]["updated"] == 0
    assert result["changes"]["deleted"] == 0
    assert _write_log() == []
    assert database.catalog_metadata()["crs"] == "EPSG:32655"
    assert _rows() == before


# ---------------------------------------------------------------------------
# Comparison is outside the write lock
# ---------------------------------------------------------------------------

def _pause_after_first_snapshot(monkeypatch):
    """Hold the rebuild after its first lock-free diff for one round."""
    started = threading.Event()
    resume = threading.Event()
    original = database._read_catalog_rows
    calls = {"n": 0}

    def paused(conn, data_version):
        snapshot = original(conn, data_version)
        calls["n"] += 1
        if calls["n"] == 1:
            started.set()
            assert resume.wait(10), "test never released the rebuild"
        return snapshot

    monkeypatch.setattr(database, "_read_catalog_rows", paused)
    return started, resume


def test_withdrawal_commits_while_comparison_is_running(catalog_paths, monkeypatch):
    _seed(catalog_paths, DEFAULT_IMAGES)
    started, resume = _pause_after_first_snapshot(monkeypatch)

    outcome: dict[str, object] = {}

    def rebuild():
        try:
            outcome["result"] = database.initialize_database(force=True)
        except BaseException as exc:  # pragma: no cover - surfaced below
            outcome["error"] = exc

    thread = threading.Thread(target=rebuild)
    thread.start()
    assert started.wait(10)

    # The diff is in progress but holds no write lock: this must commit now.
    withdrawal = _withdraw("A")
    assert withdrawal["status"] == "committed"
    assert database.get_image("A").is_active is False

    resume.set()
    thread.join(10)
    assert not thread.is_alive()
    assert "error" not in outcome, outcome.get("error")

    # The stale plan was discarded and re-diffed: A is back on the shelf,
    # exactly as the JSON requests, and the withdrawal audit was cleared.
    assert database.get_image("A").is_active is True
    assert {r.sheet_no for r in database.list_images(active_only=True)} == {
        img["sheet_no"] for img in DEFAULT_IMAGES
    }
    assert database.list_batches(10) == []
    assert outcome["result"]["changes"]["reactivated"] == 1


def test_interleaved_rebuild_publishes_own_json_snapshot(catalog_paths, monkeypatch):
    first = DEFAULT_IMAGES
    _, write = catalog_paths
    write(first, crs="EPSG:32650", units="metre")
    _seed(catalog_paths, first)

    started, resume = _pause_after_first_snapshot(monkeypatch)
    outcome: dict[str, object] = {}

    def rebuild_first():
        try:
            outcome["result"] = database.initialize_database(force=True)
        except BaseException as exc:  # pragma: no cover - surfaced below
            outcome["error"] = exc

    thread = threading.Thread(target=rebuild_first)
    thread.start()
    assert started.wait(10)

    # A second rebuild reads a *different* JSON and commits first.
    second = [img for img in first if img["sheet_no"] != "L"]
    second.append(_img("M", [1000, 0, 1100, 100], 0.25))
    write(second, crs="EPSG:32651", units="us-foot")
    second_result = database.initialize_database(force=True)
    assert second_result["crs"] == "EPSG:32651"
    assert set(_rows()) == {img["sheet_no"] for img in second}

    resume.set()
    thread.join(10)
    assert not thread.is_alive()
    assert "error" not in outcome, outcome.get("error")

    # The first request still publishes the JSON it read: L is back, M gone,
    # CRS/units are the first catalog's.
    rows = _rows()
    assert set(rows) == {img["sheet_no"] for img in first}
    assert all(row["is_active"] == 1 for row in rows.values())
    meta = database.catalog_metadata()
    assert meta["crs"] == "EPSG:32650"
    assert meta["units"] == "metre"
    assert outcome["result"]["changes"]["inserted"] == 1  # L
    assert outcome["result"]["changes"]["deleted"] == 1   # M


def test_comparison_runs_while_another_connection_holds_write_lock(
    catalog_paths, monkeypatch
):
    """The diff takes no write lock: it completes, then waits to switch."""
    _seed(catalog_paths, DEFAULT_IMAGES)
    monkeypatch.setenv("IMAGERY_BUSY_TIMEOUT_MS", "100")

    holder = sqlite3.connect(database.config.db_path(), isolation_level=None)
    try:
        holder.execute("BEGIN IMMEDIATE")
        holder.execute("UPDATE images SET is_active = 0 WHERE sheet_no = 'A'")

        started, resume = _pause_after_first_snapshot(monkeypatch)
        outcome: dict[str, object] = {}

        def rebuild():
            try:
                outcome["result"] = database.initialize_database(force=True)
            except BaseException as exc:  # pragma: no cover - surfaced below
                outcome["error"] = exc

        thread = threading.Thread(target=rebuild)
        thread.start()
        # The snapshot finished while the write lock was held elsewhere.
        assert started.wait(10)
        resume.set()

        # A plain reader is unaffected by the diff.
        reader = sqlite3.connect(database.config.db_path(), timeout=2)
        try:
            assert reader.execute("SELECT COUNT(*) FROM images").fetchone()[0] == 12
        finally:
            reader.close()
    finally:
        holder.execute("COMMIT")
        holder.close()

    thread.join(30)
    assert not thread.is_alive()
    assert "error" not in outcome, outcome.get("error")
    # The change made under the lock was observed by the fence and corrected.
    assert outcome["result"]["changes"]["reactivated"] == 1
    assert database.get_image("A").is_active is True


def test_noop_rebuild_does_not_bump_data_version(catalog_paths):
    """An in-sync rebuild performs no write, so it wakes nobody's fence."""
    _seed(catalog_paths, DEFAULT_IMAGES)
    observer = sqlite3.connect(database.config.db_path(), isolation_level=None)
    try:
        before = observer.execute("PRAGMA data_version").fetchone()[0]
        result = database.initialize_database(force=True)
        after = observer.execute("PRAGMA data_version").fetchone()[0]
    finally:
        observer.close()
    assert result["changed"] is False
    assert before == after



@pytest.mark.parametrize(
    "broken",
    [
        {"crs": "EPSG:32650", "units": "metre", "images": "nope"},
        {
            "images": [
                _img("A", [0, 0, 10, 10], 0.5),
                _img("A", [10, 0, 20, 10], 0.5),
            ]
        },
        {"images": [{**_img("A", [0, 0, 10, 10], 0.5), "bbox": [5, 5, 5, 10]}]},
        {"images": [{**_img("A", [0, 0, 10, 10], 0.5), "resolution_mpp": 0}]},
        {"images": [{"sheet_no": "A", "bbox": [0, 0, 10, 10]}]},
        {"images": [{**_img("A", [0, 0, 10, 10], 0.5), "bbox": ["x", 0, 10, 10]}]},
    ],
)
def test_invalid_catalog_changes_nothing(client, catalog_paths, broken):
    client.post("/withdrawals", json={"sheet_nos": ["A"]})
    before = _snapshot()

    catalog_file, _ = catalog_paths
    catalog_file.write_text(json.dumps(broken), encoding="utf-8")
    resp = client.post("/admin/reinitialize", params={"force": True})

    assert resp.status_code == 400
    assert _snapshot() == before


def test_write_failure_rolls_back_everything(catalog_paths, monkeypatch):
    _seed(catalog_paths, DEFAULT_IMAGES)
    _withdraw("A")
    before = _snapshot()

    # A plan that is real (L is removed) but fails after deleting L.
    changed = [img for img in DEFAULT_IMAGES if img["sheet_no"] != "L"]
    _, write = catalog_paths
    write(changed, crs="EPSG:32651")

    original = database._apply_catalog_plan

    def failing(conn, plan, catalog):
        original(conn, plan, catalog)
        raise RuntimeError("disk gone")

    monkeypatch.setattr(database, "_apply_catalog_plan", failing)
    with pytest.raises(RuntimeError):
        database.initialize_database(force=True)

    assert _snapshot() == before
    assert database.get_image("L") is not None
    assert database.get_image("A").is_active is False
    assert len(database.list_batches(10)) == 1


# ---------------------------------------------------------------------------
# Query/API surface
# ---------------------------------------------------------------------------

def test_reinitialize_response_reports_changes(client, catalog_paths):
    client.post("/withdrawals", json={"sheet_nos": ["A"]})
    resp = client.post("/admin/reinitialize", params={"force": True})
    assert resp.status_code == 200
    body = resp.json()
    assert body["initialized"] is True
    assert body["changed"] is True
    assert body["changes"]["reactivated"] == 1
    assert body["changes"]["unchanged"] == 11
    assert body["changes"]["audit_cleared"] == 1

    again = client.post("/admin/reinitialize", params={"force": True}).json()
    assert again["changed"] is False
    assert again["changes"]["audit_cleared"] == 0


def test_metadata_follows_rebuild_and_is_exposed(client, catalog_paths):
    _, write = catalog_paths
    write(DEFAULT_IMAGES, crs="EPSG:32655", units="us-foot")
    client.post("/admin/reinitialize", params={"force": True})

    health = client.get("/health").json()
    assert health["crs"] == "EPSG:32655"
    assert health["units"] == "us-foot"
    assert health["active_images"] == len(DEFAULT_IMAGES)


def test_openapi_documents_reinitialize_response(client):
    schema = client.get("/openapi.json").json()
    op = schema["paths"]["/admin/reinitialize"]["post"]
    ok = op["responses"]["200"]
    assert ok["content"]["application/json"]["schema"]["$ref"].endswith(
        "/ReinitializeResponse"
    )
    assert "ReinitializeChanges" in schema["components"]["schemas"]
