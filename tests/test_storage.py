import base64
import json

import pytest

from app.manifest import digest


def archived(test):
    return {item["run_id"]: item for item in test.get("/api/runs").json()}


def snapshot_of(test, run_id):
    with test.app.state.runs.store.connect() as connection:
        return connection.execute("SELECT format, snapshot, pinned, packed, source_blobs FROM wafer_runs"
                                  " WHERE run_id = ?", (run_id,)).fetchone()


def test_storage_panel_renders_release_and_independence_controls(client):
    page = client.get("/runs")
    assert page.status_code == 200
    for control in ('id="compact"', 'id="storage-totals"', 'id="storage-runs"', 'id="storage-note"'):
        assert control in page.text


def test_report_surfaces_reclaimable_space_and_independence_before_touching_archive(legacy_client):
    test, store = legacy_client
    disk_before = store.disk_bytes()
    report = test.get("/api/runs/storage").json()
    totals = report["totals"]

    assert totals["pending_compaction"] == 2 and totals["reclaimable_bytes"] > 0
    assert totals["independent"] == 2 and totals["ready"] == 0
    assert all(run["independent"] and run["reclaimable_bytes"] > 0 for run in report["runs"])
    # The check is read-only: bytes on disk did not move.
    assert store.disk_bytes() == disk_before
    assert all(row[0] == "json" for row in
               (snapshot_of(test, run["run_id"]) for run in report["runs"]))


def test_compaction_reclaims_space_and_every_bundle_stays_verifiable(legacy_client):
    test, store = legacy_client
    before = {run_id: test.get(f"/api/runs/{run_id}/export").content for run_id in archived(test)}
    report = test.get("/api/runs/storage").json()["totals"]

    result = test.post("/api/runs/compact", json={}).json()
    assert result["compacted"] == 2 and result["released_bytes"] == report["reclaimable_bytes"]
    assert result["after_bytes"] < result["before_bytes"] and result["disk_after_bytes"] < result["disk_before_bytes"]

    after = test.get("/api/runs/storage").json()["totals"]
    assert after["pending_compaction"] == 0 and after["reclaimable_bytes"] == 0
    assert (after["independent"], after["ready"]) == (2, 2)
    for run_id, bundle in before.items():
        assert test.get(f"/api/runs/{run_id}/export").content == bundle
        assert test.post(f"/api/runs/{run_id}/rerun").json()["verified"] is True


def test_compacted_runs_keep_original_run_id_business_key_and_single_archive(legacy_client):
    test, store = legacy_client
    original = archived(test)
    test.post("/api/runs/compact", json={})
    assert archived(test) == original

    for sample in ("mixed", "baseline"):
        replay = test.post("/api/runs", json={"sample": sample})
        assert replay.status_code == 200 and replay.json()["reused"] is True
        assert replay.json()["manifest"] in original.values()
    assert archived(test) == original


def test_second_compaction_is_a_noop_not_a_second_copy(legacy_client):
    test, store = legacy_client
    first = test.post("/api/runs/compact", json={}).json()
    assert first["compacted"] == 2
    rows = store.list()
    second = test.post("/api/runs/compact", json={}).json()
    assert second["compacted"] == 0 and second["released_bytes"] == 0
    assert second["skipped"] == 2 and store.list() == rows
    assert len({item["run_id"] for item in rows}) == 2


def test_failed_switch_restores_previous_archive_state(legacy_client, monkeypatch):
    test, store = legacy_client
    before = {run_id: test.get(f"/api/runs/{run_id}/export").content for run_id in archived(test)}
    rows_before = {run_id: snapshot_of(test, run_id)[:2] for run_id in archived(test)}

    def broken_decode(self, connection, row):
        raise RuntimeError("verification unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(type(store), "_decode", broken_decode)
        with pytest.raises(RuntimeError, match="verification unavailable"):
            test.post("/api/runs/compact", json={})

    for run_id, bundle in before.items():
        assert snapshot_of(test, run_id)[0] == "json"
        assert test.get(f"/api/runs/{run_id}/export").content == bundle
        assert test.post(f"/api/runs/{run_id}/rerun").json()["verified"] is True
    assert {run_id: snapshot_of(test, run_id)[:2] for run_id in archived(test)} == rows_before


def test_tampered_legacy_row_blocks_whole_batch_without_partial_switch(legacy_client):
    test, store = legacy_client
    run_ids = list(archived(test))
    with store.connect() as connection:
        connection.execute("UPDATE wafer_runs SET snapshot = ? WHERE run_id = ?", ("{}", run_ids[0]))

    reply = test.post("/api/runs/compact", json={})
    assert (reply.status_code, reply.json()["code"]) == (409, "SNAPSHOT_DAMAGED")
    assert all(snapshot_of(test, run_id)[0] == "json" for run_id in run_ids)
    intact = run_ids[1]
    assert test.get(f"/api/runs/{intact}/export").status_code == 200


def test_selective_compaction_and_unknown_targets(legacy_client):
    test, store = legacy_client
    run_ids = list(archived(test))
    chosen, untouched = run_ids[0], run_ids[1]

    assert test.post("/api/runs/compact", json={"run_ids": ["missing"]}).status_code == 404

    result = test.post("/api/runs/compact", json={"run_ids": [chosen]}).json()
    assert result["compacted"] == 1 and result["missing"] == [] and result["run_ids"] == [chosen]
    assert snapshot_of(test, chosen)[0] == "packed-v1" and snapshot_of(test, untouched)[0] == "json"
    assert test.post(f"/api/runs/{chosen}/rerun").json()["verified"] is True


def test_new_runs_use_packed_layout_and_stay_independent(client, run_base):
    test = client
    report = test.get("/api/runs/storage").json()
    assert report["totals"]["pending_compaction"] == 0 and report["totals"]["ready"] == 1
    run = report["runs"][0]
    assert run["format"] == "packed-v1" and run["independent"] and run["embedded_files"] == 2

    bundle = test.get(run_base + "/export").json()
    for name, encoded in bundle["input"]["files_base64"].items():
        assert digest(base64.b64decode(encoded)) == bundle["manifest"]["sources"][name]["sha256"]


def test_lossy_or_missing_blob_is_reported_as_not_independent(legacy_client):
    test, store = legacy_client
    test.post("/api/runs/compact", json={})
    archived_runs = archived(test)
    run_id = next(iter(archived_runs))
    other = next(key for key in archived_runs if key != run_id)
    with store.connect() as connection:
        digests = connection.execute("SELECT source_blobs FROM wafer_runs WHERE run_id = ?", (run_id,)).fetchone()[0]
        first_sha = next(iter(json.loads(digests).values()))
        connection.execute("DELETE FROM wafer_blobs WHERE sha256 = ?", (first_sha,))

    run = test.get(f"/api/runs/{run_id}/storage").json()["runs"][0]
    assert run["independent"] is False and run["verified"] is False
    assert run["detail"]["code"] == "SNAPSHOT_DAMAGED"
    assert test.get(f"/api/runs/{run_id}/export").status_code == 409
    assert test.get(f"/api/runs/{other}/storage").json()["runs"][0]["verified"] is True
    assert test.get("/api/runs/storage").json()["totals"]["ready"] == 1
