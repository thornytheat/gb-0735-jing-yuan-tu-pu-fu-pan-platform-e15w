import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.manifest import canonical, digest, fingerprint, pin
from app.modules.intake.catalog import load_sample
from app.modules.runs.pipeline import calculate


@pytest.fixture(scope="session")
def package():
    return load_sample("mixed")


@pytest.fixture(scope="session")
def snapshot(package):
    return calculate(package)


@pytest.fixture
def client(tmp_path):
    with TestClient(create_app(tmp_path / "review.sqlite3", seed=False)) as test:
        yield test


@pytest.fixture
def run_base(client):
    response = client.post("/api/runs", json={"sample": "mixed"})
    assert response.status_code == 201, response.text
    return "/api/runs/" + response.json()["manifest"]["run_id"]


def legacy_row(sample):
    """Build a row in the historical uncompressed layout used by older archives."""
    loaded = load_sample(sample)
    key, sources, contract = fingerprint(loaded)
    evidence = calculate(loaded)
    manifest = {"run_id": key, "sample": sample, "label": loaded.spec.label, "lot": loaded.spec.lot,
                "wafers": [wafer.wafer for wafer in loaded.spec.wafers], "rule_version": "wafer-1.0.0",
                "created_at": "2026-11-01T00:00:00+00:00", "duration_ms": 4, "sources": sources,
                "contract_sha256": contract, "result_sha256": digest(canonical(evidence).encode()),
                "summary": evidence["audit"]["summary"], "status": evidence["audit"]["status"]}
    return manifest, canonical(evidence), canonical(pin(loaded))


@pytest.fixture
def legacy_client(tmp_path):
    """A client whose archive predates compaction: both samples stored verbatim."""
    with TestClient(create_app(tmp_path / "legacy.sqlite3", seed=False)) as test:
        store = test.app.state.runs.store
        with store.connect() as connection:
            for sample in ("mixed", "baseline"):
                manifest, evidence, pinned = legacy_row(sample)
                connection.execute("INSERT INTO wafer_runs (run_id, manifest, snapshot, pinned) VALUES (?, ?, ?, ?)",
                                   (manifest["run_id"], canonical(manifest), evidence, pinned))
        yield test, store

