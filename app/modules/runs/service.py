from datetime import datetime, timezone
from time import perf_counter

from app.config import RULE_VERSION, storage_budget
from app.errors import ReviewError
from app.manifest import canonical, digest, fingerprint, pin
from app.modules.intake.catalog import load_sample, restore_package
from app.modules.runs.bundle import make_bundle
from app.modules.runs.pipeline import calculate


class RunService:
    def __init__(self, store, loader=load_sample):
        self.store = store
        self.loader = loader

    def start(self, sample):
        started = perf_counter()
        package = self.loader(sample)
        key, sources, contract = fingerprint(package)
        snapshot = calculate(package)
        result_hash = digest(canonical(snapshot).encode())
        manifest = {"run_id": key, "sample": sample, "label": package.spec.label, "lot": package.spec.lot,
                    "wafers": [wafer.wafer for wafer in package.spec.wafers], "rule_version": RULE_VERSION,
                    "created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "duration_ms": round((perf_counter() - started) * 1000), "sources": sources,
                    "contract_sha256": contract, "result_sha256": result_hash,
                    "summary": snapshot["audit"]["summary"], "status": snapshot["audit"]["status"]}
        recorded, created = self.store.put(manifest, snapshot, pin(package))
        if recorded["result_sha256"] != result_hash:
            raise ReviewError("REPLAY_DIVERGED", "同版结果发生变化", 409)
        return {"manifest": recorded, "reused": not created}

    def rerun(self, run_id):
        manifest, snapshot, pinned = self.store.get(run_id)
        if manifest["rule_version"] != RULE_VERSION:
            raise ReviewError("RULE_VERSION_CHANGED", "旧规则版本无法重算", 409)
        package = restore_package(pinned)
        key, _, _ = fingerprint(package)
        if key != run_id or digest(canonical(snapshot).encode()) != manifest["result_sha256"]:
            raise ReviewError("SNAPSHOT_DAMAGED", "归档摘要不符", 409)
        actual = digest(canonical(calculate(package)).encode())
        if actual != manifest["result_sha256"]:
            raise ReviewError("REPLAY_DIVERGED", "复算摘要不符", 409)
        return {"manifest": manifest, "reused": True, "verified": True}

    def storage_report(self, run_id=None):
        """Space accounting plus a per-archive self-containment check.

        Each archive is re-opened through the ordinary read path and an evidence
        bundle is rebuilt in memory, so a run only reports ``independent`` when one
        downloaded bundle can be re-verified without any other archive. Read-only.
        """
        entries = self.store.entries()
        if run_id is not None:
            matching = [entry for entry in entries if entry["run_id"] == run_id]
            if not matching:
                raise ReviewError("RUN_NOT_FOUND", "图批未找到", 404)
            entries = matching
        runs = []
        for entry in entries:
            record = {key: entry.get(key) for key in
                      ("run_id", "format", "lot", "label", "created_at", "stored_bytes", "own_bytes",
                       "reclaimable_bytes", "blob_count", "damaged")}
            record["damaged"] = bool(record["damaged"])
            record.update(independent=False, verified=False, bundle_bytes=None, embedded_files=0, detail=None)
            try:
                manifest, snapshot, pinned = self.store.get(entry["run_id"])
                self.store.guard(manifest, snapshot, pinned)
                bundle = make_bundle((manifest, snapshot, pinned))
                record.update(independent=True, verified=True, bundle_bytes=len(bundle),
                              embedded_files=len(pinned["files_base64"]))
            except ReviewError as exc:
                record["detail"] = {"code": exc.code, "message": str(exc)}
            runs.append(record)
        totals = self.store.storage_totals(entries)
        totals.update(independent=sum(1 for run in runs if run["independent"]),
                      ready=sum(1 for run in runs if run["independent"] and not run["reclaimable_bytes"]))
        budget = storage_budget()
        totals.update(budget_bytes=budget, over_budget=None if budget is None else totals["disk_bytes"] > budget,
                      budget_usage_pct=None if budget is None else round(
                          totals["disk_bytes"] * 100 / budget, 2))
        return {"runs": runs, "totals": totals}

    def compact(self, run_ids=None):
        result = self.store.compact(run_ids)
        result["storage"] = self.storage_report()["totals"]
        return result
