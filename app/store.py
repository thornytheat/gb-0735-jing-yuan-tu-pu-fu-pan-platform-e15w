import base64
import binascii
import json
import sqlite3
import zlib
from contextlib import closing, contextmanager
from pathlib import Path

from app.errors import ReviewError
from app.manifest import canonical, digest, fingerprint
from app.modules.intake.contract import InputPackage, Sample

LEGACY = "json"
PACKED = "packed-v1"
FORMATS = (LEGACY, PACKED)


def compress(data):
    return zlib.compress(data, 9)


def decompress(data):
    return zlib.decompress(data)


class RunStore:
    """SQLite archive with two interchangeable row formats.

    * ``json`` — the historical layout: the full snapshot and a Base64 copy of
      every source file sit verbatim in the row. Large and duplicative.
    * ``packed-v1`` — the ``{snapshot, spec}`` payload is zlib-packed, while the
      original source bytes live once in ``wafer_blobs`` keyed by SHA-256. The
      ``snapshot`` column keeps the result digest as an integrity anchor.

    ``get`` returns the same ``(manifest, snapshot, pinned)`` triple for both, so
    the run id, business key and evidence bundle are unchanged by compaction.
    """

    def __init__(self, path):
        self.path = Path(path)

    @contextmanager
    def connect(self):
        with closing(sqlite3.connect(self.path, timeout=10)) as connection, connection:
            yield connection

    def initialize(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("""
                CREATE TABLE IF NOT EXISTS wafer_runs (
                    run_id TEXT PRIMARY KEY,
                    manifest TEXT NOT NULL,
                    snapshot TEXT NOT NULL,
                    pinned TEXT NOT NULL
                )
            """)
            columns = {row[1] for row in connection.execute("PRAGMA table_info(wafer_runs)")}
            for name, definition in (("format", "TEXT NOT NULL DEFAULT 'json'"),
                                     ("packed", "BLOB"), ("source_blobs", "TEXT")):
                if name not in columns:
                    connection.execute(f"ALTER TABLE wafer_runs ADD COLUMN {name} {definition}")
            connection.execute("""
                CREATE TABLE IF NOT EXISTS wafer_blobs (
                    sha256 TEXT PRIMARY KEY,
                    raw_bytes INTEGER NOT NULL,
                    stored_bytes INTEGER NOT NULL,
                    data BLOB NOT NULL
                )
            """)

    # -- archive rows -------------------------------------------------------
    def put(self, manifest, snapshot, pinned):
        packed, digests, blobs = self._encode(snapshot, pinned)
        with self.connect() as connection:
            cursor = connection.execute(
                "INSERT INTO wafer_runs (run_id, manifest, snapshot, pinned, format, packed, source_blobs)"
                " VALUES (?, ?, ?, '', ?, ?, ?) ON CONFLICT(run_id) DO NOTHING",
                (manifest["run_id"], canonical(manifest), manifest["result_sha256"], PACKED,
                 packed, canonical(digests)))
            created = cursor.rowcount == 1
            if created:
                self._store_blobs(connection, blobs)
            row = connection.execute("SELECT manifest FROM wafer_runs WHERE run_id = ?",
                                     (manifest["run_id"],)).fetchone()
        return json.loads(row[0]), created

    def get(self, run_id):
        with self.connect() as connection:
            row = connection.execute(
                "SELECT manifest, snapshot, pinned, format, packed, source_blobs"
                " FROM wafer_runs WHERE run_id = ?", (run_id,)).fetchone()
            if row is None:
                raise ReviewError("RUN_NOT_FOUND", "图批未找到", 404)
            return self._decode(connection, row)

    def list(self):
        with self.connect() as connection:
            rows = connection.execute("SELECT manifest FROM wafer_runs ORDER BY rowid DESC").fetchall()
        return [json.loads(row[0]) for row in rows]

    def _decode(self, connection, row):
        manifest_text, snapshot_text, pinned_text, fmt, packed, source_text = row
        try:
            manifest = json.loads(manifest_text)
        except (TypeError, ValueError) as exc:
            raise ReviewError("SNAPSHOT_DAMAGED", "运行归档无法读取", 409) from exc
        if fmt == LEGACY:
            try:
                return manifest, json.loads(snapshot_text), json.loads(pinned_text)
            except (TypeError, ValueError) as exc:
                raise ReviewError("SNAPSHOT_DAMAGED", "运行归档无法读取", 409) from exc
        if fmt != PACKED:
            raise ReviewError("SNAPSHOT_DAMAGED", "运行归档格式未知", 409)
        if snapshot_text != manifest.get("result_sha256"):
            raise ReviewError("SNAPSHOT_DAMAGED", "归档摘要不符", 409)
        try:
            payload = json.loads(decompress(packed).decode("utf-8"))
            digests = json.loads(source_text)
            files = {}
            for name, sha in digests.items():
                stored = connection.execute(
                    "SELECT data FROM wafer_blobs WHERE sha256 = ?", (sha,)).fetchone()
                if stored is None:
                    raise ReviewError("SNAPSHOT_DAMAGED", "归档原文缺失", 409)
                blob = decompress(stored[0])
                if digest(blob) != sha:
                    raise ReviewError("SNAPSHOT_DAMAGED", "归档原文损坏", 409)
                files[name] = base64.b64encode(blob).decode("ascii")
        except (TypeError, ValueError, zlib.error, binascii.Error) as exc:
            raise ReviewError("SNAPSHOT_DAMAGED", "运行归档无法读取", 409) from exc
        return manifest, payload["snapshot"], {"spec": payload["spec"], "files_base64": files}

    # -- encoding helpers ---------------------------------------------------
    @staticmethod
    def _package(pinned):
        try:
            spec = Sample.model_validate(pinned["spec"])
            files = {name: base64.b64decode(raw, validate=True) for name, raw in pinned["files_base64"].items()}
            if set(files) != {source.name for source in spec.sources}:
                raise ValueError("来源清单不符")
        except (KeyError, TypeError, ValueError, AttributeError, binascii.Error) as exc:
            raise ReviewError("SNAPSHOT_DAMAGED", "运行归档无法读取", 409) from exc
        return InputPackage(spec, files)

    def guard(self, manifest, snapshot, pinned):
        """Fail unless the archive is internally consistent and re-identifies."""
        package = self._package(pinned)
        key, sources, contract = fingerprint(package, manifest["rule_version"])
        if (key != manifest["run_id"] or sources != manifest["sources"] or contract != manifest["contract_sha256"]
                or digest(canonical(snapshot).encode()) != manifest["result_sha256"]):
            raise ReviewError("SNAPSHOT_DAMAGED", "归档摘要不符", 409)

    @staticmethod
    def _encode(snapshot, pinned):
        packed = compress(canonical({"snapshot": snapshot, "spec": pinned["spec"]}).encode("utf-8"))
        raw_files = {name: base64.b64decode(raw, validate=True)
                     for name, raw in pinned["files_base64"].items()}
        digests = {name: digest(blob) for name, blob in raw_files.items()}
        blobs = {digests[name]: (len(raw), compress(raw)) for name, raw in raw_files.items()}
        return packed, digests, blobs

    def _store_blobs(self, connection, blobs):
        for sha, (raw_bytes, stored) in blobs.items():
            connection.execute(
                "INSERT OR IGNORE INTO wafer_blobs (sha256, raw_bytes, stored_bytes, data)"
                " VALUES (?, ?, ?, ?)", (sha, raw_bytes, len(stored), stored))

    # -- reporting ----------------------------------------------------------
    def entries(self):
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT run_id, manifest, snapshot, pinned, format, packed, source_blobs"
                " FROM wafer_runs ORDER BY rowid DESC").fetchall()
            blob_sizes = dict(connection.execute("SELECT sha256, stored_bytes FROM wafer_blobs"))
        result = []
        for run_id, manifest_text, snapshot_text, pinned_text, fmt, packed, source_text in rows:
            entry = {"run_id": run_id, "format": fmt,
                     "own_bytes": len(manifest_text.encode("utf-8")),
                     "stored_bytes": None, "reclaimable_bytes": 0, "blob_count": 0}
            try:
                manifest = json.loads(manifest_text)
                entry.update(lot=manifest.get("lot"), label=manifest.get("label"),
                             created_at=manifest.get("created_at"),
                             sources=len(manifest.get("sources", {})))
                if fmt == PACKED:
                    digests = json.loads(source_text or "{}")
                    entry["own_bytes"] += len(packed or b"")
                    entry["blob_count"] = len(digests)
                    entry["stored_bytes"] = entry["own_bytes"] + sum(
                        blob_sizes.get(sha, 0) for sha in digests.values())
                elif fmt == LEGACY:
                    entry["own_bytes"] += len(snapshot_text.encode("utf-8")) + len(pinned_text.encode("utf-8"))
                    entry["stored_bytes"] = entry["own_bytes"]
                    snapshot, pinned = json.loads(snapshot_text), json.loads(pinned_text)
                    packed_new, _, blobs = self._encode(snapshot, pinned)
                    new_bytes = sum(len(stored) for sha, (_, stored) in blobs.items()
                                    if sha not in blob_sizes)
                    entry["blob_count"] = len(blobs)
                    entry["reclaimable_bytes"] = max(0, entry["stored_bytes"] - (
                        len(manifest_text.encode("utf-8")) + len(packed_new) + new_bytes))
                else:
                    raise ValueError("未知格式")
            except (KeyError, ValueError, TypeError, binascii.Error, zlib.error, ReviewError):
                entry["damaged"] = True
                entry["stored_bytes"] = entry["own_bytes"]
            result.append(entry)
        return result

    def blob_totals(self):
        with self.connect() as connection:
            count, stored, raw = connection.execute(
                "SELECT COUNT(*), COALESCE(SUM(stored_bytes), 0), COALESCE(SUM(raw_bytes), 0) FROM wafer_blobs"
            ).fetchone()
        return {"count": count, "stored_bytes": stored, "raw_bytes": raw}

    def disk_bytes(self):
        total = 0
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(f"{self.path}{suffix}")
            if candidate.exists():
                total += candidate.stat().st_size
        return total

    def storage_totals(self, entries=None):
        entries = self.entries() if entries is None else entries
        blobs = self.blob_totals()
        return {
            "runs": len(entries),
            "own_bytes": sum(entry["own_bytes"] for entry in entries),
            "blob_unique_bytes": blobs["stored_bytes"],
            "blob_raw_bytes": blobs["raw_bytes"],
            "blob_count": blobs["count"],
            "attributed_bytes": sum(entry["stored_bytes"] or 0 for entry in entries),
            "reclaimable_bytes": sum(entry["reclaimable_bytes"] or 0 for entry in entries),
            "pending_compaction": sum(1 for entry in entries if entry["format"] != PACKED),
            "packed": sum(1 for entry in entries if entry["format"] == PACKED),
            "damaged": sum(1 for entry in entries if entry.get("damaged")),
            "disk_bytes": self.disk_bytes(),
        }

    @staticmethod
    def _logical(connection):
        total = 0
        for manifest_text, snapshot_text, pinned_text, fmt, packed in connection.execute(
                "SELECT manifest, snapshot, pinned, format, packed FROM wafer_runs"):
            total += len(manifest_text.encode("utf-8"))
            if fmt == PACKED:
                total += len(packed or b"")
            else:
                total += len(snapshot_text.encode("utf-8")) + len(pinned_text.encode("utf-8"))
        total += connection.execute("SELECT COALESCE(SUM(stored_bytes), 0) FROM wafer_blobs").fetchone()[0]
        return total

    # -- compaction ---------------------------------------------------------
    def compact(self, run_ids=None):
        """Repack legacy rows into ``packed-v1`` inside a single transaction.

        Before commit every rewritten row is re-read through ``get``'s decoder and
        re-checked against its manifest, so a failed switch rolls back and leaves
        the previous archive state intact. The update only touches rows still in the
        legacy format and source blobs are content-addressed, which makes a retry a
        no-op rather than a second archive.
        """
        requested = None if run_ids is None else list(dict.fromkeys(run_ids))
        disk_before = self.disk_bytes()
        with self.connect() as connection:
            rows = connection.execute(
                "SELECT run_id, manifest, snapshot, pinned, format FROM wafer_runs ORDER BY rowid").fetchall()
            present = {row[0] for row in rows}
            missing = [run_id for run_id in requested or [] if run_id not in present]
            if requested and not present.intersection(requested):
                raise ReviewError("RUN_NOT_FOUND", "图批未找到", 404)
            targets = [row for row in rows if requested is None or row[0] in requested]
            known_blobs = {row[0] for row in connection.execute("SELECT sha256 FROM wafer_blobs")}

            before = self._logical(connection)
            plan = []
            for run_id, manifest_text, snapshot_text, pinned_text, fmt in targets:
                if fmt != LEGACY:
                    continue
                manifest, snapshot, pinned = (json.loads(manifest_text), json.loads(snapshot_text),
                                              json.loads(pinned_text))
                self.guard(manifest, snapshot, pinned)
                packed, digests, blobs = self._encode(snapshot, pinned)
                fresh = {sha: value for sha, value in blobs.items() if sha not in known_blobs}
                known_blobs.update(fresh)
                current = sum(len(part.encode("utf-8")) for part in (manifest_text, snapshot_text, pinned_text))
                replacement = len(manifest_text.encode("utf-8")) + len(packed) + sum(
                    len(stored) for _, stored in fresh.values())
                plan.append({"run_id": run_id, "manifest": manifest, "packed": packed,
                             "digests": digests, "blobs": blobs, "fresh": fresh,
                             "released": max(0, current - replacement), "written": False})

            for item in plan:
                self._store_blobs(connection, item["fresh"])
                cursor = connection.execute(
                    "UPDATE wafer_runs SET snapshot = ?, pinned = '', format = ?, packed = ?, source_blobs = ?"
                    " WHERE run_id = ? AND format = ?",
                    (item["manifest"]["result_sha256"], PACKED, item["packed"],
                     canonical(item["digests"]), item["run_id"], LEGACY))
                item["written"] = cursor.rowcount == 1

            # Recoverability gate inside the transaction.
            for item in plan:
                if not item["written"]:
                    continue
                row = connection.execute(
                    "SELECT manifest, snapshot, pinned, format, packed, source_blobs"
                    " FROM wafer_runs WHERE run_id = ?", (item["run_id"],)).fetchone()
                manifest, snapshot, pinned = self._decode(connection, row)
                self.guard(manifest, snapshot, pinned)

            written = sum(1 for item in plan if item["written"])
            released = sum(item["released"] for item in plan if item["written"])
            after = self._logical(connection)
            skipped = [row[0] for row in targets if row[4] != LEGACY]

        if written:
            self._reclaim()
        return {"compacted": written, "skipped": len(skipped), "skipped_run_ids": skipped,
                "missing": missing, "released_bytes": released,
                "before_bytes": before, "after_bytes": after,
                "disk_before_bytes": disk_before, "disk_after_bytes": self.disk_bytes(),
                "run_ids": [item["run_id"] for item in plan if item["written"]]}

    def _reclaim(self):
        try:
            with self.connect() as connection:
                connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error:
            pass
        try:
            connection = sqlite3.connect(self.path, timeout=10)
            try:
                connection.execute("VACUUM")
                connection.commit()
            finally:
                connection.close()
        except sqlite3.Error:
            pass
