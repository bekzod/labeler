#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import mimetypes
import math
import shutil
import threading
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse


VALID_SOURCES = {"text", "model_hypothesis", "likely_bad_text", "likely_bad_model_text"}


@dataclass
class CommitResult:
    output_path: str
    deleted_output_path: str
    backup_path: str | None
    kept_rows: int
    deleted_rows: int
    changed_rows: int


class LabelStore:
    def __init__(self, jsonl_path: Path):
        self.jsonl_path = jsonl_path.resolve()
        self.base_dir = self.jsonl_path.parent
        self.state_path = self.jsonl_path.with_suffix(".label_state.json")
        self.default_deleted_output_path = self.jsonl_path.with_suffix(".deleted.jsonl")

        self._lock = threading.Lock()
        self.offsets = self._build_offsets()
        self.total_rows = len(self.offsets)
        self.edits: dict[str, dict] = {}
        self.deleted_indices: set[int] = set()
        self.checked_indices: set[int] = set()
        self._load_state()

    def _build_offsets(self) -> list[int]:
        offsets: list[int] = []
        with self.jsonl_path.open("rb") as handle:
            while True:
                pos = handle.tell()
                line = handle.readline()
                if not line:
                    break
                offsets.append(pos)
        return offsets

    def _sanitize_state(self, data: dict) -> tuple[dict[str, dict], set[int], set[int]]:
        clean_edits: dict[str, dict] = {}
        clean_deleted: set[int] = set()
        clean_checked: set[int] = set()

        edits = data.get("edits", {})
        if isinstance(edits, dict):
            for key, value in edits.items():
                if not str(key).isdigit():
                    continue
                idx = int(key)
                if idx < 0 or idx >= self.total_rows:
                    continue
                if not isinstance(value, dict):
                    continue

                selected_source = value.get("selected_source", "text")
                if selected_source not in VALID_SOURCES:
                    selected_source = "text"

                edited_text = value.get("edited_text", "")
                clean_edits[str(idx)] = {
                    "selected_source": selected_source,
                    "edited_text": str(edited_text),
                    "updated_at": value.get("updated_at"),
                }

        deleted_indices = data.get("deleted_indices", [])
        if isinstance(deleted_indices, list):
            for raw_idx in deleted_indices:
                if isinstance(raw_idx, int) and 0 <= raw_idx < self.total_rows:
                    clean_deleted.add(raw_idx)

        checked_indices = data.get("checked_indices", [])
        if isinstance(checked_indices, list):
            for raw_idx in checked_indices:
                if isinstance(raw_idx, int) and 0 <= raw_idx < self.total_rows:
                    clean_checked.add(raw_idx)

        return clean_edits, clean_deleted, clean_checked

    def _load_state(self) -> None:
        if not self.state_path.exists():
            return

        try:
            raw = self.state_path.read_text(encoding="utf-8")
            data = json.loads(raw)
        except (OSError, json.JSONDecodeError):
            return

        if data.get("jsonl_path") != str(self.jsonl_path):
            return

        edits, deleted, checked = self._sanitize_state(data)
        self.edits = edits
        self.deleted_indices = deleted
        self.checked_indices = checked

    def _save_state_locked(self) -> None:
        payload = {
            "version": 1,
            "jsonl_path": str(self.jsonl_path),
            "total_rows": self.total_rows,
            "updated_at": datetime.utcnow().isoformat() + "Z",
            "edits": self.edits,
            "deleted_indices": sorted(self.deleted_indices),
            "checked_indices": sorted(self.checked_indices),
        }
        tmp = self.state_path.with_name(self.state_path.name + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(self.state_path)

    def _read_row_locked(self, index: int) -> dict:
        with self.jsonl_path.open("rb") as handle:
            handle.seek(self.offsets[index])
            raw_line = handle.readline()

        line = raw_line.decode("utf-8").rstrip("\n")
        return json.loads(line)

    def metadata(self) -> dict:
        with self._lock:
            return {
                "jsonl_path": str(self.jsonl_path),
                "state_path": str(self.state_path),
                "default_deleted_output_path": str(self.default_deleted_output_path),
                "total_rows": self.total_rows,
                "edited_rows": len(self.edits),
                "deleted_rows": len(self.deleted_indices),
                "checked_rows": len(self.checked_indices),
            }

    def get_row(self, index: int) -> dict:
        with self._lock:
            if index < 0 or index >= self.total_rows:
                raise IndexError(f"Row index {index} out of range")

            row = self._read_row_locked(index)
            saved = self.edits.get(str(index), {})
            selected_source = saved.get("selected_source", "text")
            if selected_source not in VALID_SOURCES:
                selected_source = "text"

            has_saved_edit = "edited_text" in saved
            edited_text = str(saved.get("edited_text", "")) if has_saved_edit else ""

            return {
                "index": index,
                "total_rows": self.total_rows,
                "row": row,
                "state": {
                    "selected_source": selected_source,
                    "edited_text": edited_text,
                    "has_saved_edit": has_saved_edit,
                    "deleted": index in self.deleted_indices,
                    "checked": index in self.checked_indices,
                },
            }

    def save_row(
        self, index: int, selected_source: str, edited_text: str, deleted: bool, mark_checked: bool = False
    ) -> dict:
        if selected_source not in VALID_SOURCES:
            raise ValueError("selected_source must be text or model_hypothesis")

        with self._lock:
            if index < 0 or index >= self.total_rows:
                raise IndexError(f"Row index {index} out of range")

            row = self._read_row_locked(index)
            edited_text_value = str(edited_text)
            is_noop_edit = (
                selected_source == "text"
                and not deleted
                and edited_text_value == str(row.get("text", ""))
                and not (mark_checked and selected_source in {"text", "model_hypothesis"})
            )

            if is_noop_edit:
                self.edits.pop(str(index), None)
            else:
                self.edits[str(index)] = {
                    "selected_source": selected_source,
                    "edited_text": edited_text_value,
                    "updated_at": datetime.utcnow().isoformat() + "Z",
                }

            if deleted:
                self.deleted_indices.add(index)
                self.checked_indices.discard(index)
            else:
                self.deleted_indices.discard(index)

            if mark_checked and selected_source in {"text", "model_hypothesis"} and not deleted:
                self.checked_indices.add(index)

            self._save_state_locked()

            return {
                "index": index,
                "edited_rows": len(self.edits),
                "deleted_rows": len(self.deleted_indices),
                "checked_rows": len(self.checked_indices),
                "checked": index in self.checked_indices,
            }

    def reset_row(self, index: int) -> dict:
        with self._lock:
            if index < 0 or index >= self.total_rows:
                raise IndexError(f"Row index {index} out of range")

            self.edits.pop(str(index), None)
            self.deleted_indices.discard(index)
            self.checked_indices.discard(index)
            self._save_state_locked()

            return {
                "index": index,
                "edited_rows": len(self.edits),
                "deleted_rows": len(self.deleted_indices),
                "checked_rows": len(self.checked_indices),
            }

    def reset_all(self) -> dict:
        with self._lock:
            cleared_edits = len(self.edits)
            cleared_deleted = len(self.deleted_indices)

            self.edits.clear()
            self.deleted_indices.clear()
            self.checked_indices.clear()
            self._save_state_locked()

            return {
                "cleared_edits": cleared_edits,
                "cleared_deleted": cleared_deleted,
                "edited_rows": len(self.edits),
                "deleted_rows": len(self.deleted_indices),
                "checked_rows": len(self.checked_indices),
            }

    def _resolve_output_path(self, provided: str | None, default_path: Path) -> Path:
        if not provided:
            return default_path
        candidate = Path(provided).expanduser()
        if not candidate.is_absolute():
            candidate = (self.base_dir / candidate).resolve()
        return candidate

    def _audio_path(self, relative_path: str) -> Path:
        candidate = Path(relative_path)
        if candidate.is_absolute():
            resolved = candidate.resolve()
        else:
            resolved = (self.base_dir / candidate).resolve()

        if self.base_dir not in resolved.parents and resolved != self.base_dir:
            raise ValueError("audio path escapes dataset root")
        return resolved

    def resolve_audio(self, relative_path: str) -> Path:
        with self._lock:
            return self._audio_path(relative_path)

    def commit(self, output_path: str | None, deleted_output_path: str | None) -> CommitResult:
        with self._lock:
            target_output = self._resolve_output_path(output_path, self.jsonl_path)
            target_deleted = self._resolve_output_path(
                deleted_output_path, self.default_deleted_output_path
            )
            if target_output == target_deleted:
                raise ValueError("output_path and deleted_output_path must be different")

            target_output.parent.mkdir(parents=True, exist_ok=True)
            target_deleted.parent.mkdir(parents=True, exist_ok=True)

            tmp_output = target_output.with_name(target_output.name + ".tmp")
            tmp_deleted = target_deleted.with_name(target_deleted.name + ".tmp")

            backup_path: Path | None = None
            if target_output == self.jsonl_path:
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = self.jsonl_path.with_name(f"{self.jsonl_path.name}.bak.{stamp}")
                shutil.copy2(self.jsonl_path, backup_path)

            kept_rows = 0
            deleted_rows = 0
            changed_rows = 0
            edits = dict(self.edits)
            deleted_indices = set(self.deleted_indices)
            checked_indices = set(self.checked_indices)

            try:
                with self.jsonl_path.open("r", encoding="utf-8") as source, tmp_output.open(
                    "w", encoding="utf-8"
                ) as out, tmp_deleted.open("w", encoding="utf-8") as deleted_out:
                    for idx, line in enumerate(source):
                        line = line.rstrip("\n")
                        if not line:
                            continue

                        try:
                            row = json.loads(line)
                        except json.JSONDecodeError:
                            if idx in deleted_indices:
                                deleted_out.write(line + "\n")
                                deleted_rows += 1
                            else:
                                out.write(line + "\n")
                                kept_rows += 1
                            continue

                        edit = edits.get(str(idx))

                        if idx in deleted_indices:
                            deleted_record = dict(row)
                            if edit:
                                deleted_record["_label_meta"] = {
                                    "deleted_at": datetime.utcnow().isoformat() + "Z",
                                    "index": idx,
                                    "selected_source": edit.get("selected_source", "text"),
                                    "edited_text": edit.get("edited_text", ""),
                                }
                            deleted_out.write(
                                json.dumps(deleted_record, ensure_ascii=False, allow_nan=True) + "\n"
                            )
                            deleted_rows += 1
                            continue

                        row_changed = False
                        output_row = row

                        if edit:
                            new_text = str(edit.get("edited_text", ""))
                            if output_row.get("text") != new_text:
                                output_row = dict(output_row)
                                output_row["text"] = new_text
                                row_changed = True

                        if idx in checked_indices:
                            checked_value = (
                                str(edit.get("edited_text", ""))
                                if edit
                                else str(output_row.get("text", ""))
                            )
                            if output_row.get("text") != checked_value:
                                if output_row is row:
                                    output_row = dict(output_row)
                                output_row["text"] = checked_value
                                row_changed = True
                            if output_row.get("model_hypothesis") != checked_value:
                                if output_row is row:
                                    output_row = dict(output_row)
                                output_row["model_hypothesis"] = checked_value
                                row_changed = True

                        if row_changed:
                            changed_rows += 1

                        out.write(json.dumps(output_row, ensure_ascii=False, allow_nan=True) + "\n")
                        kept_rows += 1
                tmp_output.replace(target_output)

                if target_deleted.exists():
                    needs_newline = False
                    new_deleted_size = tmp_deleted.stat().st_size

                    if target_deleted.stat().st_size > 0 and new_deleted_size > 0:
                        with target_deleted.open("rb") as existing_deleted:
                            existing_deleted.seek(-1, 2)
                            needs_newline = existing_deleted.read(1) != b"\n"

                    with target_deleted.open("ab") as deleted_append, tmp_deleted.open("rb") as deleted_new:
                        if needs_newline:
                            deleted_append.write(b"\n")
                        deleted_append.write(deleted_new.read())

                    tmp_deleted.unlink(missing_ok=True)
                else:
                    tmp_deleted.replace(target_deleted)
            finally:
                if tmp_output.exists():
                    tmp_output.unlink(missing_ok=True)
                if tmp_deleted.exists():
                    tmp_deleted.unlink(missing_ok=True)

            if target_output == self.jsonl_path:
                # In-place write changes row numbering, so state must be reset.
                self.edits.clear()
                self.deleted_indices.clear()
                self.checked_indices.clear()
                self.offsets = self._build_offsets()
                self.total_rows = len(self.offsets)
                self._save_state_locked()

            return CommitResult(
                output_path=str(target_output),
                deleted_output_path=str(target_deleted),
                backup_path=str(backup_path) if backup_path else None,
                kept_rows=kept_rows,
                deleted_rows=deleted_rows,
                changed_rows=changed_rows,
            )



def _json_safe(value):
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


class LabelingHandler(BaseHTTPRequestHandler):
    store: LabelStore
    static_dir: Path

    def _send_json(self, payload: dict, code: int = 200) -> None:
        safe_payload = _json_safe(payload)
        raw = json.dumps(safe_payload, ensure_ascii=False, allow_nan=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _send_text(self, text: str, code: int = 200) -> None:
        data = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length > 0 else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _serve_static(self, path: str) -> None:
        if path == "/":
            rel = "index.html"
        else:
            rel = path.removeprefix("/static/")

        file_path = (self.static_dir / rel).resolve()
        if self.static_dir not in file_path.parents and file_path != self.static_dir:
            self._send_text("Not found", code=404)
            return

        if not file_path.exists() or not file_path.is_file():
            self._send_text("Not found", code=404)
            return

        content = file_path.read_bytes()
        mime, _ = mimetypes.guess_type(file_path.name)
        self.send_response(200)
        self.send_header("Content-Type", f"{mime or 'application/octet-stream'}")
        self.send_header("Content-Length", str(len(content)))
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path == "/" or parsed.path.startswith("/static/"):
            self._serve_static(parsed.path)
            return

        if parsed.path == "/api/meta":
            self._send_json({"ok": True, "meta": self.store.metadata()})
            return

        if parsed.path == "/api/row":
            params = parse_qs(parsed.query)
            raw_idx = params.get("index", ["0"])[0]
            try:
                idx = int(raw_idx)
                payload = self.store.get_row(idx)
                self._send_json({"ok": True, **payload})
            except (ValueError, IndexError, json.JSONDecodeError) as exc:
                self._send_json({"ok": False, "error": str(exc)}, code=400)
            return

        if parsed.path == "/api/audio":
            params = parse_qs(parsed.query)
            relative_path = params.get("path", [""])[0]
            try:
                audio_path = self.store.resolve_audio(relative_path)
            except ValueError as exc:
                self._send_json({"ok": False, "error": str(exc)}, code=400)
                return

            if not audio_path.exists() or not audio_path.is_file():
                self._send_json({"ok": False, "error": "Audio file not found"}, code=404)
                return

            data = audio_path.read_bytes()
            mime, _ = mimetypes.guess_type(audio_path.name)
            self.send_response(200)
            self.send_header("Content-Type", mime or "application/octet-stream")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return

        self._send_text("Not found", code=404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)

        if parsed.path == "/api/row/save":
            try:
                payload = self._read_json()
                idx = int(payload.get("index"))
                selected_source = str(payload.get("selected_source", "text"))
                edited_text = str(payload.get("edited_text", ""))
                deleted = bool(payload.get("deleted", False))
                mark_checked = bool(payload.get("mark_checked", False))
                result = self.store.save_row(
                    idx, selected_source, edited_text, deleted, mark_checked=mark_checked
                )
                self._send_json({"ok": True, "result": result})
            except (ValueError, TypeError, IndexError, json.JSONDecodeError) as exc:
                self._send_json({"ok": False, "error": str(exc)}, code=400)
            return

        if parsed.path == "/api/row/reset":
            try:
                payload = self._read_json()
                idx = int(payload.get("index"))
                result = self.store.reset_row(idx)
                self._send_json({"ok": True, "result": result, "meta": self.store.metadata()})
            except (ValueError, TypeError, IndexError, json.JSONDecodeError) as exc:
                self._send_json({"ok": False, "error": str(exc)}, code=400)
            return

        if parsed.path == "/api/reset_all":
            try:
                result = self.store.reset_all()
                self._send_json({"ok": True, "result": result, "meta": self.store.metadata()})
            except (ValueError, TypeError, json.JSONDecodeError, OSError) as exc:
                self._send_json({"ok": False, "error": str(exc)}, code=400)
            return

        if parsed.path == "/api/commit":
            try:
                payload = self._read_json()
                output_path = payload.get("output_path")
                deleted_output_path = payload.get("deleted_output_path")
                result = self.store.commit(output_path, deleted_output_path)
                self._send_json(
                    {
                        "ok": True,
                        "result": {
                            "output_path": result.output_path,
                            "deleted_output_path": result.deleted_output_path,
                            "backup_path": result.backup_path,
                            "kept_rows": result.kept_rows,
                            "deleted_rows": result.deleted_rows,
                            "changed_rows": result.changed_rows,
                        },
                        "meta": self.store.metadata(),
                    }
                )
            except (ValueError, TypeError, json.JSONDecodeError, OSError) as exc:
                self._send_json({"ok": False, "error": str(exc)}, code=400)
            return

        self._send_text("Not found", code=404)

    def log_message(self, fmt: str, *args) -> None:
        # Keep logging concise but visible in terminal.
        print(f"[{self.log_date_time_string()}] {self.address_string()} {fmt % args}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Mini JSONL labeling app")
    parser.add_argument("--jsonl", required=True, help="Path to source JSONL file")
    parser.add_argument("--host", default="127.0.0.1", help="Server host")
    parser.add_argument("--port", type=int, default=8765, help="Server port")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    jsonl_path = Path(args.jsonl).expanduser().resolve()
    if not jsonl_path.exists():
        raise FileNotFoundError(f"JSONL not found: {jsonl_path}")

    app_dir = Path(__file__).resolve().parent
    static_dir = app_dir / "static"
    if not static_dir.exists():
        raise FileNotFoundError(f"Static directory not found: {static_dir}")

    store = LabelStore(jsonl_path)
    LabelingHandler.store = store
    LabelingHandler.static_dir = static_dir

    server = ThreadingHTTPServer((args.host, args.port), LabelingHandler)
    print(f"Serving labeler at http://{args.host}:{args.port}")
    print(f"JSONL: {jsonl_path}")
    print(f"State: {store.state_path}")
    print(f"Deleted output default: {store.default_deleted_output_path}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
