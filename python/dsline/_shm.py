"""`dsline shm` — named-channel region management.

Named channels on Windows live in paging-file sections (no backing file,
destroyed with the last handle); everywhere else they live as `dsline-*.bin`
region files in the platform runtime directory. These commands manage the
runtime directory and its region files.

The name → file mapping mirrors `dsline-shm` exactly (CRC-32 of the UTF-8
name, the same checksum the frame protocol uses): `dsline-<8 hex digits>.bin`.
"""

from __future__ import annotations

import os
import zlib
from datetime import datetime, timedelta, timezone
from typing import Any


def _checksum32(data: bytes) -> int:
    """CRC-32 (IEEE), mirroring dsline-core checksum32 (`crc32fast`).

    `zlib.crc32` is the same CRC-32/ISO-HDLC, so the digests agree byte for
    byte. The Rust side switched from FNV-1a to CRC-32 for speed, and this
    mirror must switch with it or the CLI would compute a different region
    path than the channel it is inspecting.
    """
    return zlib.crc32(data) & 0xFFFF_FFFF


_FNV_OFFSET = 0x811C_9DC5
_FNV_PRIME = 0x0100_0193


def _fnv1a32(data: bytes) -> int:
    """FNV-1a 32-bit — the *routing* hash of the sharded fan-out.

    Deliberately not `_checksum32`: the frame checksum is free to change with
    the protocol version, but the routing hash is a documented, solver-checked
    contract (`ShardedChannel` routes by `FNV-1a-32(key) % shards`, so every
    process agrees on the shard for a key).
    """
    value = _FNV_OFFSET
    for byte in data:
        value ^= byte
        value = (value * _FNV_PRIME) & 0xFFFF_FFFF
    return value


def runtime_dir() -> str:
    """The runtime directory holding named-channel region files."""
    if os.name == "nt":
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("TEMP") or os.getcwd()
    else:
        base = os.environ.get("XDG_RUNTIME_DIR") or os.environ.get("TMPDIR") or os.getcwd()
    return os.path.join(base, "dsline", "shm")


def region_path(name: str) -> str:
    """The region file path a named channel resolves to."""
    digest = _checksum32(name.encode("utf-8"))
    return os.path.join(runtime_dir(), f"dsline-{digest:08x}.bin")


def _entries() -> list[dict[str, Any]]:
    directory = runtime_dir()
    try:
        names = os.listdir(directory)
    except OSError:
        return []
    entries = []
    for name in names:
        if not name.startswith("dsline-") or not name.endswith(".bin"):
            continue
        path = os.path.join(directory, name)
        try:
            stat = os.stat(path)
        except OSError:
            continue
        entries.append(
            {
                "file": name,
                "bytes": stat.st_size,
                "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            }
        )
    return entries


def list_regions(json_output: bool) -> str:
    import json

    entries = _entries()
    if json_output:
        return json.dumps(entries, sort_keys=True)
    if not entries:
        return "(no region files in " + runtime_dir() + ")"
    lines = [f"{entry['file']}  {entry['bytes']:>9} bytes  {entry['mtime']}" for entry in entries]
    return "\n".join(lines)


def inspect(name: str, json_output: bool) -> str:
    import json

    path = region_path(name)
    try:
        stat = os.stat(path)
    except OSError:
        return json.dumps({"name": name, "exists": False}) if json_output else (
            f"channel '{name}' has no region file"
        )
    info = {
        "name": name,
        "path": path,
        "exists": True,
        "bytes": stat.st_size,
        "mtime": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
    }
    if json_output:
        return json.dumps(info, sort_keys=True)
    return "\n".join(f"{key}: {value}" for key, value in info.items())


def _parse_age(text: str) -> timedelta:
    """Parse a duration like `90s`, `30m`, `2h`, `1d`."""
    text = text.strip().lower()
    if text.endswith("s"):
        return timedelta(seconds=float(text[:-1]))
    if text.endswith("m"):
        return timedelta(minutes=float(text[:-1]))
    if text.endswith("h"):
        return timedelta(hours=float(text[:-1]))
    if text.endswith("d"):
        return timedelta(days=float(text[:-1]))
    raise ValueError(f"invalid duration '{text}' (use e.g. 90s, 30m, 2h, 1d)")


def cleanup(older_than: str, json_output: bool) -> str:
    import json

    age = _parse_age(older_than)
    now = datetime.now(tz=timezone.utc)
    removed: list[str] = []
    for entry in _entries():
        mtime = datetime.fromisoformat(entry["mtime"])
        if now - mtime > age:
            path = os.path.join(runtime_dir(), entry["file"])
            try:
                os.remove(path)
                removed.append(entry["file"])
            except OSError:
                pass
    if json_output:
        return json.dumps({"removed": removed}, sort_keys=True)
    if not removed:
        return "nothing to clean up"
    return "\n".join(removed)


def remove(name: str, json_output: bool) -> str:
    import json

    path = region_path(name)
    try:
        os.remove(path)
        removed = True
    except OSError:
        removed = False
    if json_output:
        return json.dumps({"name": name, "removed": removed}, sort_keys=True)
    if removed:
        return f"removed {path}"
    return f"no region file for '{name}' (windows named channels use sections and leave no files)"


def dir_path(json_output: bool) -> str:
    import json

    path = runtime_dir()
    if json_output:
        return json.dumps({"dir": path}, sort_keys=True)
    return path