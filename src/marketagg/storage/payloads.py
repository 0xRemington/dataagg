"""Private content-addressed payload spool (03 step 3).

Bodies are written atomically (temp file, fsync, rename) *before* facts are
staged, so a crash never leaves a batch referring to a missing body. URIs are
logical (``payload://<provider>/<aa>/<sha>.bin``); readers resolve them only
through this store, never from client-supplied paths.
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from pathlib import Path

_URI = re.compile(r"^payload://(?P<provider>[a-z][a-z0-9_.-]{1,63})/(?P<sha>[0-9a-f]{64})$")


class PayloadStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def _path(self, provider_id: str, sha256: str) -> Path:
        return self.root / provider_id / sha256[:2] / f"{sha256}.bin"

    def put(self, provider_id: str, body: bytes) -> str:
        sha = hashlib.sha256(body).hexdigest()
        path = self._path(provider_id, sha)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".spool-")
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(body)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp, path)
            except BaseException:
                Path(tmp).unlink(missing_ok=True)
                raise
        return f"payload://{provider_id}/{sha}"

    def get(self, uri: str) -> bytes:
        match = _URI.match(uri)
        if match is None:
            raise ValueError(f"not a payload URI: {uri!r}")
        body = self._path(match["provider"], match["sha"]).read_bytes()
        if hashlib.sha256(body).hexdigest() != match["sha"]:
            raise ValueError(f"payload checksum mismatch for {uri}")
        return body

    def exists(self, uri: str) -> bool:
        match = _URI.match(uri)
        return bool(match) and self._path(match["provider"], match["sha"]).is_file()  # type: ignore[index]

    def orphan_temp_files(self) -> list[Path]:
        """Interrupted writes left behind (startup recovery scan)."""
        return sorted(self.root.rglob(".spool-*")) if self.root.exists() else []
