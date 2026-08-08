"""Deterministic fixture repository and independent tree snapshots."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path, PurePosixPath

from temporal_native.contract import StrictModel

README_CONTENT = "# Durable agent fixture\n"


class FixtureSnapshot(StrictModel):
    """Content-addressed observation of every regular fixture file."""

    files: dict[str, str]
    tree_sha256: str


class FixtureRepository:
    """A small workspace whose correctness does not require model judgment."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    @classmethod
    def create(cls, root: Path) -> FixtureRepository:
        root.mkdir(mode=0o750, parents=True, exist_ok=False)
        repository = cls(root)
        repository.write("README.md", README_CONTENT)
        return repository

    @classmethod
    def open(cls, root: Path) -> FixtureRepository:
        if not root.is_dir():
            raise ValueError("fixture repository does not exist")
        return cls(root)

    @property
    def root(self) -> Path:
        return self._root

    def write(self, relative_path: str, content: str) -> str:
        target = self._target(relative_path)
        target.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        try:
            with temporary.open("x", encoding="utf-8") as output:
                output.write(content)
                output.flush()
                os.fsync(output.fileno())
            temporary.replace(target)
        finally:
            temporary.unlink(missing_ok=True)
        return hashlib.sha256(content.encode()).hexdigest()

    def snapshot(self) -> FixtureSnapshot:
        files: dict[str, str] = {}
        for path in sorted(self._root.rglob("*")):
            if path.is_symlink():
                raise ValueError("fixture repository must not contain symbolic links")
            if path.is_file():
                relative = path.relative_to(self._root).as_posix()
                files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
        canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
        return FixtureSnapshot(
            files=files,
            tree_sha256=hashlib.sha256(canonical).hexdigest(),
        )

    def verify(self, expected: FixtureSnapshot) -> None:
        if self.snapshot() != expected:
            raise ValueError("fixture repository does not match expected snapshot")

    def _target(self, relative_path: str) -> Path:
        logical_path = PurePosixPath(relative_path)
        if (
            not relative_path
            or logical_path.is_absolute()
            or "." in logical_path.parts
            or ".." in logical_path.parts
        ):
            raise ValueError("fixture path must be a safe relative path")
        target = self._root.joinpath(*logical_path.parts)
        resolved_parent = target.parent.resolve()
        if resolved_parent != self._root and self._root not in resolved_parent.parents:
            raise ValueError("fixture path must be a safe relative path")
        return target
