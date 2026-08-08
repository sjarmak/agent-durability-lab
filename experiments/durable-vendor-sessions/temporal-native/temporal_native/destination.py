"""SQLite-backed controlled destination for unsafe and idempotent effects."""

from __future__ import annotations

import hashlib
import sqlite3
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path

from temporal_native.contract import (
    CleanupRequest,
    DestinationReceipt,
    StrictModel,
    ToolRequest,
)
from temporal_native.fixture import FixtureRepository


class ProtectionMode(StrEnum):
    """Destination behavior under repeated Activity delivery."""

    UNSAFE = "unsafe"
    IDEMPOTENT = "idempotent"


class DestinationAttempt(StrictModel):
    session_id: str
    logical_turn_id: str
    logical_effect_id: str
    physical_attempt_id: str
    generation: int
    owner_capability: str
    applied: bool
    receipt_id: str
    artifact_sha256: str
    observed_at: datetime


class CleanupAttempt(StrictModel):
    session_id: str
    logical_turn_id: str
    generation: int
    physical_attempt_id: str
    reason: str
    applied: bool
    observed_at: datetime


class DestinationSnapshot(StrictModel):
    destination_id: str
    mode: ProtectionMode
    attempts: tuple[DestinationAttempt, ...]
    cleanups: tuple[CleanupAttempt, ...]


class ControlledDestination:
    """Records every delivery attempt and optionally deduplicates logical effects."""

    def __init__(
        self,
        database_path: Path,
        workspace: FixtureRepository,
        mode: ProtectionMode,
    ) -> None:
        self._database_path = database_path.resolve()
        self._workspace = workspace
        self._mode = mode
        self._destination_id = f"sqlite:{self._database_path}"

    @classmethod
    def create(
        cls,
        *,
        database_path: Path,
        workspace_path: Path,
        mode: ProtectionMode,
    ) -> ControlledDestination:
        database_path.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
        if database_path.exists():
            raise ValueError("destination database already exists")
        workspace = FixtureRepository.create(workspace_path)
        destination = cls(database_path, workspace, mode)
        destination._initialize_database()
        return destination

    def _initialize_database(self) -> None:
        with self._connect() as connection:
            connection.executescript(
                """
                CREATE TABLE metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE attempts (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    physical_attempt_id TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL,
                    logical_turn_id TEXT NOT NULL,
                    logical_effect_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    owner_capability TEXT NOT NULL,
                    relative_path TEXT NOT NULL,
                    content_sha256 TEXT NOT NULL,
                    receipt_id TEXT NOT NULL,
                    artifact_sha256 TEXT NOT NULL,
                    applied INTEGER NOT NULL CHECK (applied IN (0, 1)),
                    observed_at TEXT NOT NULL
                );
                CREATE TABLE cleanups (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    physical_attempt_id TEXT NOT NULL UNIQUE,
                    session_id TEXT NOT NULL,
                    logical_turn_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    owner_capability TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    applied INTEGER NOT NULL CHECK (applied IN (0, 1)),
                    observed_at TEXT NOT NULL
                );
                """
            )
            connection.executemany(
                "INSERT INTO metadata(key, value) VALUES (?, ?)",
                (("mode", self._mode.value), ("destination_id", self._destination_id)),
            )

    @classmethod
    def open(
        cls,
        *,
        database_path: Path,
        workspace_path: Path,
    ) -> ControlledDestination:
        if not database_path.is_file():
            raise ValueError("destination database does not exist")
        workspace = FixtureRepository.open(workspace_path)
        with sqlite3.connect(database_path) as connection:
            row = connection.execute("SELECT value FROM metadata WHERE key = 'mode'").fetchone()
        if row is None:
            raise ValueError("destination database lacks protection mode")
        return cls(database_path, workspace, ProtectionMode(row[0]))

    def apply(self, request: ToolRequest) -> DestinationReceipt:
        content_sha256 = hashlib.sha256(request.content.encode()).hexdigest()
        with self._connect() as connection:
            existing_attempt = self._find_attempt(connection, request.physical_attempt_id)
            if existing_attempt is not None:
                if existing_attempt[2] != request.logical_effect_id:
                    raise ValueError("physical attempt identity was reused for another effect")
                return self._receipt_from_row(existing_attempt)
            prior = self._find_prior_effect(connection, request.logical_effect_id)
            applied = prior is None
            if applied:
                artifact_sha256 = self._workspace.write(request.relative_path, request.content)
                receipt_id = self._receipt_id(request.logical_effect_id, artifact_sha256)
            else:
                receipt_id = prior[6]
                artifact_sha256 = prior[7]
            observed_at = self._next_observed_at(connection, "attempts")
            self._insert_attempt(
                connection,
                request,
                content_sha256,
                receipt_id,
                artifact_sha256,
                applied,
                observed_at,
            )
        return DestinationReceipt(
            destination_id=self._destination_id,
            session_id=request.session_id,
            logical_turn_id=request.logical_turn_id,
            logical_effect_id=request.logical_effect_id,
            generation=request.generation,
            owner_capability=request.owner_capability,
            physical_attempt_id=request.physical_attempt_id,
            receipt_id=receipt_id,
            artifact_sha256=artifact_sha256,
            applied=applied,
        )

    def record_cleanup(self, request: CleanupRequest) -> CleanupAttempt:
        """Record retry-safe cleanup for one logical session owner."""

        with self._connect() as connection:
            existing_attempt = connection.execute(
                """
                SELECT session_id, logical_turn_id, generation,
                       physical_attempt_id, reason, applied, observed_at
                FROM cleanups WHERE physical_attempt_id = ?
                """,
                (request.physical_attempt_id,),
            ).fetchone()
            if existing_attempt is not None:
                if existing_attempt[0] != request.session_id:
                    raise ValueError("physical cleanup identity was reused for another session")
                return self._cleanup_from_row(existing_attempt)

            already_applied = connection.execute(
                """
                SELECT 1 FROM cleanups
                WHERE session_id = ? AND generation = ? AND applied = 1
                LIMIT 1
                """,
                (request.session_id, request.generation),
            ).fetchone()
            applied = already_applied is None
            observed_at = self._next_observed_at(connection, "cleanups")
            self._insert_cleanup(connection, request, applied, observed_at)
        return CleanupAttempt(
            session_id=request.session_id,
            logical_turn_id=request.logical_turn_id,
            generation=request.generation,
            physical_attempt_id=request.physical_attempt_id,
            reason=request.reason,
            applied=applied,
            observed_at=observed_at,
        )

    def snapshot(self) -> DestinationSnapshot:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT session_id, logical_turn_id, logical_effect_id,
                       physical_attempt_id, generation, owner_capability,
                       applied, receipt_id, artifact_sha256, observed_at
                FROM attempts ORDER BY sequence
                """
            ).fetchall()
            cleanup_rows = connection.execute(
                """
                SELECT session_id, logical_turn_id, generation,
                       physical_attempt_id, reason, applied, observed_at
                FROM cleanups ORDER BY sequence
                """
            ).fetchall()
        attempts = tuple(
            DestinationAttempt(
                session_id=row[0],
                logical_turn_id=row[1],
                logical_effect_id=row[2],
                physical_attempt_id=row[3],
                generation=row[4],
                owner_capability=row[5],
                applied=bool(row[6]),
                receipt_id=row[7],
                artifact_sha256=row[8],
                observed_at=row[9],
            )
            for row in rows
        )
        return DestinationSnapshot(
            destination_id=self._destination_id,
            mode=self._mode,
            attempts=attempts,
            cleanups=tuple(self._cleanup_from_row(row) for row in cleanup_rows),
        )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._database_path, timeout=10)
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @staticmethod
    def _find_attempt(
        connection: sqlite3.Connection, physical_attempt_id: str
    ) -> tuple[object, ...] | None:
        return connection.execute(
            """
            SELECT session_id, logical_turn_id, logical_effect_id, generation,
                   owner_capability, physical_attempt_id, receipt_id,
                   artifact_sha256, applied
            FROM attempts WHERE physical_attempt_id = ?
            """,
            (physical_attempt_id,),
        ).fetchone()

    def _find_prior_effect(
        self, connection: sqlite3.Connection, logical_effect_id: str
    ) -> tuple[object, ...] | None:
        if self._mode is ProtectionMode.UNSAFE:
            return None
        return connection.execute(
            """
            SELECT session_id, logical_turn_id, logical_effect_id, generation,
                   owner_capability, physical_attempt_id, receipt_id,
                   artifact_sha256, applied
            FROM attempts
            WHERE logical_effect_id = ? AND applied = 1
            ORDER BY sequence LIMIT 1
            """,
            (logical_effect_id,),
        ).fetchone()

    @staticmethod
    def _insert_attempt(
        connection: sqlite3.Connection,
        request: ToolRequest,
        content_sha256: str,
        receipt_id: str,
        artifact_sha256: str,
        applied: bool,
        observed_at: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO attempts(
                physical_attempt_id, session_id, logical_turn_id,
                logical_effect_id, generation, owner_capability,
                relative_path, content_sha256, receipt_id,
                artifact_sha256, applied, observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.physical_attempt_id,
                request.session_id,
                request.logical_turn_id,
                request.logical_effect_id,
                request.generation,
                request.owner_capability,
                request.relative_path,
                content_sha256,
                receipt_id,
                artifact_sha256,
                int(applied),
                observed_at.isoformat(),
            ),
        )

    @staticmethod
    def _insert_cleanup(
        connection: sqlite3.Connection,
        request: CleanupRequest,
        applied: bool,
        observed_at: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO cleanups(
                physical_attempt_id, session_id, logical_turn_id,
                generation, owner_capability, reason, applied, observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request.physical_attempt_id,
                request.session_id,
                request.logical_turn_id,
                request.generation,
                request.owner_capability,
                request.reason,
                int(applied),
                observed_at.isoformat(),
            ),
        )

    def _receipt_from_row(self, row: tuple[object, ...]) -> DestinationReceipt:
        return DestinationReceipt(
            destination_id=self._destination_id,
            session_id=str(row[0]),
            logical_turn_id=str(row[1]),
            logical_effect_id=str(row[2]),
            generation=int(row[3]),
            owner_capability=str(row[4]),
            physical_attempt_id=str(row[5]),
            receipt_id=str(row[6]),
            artifact_sha256=str(row[7]),
            applied=bool(row[8]),
        )

    @staticmethod
    def _cleanup_from_row(row: tuple[object, ...]) -> CleanupAttempt:
        return CleanupAttempt(
            session_id=str(row[0]),
            logical_turn_id=str(row[1]),
            generation=int(row[2]),
            physical_attempt_id=str(row[3]),
            reason=str(row[4]),
            applied=bool(row[5]),
            observed_at=str(row[6]),
        )

    @staticmethod
    def _next_observed_at(connection: sqlite3.Connection, table: str) -> datetime:
        if table not in {"attempts", "cleanups"}:
            raise ValueError("unsupported observation table")
        row = connection.execute(
            f"SELECT observed_at FROM {table} ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        observed_at = datetime.now(UTC)
        if row is not None:
            previous = datetime.fromisoformat(row[0])
            if observed_at <= previous:
                observed_at = previous + timedelta(microseconds=1)
        return observed_at

    @staticmethod
    def _receipt_id(logical_effect_id: str, artifact_sha256: str) -> str:
        value = f"{logical_effect_id}\x00{artifact_sha256}".encode()
        return "receipt-" + hashlib.sha256(value).hexdigest()
