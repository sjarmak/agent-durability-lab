"""Strict JSON and shared-schema validation adapter."""

from __future__ import annotations

import json
import ntpath
import posixpath
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError
from referencing import Registry, Resource

from .errors import ContractValidationError
from .models import validate_utc_timestamp

type JsonScalar = str | int | float | bool | None
type JsonValue = JsonScalar | dict[str, JsonValue] | list[JsonValue]

_SCHEMA_KINDS = frozenset({"identity", "transition", "event", "evidence"})
MAX_JSON_DOCUMENT_BYTES = 4 << 20
MAX_JSON_DEPTH = 64
MAX_JSON_COLLECTION_ITEMS = 10_000


def _reject_duplicate_keys(pairs: list[tuple[str, JsonValue]]) -> dict[str, JsonValue]:
    result: dict[str, JsonValue] = {}
    for key, value in pairs:
        if key in result:
            raise ContractValidationError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _reject_nonfinite(value: str) -> JsonValue:
    raise ContractValidationError(f"non-finite JSON number is forbidden: {value}")


def loads_strict(data: str | bytes) -> JsonValue:
    """Decode one RFC-compliant JSON value, rejecting duplicate object keys."""

    if isinstance(data, bytes):
        if len(data) > MAX_JSON_DOCUMENT_BYTES:
            raise ContractValidationError("JSON document exceeds byte budget")
        try:
            data = data.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ContractValidationError("JSON input must be UTF-8") from error
    elif len(data.encode("utf-8")) > MAX_JSON_DOCUMENT_BYTES:
        raise ContractValidationError("JSON document exceeds byte budget")
    try:
        result = cast(
            JsonValue,
            json.loads(
                data,
                object_pairs_hook=_reject_duplicate_keys,
                parse_constant=_reject_nonfinite,
            ),
        )
        _validate_json_budget(result, 1)
        return result
    except (json.JSONDecodeError, UnicodeError, RecursionError) as error:
        raise ContractValidationError(f"invalid JSON: {error}") from error


def _validate_json_budget(value: JsonValue, depth: int) -> None:
    if depth > MAX_JSON_DEPTH:
        raise ContractValidationError("JSON document exceeds nesting budget")
    if isinstance(value, dict):
        if len(value) > MAX_JSON_COLLECTION_ITEMS:
            raise ContractValidationError("JSON object exceeds field budget")
        for child in value.values():
            _validate_json_budget(child, depth + 1)
    elif isinstance(value, list):
        if len(value) > MAX_JSON_COLLECTION_ITEMS:
            raise ContractValidationError("JSON array exceeds item budget")
        for child in value:
            _validate_json_budget(child, depth + 1)


@dataclass(frozen=True, slots=True)
class SchemaCorpus:
    """Validate strict JSON against the repository's v1 shared schemas."""

    schema_dir: Path
    _schemas: Mapping[str, Mapping[str, Any]] = field(init=False, repr=False)
    _registry: Registry[Any] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        schemas: dict[str, Mapping[str, Any]] = {}
        resources: list[tuple[str, Resource[Any]]] = []
        for kind in sorted(_SCHEMA_KINDS):
            path = self.schema_dir / f"{kind}.schema.json"
            document = loads_strict(path.read_bytes())
            if not isinstance(document, dict):
                raise ContractValidationError(f"schema must be an object: {path}")
            Draft202012Validator.check_schema(document)
            schemas[kind] = document
            schema_id = document.get("$id")
            if not isinstance(schema_id, str):
                raise ContractValidationError(f"schema must declare a string $id: {path}")
            resource = Resource.from_contents(document)
            resources.extend(((path.name, resource), (schema_id, resource)))
        object.__setattr__(self, "_schemas", MappingProxyType(schemas))
        object.__setattr__(self, "_registry", Registry().with_resources(resources))

    def validate_json(self, kind: str, data: str | bytes) -> JsonValue:
        if kind not in _SCHEMA_KINDS:
            raise ContractValidationError(f"unknown schema kind: {kind}")
        document = loads_strict(data)
        validator = Draft202012Validator(
            self._schemas[kind],
            registry=self._registry,
            format_checker=FormatChecker(),
        )
        try:
            validator.validate(document)
            self._validate_binding_rules(kind, document)
        except (ValidationError, ValueError, KeyError, TypeError) as error:
            raise ContractValidationError(f"{kind} is not v1 conformant: {error}") from error
        return document

    @staticmethod
    def _validate_binding_rules(kind: str, document: JsonValue) -> None:
        if not isinstance(document, dict):
            return
        if kind in {"transition", "event"}:
            validate_utc_timestamp(str(document["occurred_at"]))
        if kind == "transition":
            _validate_transition_equalities(document)
        elif kind == "evidence":
            _validate_evidence_equalities(document)
            _validate_evidence_paths(document)


def _validate_transition_equalities(document: dict[str, JsonValue]) -> None:
    request_hash = document["request_hash"]
    result = document.get("receipt", document.get("rejection"))
    if not isinstance(result, dict) or result.get("request_hash") != request_hash:
        raise ValueError("result request_hash must equal transition request_hash")
    before = document.get("before")
    after = document.get("after")
    decision = document["decision"]
    if not isinstance(decision, dict):
        raise TypeError("decision must be an object")
    if decision["applied"] is False and before != after:
        raise ValueError("rejected transition must preserve complete state")
    operation = document["operation"]
    if decision["disposition"] not in {"accepted", "replayed"}:
        return
    if operation == "replace":
        _validate_replace(before, after)
    elif operation != "claim":
        _validate_preserved_authority(before, after)


def _validate_replace(before: JsonValue | None, after: JsonValue | None) -> None:
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise TypeError("replace states must be objects")
    if after["generation"] != before["generation"] + 1:  # type: ignore[operator]
        raise ValueError("replace must increment generation exactly once")


def _validate_preserved_authority(before: JsonValue | None, after: JsonValue | None) -> None:
    if not isinstance(before, dict) or not isinstance(after, dict):
        raise TypeError("transition states must be objects")
    for field_name in ("generation", "owner_capability_digest"):
        if before[field_name] != after[field_name]:
            raise ValueError(f"non-replace transition must preserve {field_name}")


def _validate_evidence_equalities(document: dict[str, JsonValue]) -> None:
    identities = document["identities"]
    observations = document["observations"]
    if not isinstance(identities, dict) or not isinstance(observations, list):
        raise TypeError("evidence identities and observations have invalid shapes")
    previous_by_stream: dict[tuple[str, str], int] = {}
    for observation in observations:
        if not isinstance(observation, dict):
            raise TypeError("observation must be an object")
        for field_name in ("session_id", "turn_id", "operation_id", "effect_id"):
            if field_name in identities and observation.get(field_name) != identities[field_name]:
                raise ValueError(f"observation {field_name} does not match episode identity")
        source = observation["source"]
        if not isinstance(source, dict):
            raise TypeError("observation source must be an object")
        stream = (str(source["layer"]), str(source["component"]))
        sequence = int(observation["sequence"])  # type: ignore[arg-type]
        if sequence <= previous_by_stream.get(stream, 0):
            raise ValueError("observation sequence must be monotonic within its source stream")
        previous_by_stream[stream] = sequence


def _validate_evidence_paths(document: dict[str, JsonValue]) -> None:
    artifacts = document.get("artifacts")
    replay = document.get("replay")
    observations = document.get("observations")
    if not isinstance(artifacts, list) or not isinstance(replay, dict) or not isinstance(
        observations, list
    ):
        raise TypeError("evidence path containers have invalid shapes")
    for artifact in artifacts:
        if not isinstance(artifact, dict) or not _confined_artifact_path(artifact.get("path")):
            raise ValueError("artifact path must be confined")
    if not _confined_artifact_path(replay.get("history_path")):
        raise ValueError("history path must be confined")
    for observation in observations:
        if not isinstance(observation, dict):
            raise TypeError("observation must be an object")
        reference = observation.get("reference")
        if (
            isinstance(reference, dict)
            and "artifact_path" in reference
            and not _confined_artifact_path(reference["artifact_path"])
        ):
            raise ValueError("observation artifact path must be confined")


def _confined_artifact_path(value: JsonValue | None) -> bool:
    return (
        isinstance(value, str)
        and 1 <= len(value) <= 512
        and "\\" not in value
        and "\x00" not in value
        and not ntpath.splitdrive(value)[0]
        and not posixpath.isabs(value)
        and value == posixpath.normpath(value)
        and value != ".."
        and not value.startswith("../")
    )
