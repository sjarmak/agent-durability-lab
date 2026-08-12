from __future__ import annotations

from pathlib import Path

import pytest

from temporal_coding_agent import (
    MAX_JSON_DEPTH,
    MAX_JSON_DOCUMENT_BYTES,
    ContractValidationError,
    SchemaCorpus,
    loads_strict,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "raw",
    [
        '{"event_id":"first","event_id":"second"}',
        '{"value": NaN}',
        '{"value": Infinity}',
    ],
)
def test_strict_json_rejects_duplicate_keys_and_nonfinite_numbers(raw: str) -> None:
    with pytest.raises(ContractValidationError):
        loads_strict(raw)


@pytest.mark.integration
def test_shared_schema_corpus_accepts_every_valid_fixture(spec_root: Path) -> None:
    corpus = SchemaCorpus(spec_root / "schema")

    fixtures = [
        *sorted((spec_root / "fixtures" / "valid" / "identity").glob("*.json")),
        *sorted((spec_root / "fixtures" / "valid" / "transition").glob("*.json")),
        *sorted((spec_root / "fixtures" / "valid" / "event").glob("*.json")),
        *sorted((spec_root / "fixtures" / "valid" / "evidence").glob("*.json")),
        *sorted((spec_root / "fixtures" / "valid-result" / "transition").glob("*.json")),
    ]

    for fixture in fixtures:
        kind = fixture.parent.name
        if fixture.parent.parent.name == "valid-result":
            kind = "transition"
        corpus.validate_json(kind, fixture.read_bytes())


@pytest.mark.integration
def test_shared_schema_corpus_rejects_every_invalid_fixture(spec_root: Path) -> None:
    corpus = SchemaCorpus(spec_root / "schema")

    for fixture in sorted((spec_root / "fixtures" / "invalid").glob("*/*.json")):
        with pytest.raises(ContractValidationError):
            corpus.validate_json(fixture.parent.name, fixture.read_bytes())


@pytest.mark.integration
def test_schema_adapter_rejects_unsafe_paths_and_resource_exhaustion(spec_root: Path) -> None:
    corpus = SchemaCorpus(spec_root / "schema")
    evidence = (spec_root / "fixtures" / "valid" / "evidence" / "evidence.json").read_text()
    traversal = evidence.replace(
        "evidence/episode-protected-001/events.jsonl", "../../etc/passwd"
    )
    absolute = evidence.replace(
        "evidence/episode-protected-001/history.json", "/etc/passwd"
    )
    windows_absolute = evidence.replace(
        "evidence/episode-protected-001/history.json", "C:/Windows/System32"
    )
    for document in (traversal, absolute, windows_absolute):
        with pytest.raises(ContractValidationError):
            corpus.validate_json("evidence", document)
    with pytest.raises(ContractValidationError):
        loads_strict(b'"' + b"x" * MAX_JSON_DOCUMENT_BYTES + b'"')
    with pytest.raises(ContractValidationError):
        loads_strict("[" * (MAX_JSON_DEPTH + 1) + "0" + "]" * (MAX_JSON_DEPTH + 1))
