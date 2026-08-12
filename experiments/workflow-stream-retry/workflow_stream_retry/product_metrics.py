from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProtocolSurface:
    protocol_lines: int
    state_fields: int
    branches: int
    sdk_operations: tuple[str, ...]


@dataclass(frozen=True)
class RecoverySurfaceMetrics:
    definition: str
    manual: ProtocolSurface
    product: ProtocolSurface


_MANUAL_DEFINITIONS = {
    "ManualLogicalOutputPublisher",
    "ManualLogicalOutputReconstructor",
    "validate_manual_acknowledgement",
    "_update_hash",
}
_SDK_OPERATIONS = {
    "logical_output_publisher",
    "logical_output_reconstructor",
    "validate_logical_output_acknowledgement",
}


def measure_recovery_surface(project_root: Path) -> RecoverySurfaceMetrics:
    """Count application-owned protocol code, excluding SDK implementation.

    Protocol lines are nonblank, non-comment source lines in the manual
    publisher, reconstructor, exact acknowledgement validator, and hash helper.
    State fields are distinct ``self`` attributes assigned by those definitions;
    branches are AST conditional nodes. The product arm owns no corresponding
    protocol implementation and delegates through the three recorded SDK calls.
    """
    package = project_root / "workflow_stream_retry"
    manual_path = package / "product_manual.py"
    source = manual_path.read_text()
    tree = ast.parse(source, filename=str(manual_path))
    selected = [
        node
        for node in tree.body
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in _MANUAL_DEFINITIONS
    ]
    if {node.name for node in selected} != _MANUAL_DEFINITIONS:
        raise ValueError("manual recovery surface definitions differ")
    lines = source.splitlines()
    covered_lines = {
        number
        for node in selected
        for number in range(node.lineno, (node.end_lineno or node.lineno) + 1)
        if lines[number - 1].strip() and not lines[number - 1].lstrip().startswith("#")
    }
    assigned_fields = {
        child.attr
        for node in selected
        for child in ast.walk(node)
        if isinstance(child, ast.Attribute)
        and isinstance(child.value, ast.Name)
        and child.value.id == "self"
        and isinstance(child.ctx, ast.Store)
    }
    branch_nodes = (ast.If, ast.IfExp, ast.Match, ast.Try)
    branches = sum(isinstance(child, branch_nodes) for node in selected for child in ast.walk(node))
    operations: set[str] = set()
    for name in ("product_activity.py", "product_runner.py", "product_workflow.py"):
        candidate = ast.parse((package / name).read_text(), filename=name)
        operations.update(
            child.func.attr
            for child in ast.walk(candidate)
            if isinstance(child, ast.Call)
            and isinstance(child.func, ast.Attribute)
            and child.func.attr in _SDK_OPERATIONS
        )
    if operations != _SDK_OPERATIONS:
        raise ValueError("product SDK recovery operation set differs")
    return RecoverySurfaceMetrics(
        definition=(
            "Application-owned protocol implementation only; SDK internals and "
            "experiment/oracle code are excluded. Branches are AST If, IfExp, "
            "Match, and Try nodes."
        ),
        manual=ProtocolSurface(
            protocol_lines=len(covered_lines),
            state_fields=len(assigned_fields),
            branches=branches,
            sdk_operations=(),
        ),
        product=ProtocolSurface(
            protocol_lines=0,
            state_fields=0,
            branches=0,
            sdk_operations=tuple(sorted(operations)),
        ),
    )
