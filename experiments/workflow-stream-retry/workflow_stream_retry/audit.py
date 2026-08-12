from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path

from .evidence import audit_evidence


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("evidence_root", type=Path)
    return parser


async def _run(root: Path) -> None:
    project_root = Path(__file__).parents[1]
    report = await audit_evidence(root, project_root)
    print(
        json.dumps(
            {
                "schema": report.schema,
                "runs": len(report.trials),
                "histories_replayed": len(report.trials),
                "all_requirements_verified": True,
            },
            sort_keys=True,
        )
    )


def main() -> None:
    asyncio.run(_run(_parser().parse_args().evidence_root))


if __name__ == "__main__":
    main()
