#!/usr/bin/env python
"""Replay classifier over a historical date range.

Usage:
    python scripts/backfill_classify.py --start 2020-01-01 --end 2026-04-22
    python scripts/backfill_classify.py --start 2020-01-01 --end 2026-04-22 --skip-insufficient

Reads DATABASE_URL from env. Idempotent: upserts on
(as_of_date, classifier_name, classifier_version).
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta

from markets_core.errors import InsufficientDataError
from markets_pipeline.classifiers.rule_based import RuleBasedClassifier
from markets_pipeline.stores.timescale import TimescaleWarehouse


def _parse_date(s: str) -> date:
    """Parse ISO date string to date object."""
    return date.fromisoformat(s)


def main(argv: list[str] | None = None) -> int:
    """Run classifier backfill over a date range.

    Returns:
        0 on success, 2 on InsufficientDataError (unless --skip-insufficient).
    """
    parser = argparse.ArgumentParser(
        description="Replay classifier over a historical date range."
    )
    parser.add_argument("--start", type=_parse_date, required=True,
                        help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=_parse_date, required=True,
                        help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--skip-insufficient",
        action="store_true",
        help="Continue past InsufficientDataError instead of aborting.",
    )
    args = parser.parse_args(argv)

    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("ERROR: DATABASE_URL environment variable not set", file=sys.stderr)
        return 1

    wh = TimescaleWarehouse(dsn=dsn)
    clf = RuleBasedClassifier(warehouse=wh)

    d = args.start
    attempted = 0
    written = 0
    skipped = 0
    errors = 0

    while d <= args.end:
        if d.weekday() < 5:  # business days only (0-4 = Mon-Fri)
            attempted += 1
            try:
                result = clf.classify(d)
                wh.upsert_regime_state({
                    "as_of_date": result.as_of_date,
                    "classifier_name": result.classifier_name,
                    "classifier_version": result.classifier_version,
                    "regime_name": result.regime_name,
                    "confidence": result.confidence,
                    "trigger_variables": result.trigger_variables,
                    "rationale": result.rationale,
                    "rationale_detail": result.rationale_detail,
                    "ontology_version": result.ontology_version,
                })
                written += 1
            except InsufficientDataError as e:
                if not args.skip_insufficient:
                    print(
                        f"ERROR: insufficient data at {d}: {e}",
                        file=sys.stderr,
                    )
                    return 2
                skipped += 1
            except Exception as e:
                errors += 1
                print(
                    f"ERROR: exception at {d}: {type(e).__name__}: {e}",
                    file=sys.stderr,
                )
                if not args.skip_insufficient:
                    return 1

        d = d + timedelta(days=1)

    print(
        f"backfill_classify: attempted={attempted} written={written} "
        f"skipped={skipped} errors={errors}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
