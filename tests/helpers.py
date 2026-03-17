from __future__ import annotations

from execution_specs import ExecutionSpec

LONG_ENTRY_SIGNAL = "long_entry"
SHORT_ENTRY_SIGNAL = "short_entry"
LONG_PCT_SIGNAL = "long_pct_entry"
SHORT_ORB_SIGNAL = "short_orb_entry"


def build_test_execution_specs() -> dict[str, ExecutionSpec]:
    return {
        LONG_ENTRY_SIGNAL: ExecutionSpec(
            side="long",
            target_kind="orb_high",
            stop_kind="symmetric_to_target",
        ),
        SHORT_ENTRY_SIGNAL: ExecutionSpec(
            side="short",
            target_kind="entry_pct",
            target_pct=0.02,
            stop_kind="orb_high",
        ),
        LONG_PCT_SIGNAL: ExecutionSpec(
            side="long",
            target_kind="entry_pct",
            target_pct=0.02,
            stop_kind="orb_low",
        ),
        SHORT_ORB_SIGNAL: ExecutionSpec(
            side="short",
            target_kind="orb_low",
            stop_kind="orb_high",
        ),
    }
