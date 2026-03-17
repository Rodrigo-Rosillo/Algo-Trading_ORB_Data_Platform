from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal, Mapping

Side = Literal["long", "short"]
TargetKind = Literal["orb_high", "orb_low", "entry_pct"]
StopKind = Literal["orb_high", "orb_low", "symmetric_to_target"]


@dataclass(frozen=True)
class ExecutionSpec:
    side: Side
    target_kind: TargetKind
    target_pct: float | None = None
    stop_kind: StopKind = "symmetric_to_target"

    def __post_init__(self) -> None:
        if self.target_kind == "entry_pct":
            if self.target_pct is None or float(self.target_pct) <= 0:
                raise ValueError("entry_pct execution specs require a positive target_pct")
        elif self.target_pct is not None:
            raise ValueError("target_pct is supported only when target_kind='entry_pct'")


@dataclass(frozen=True)
class ResolvedExecutionPlan:
    side: Side
    target_price: float
    stop_loss: float


def get_execution_spec(
    execution_specs: Mapping[str, ExecutionSpec],
    signal_type: str,
) -> ExecutionSpec:
    try:
        return execution_specs[str(signal_type)]
    except KeyError as exc:
        raise ValueError(f"Unsupported signal_type for execution: {signal_type!r}") from exc


def required_orb_fields(spec: ExecutionSpec) -> tuple[str, ...]:
    fields: list[str] = []
    if spec.target_kind == "orb_high":
        fields.append("orb_high")
    elif spec.target_kind == "orb_low":
        fields.append("orb_low")

    if spec.stop_kind == "orb_high":
        fields.append("orb_high")
    elif spec.stop_kind == "orb_low":
        fields.append("orb_low")

    return tuple(dict.fromkeys(fields))


def resolve_execution_plan(
    *,
    execution_spec: ExecutionSpec,
    entry_price: float,
    orb_high: float,
    orb_low: float,
) -> ResolvedExecutionPlan:
    entry = float(entry_price)
    high = float(orb_high)
    low = float(orb_low)
    spec = execution_spec

    if spec.target_kind == "orb_high":
        target_price = high
    elif spec.target_kind == "orb_low":
        target_price = low
    else:
        assert spec.target_pct is not None
        if spec.side == "long":
            target_price = entry * (1.0 + float(spec.target_pct))
        else:
            target_price = entry * (1.0 - float(spec.target_pct))

    if spec.stop_kind == "orb_high":
        stop_loss = high
    elif spec.stop_kind == "orb_low":
        stop_loss = low
    else:
        if spec.side == "long":
            distance = target_price - entry
            stop_loss = entry - distance
        else:
            distance = entry - target_price
            stop_loss = entry + distance

    return ResolvedExecutionPlan(
        side=spec.side,
        target_price=float(target_price),
        stop_loss=float(stop_loss),
    )


def serialize_execution_spec(spec: ExecutionSpec) -> dict[str, Any]:
    return {
        "side": spec.side,
        "target_kind": spec.target_kind,
        "target_pct": spec.target_pct,
        "stop_kind": spec.stop_kind,
    }


def serialize_execution_specs(
    execution_specs: Mapping[str, ExecutionSpec],
) -> dict[str, dict[str, Any]]:
    return {
        str(signal_type): serialize_execution_spec(spec)
        for signal_type, spec in execution_specs.items()
    }
