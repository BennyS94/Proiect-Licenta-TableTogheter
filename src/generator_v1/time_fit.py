from __future__ import annotations

import math


def base_time_fit(total_time_min: object, slot: str) -> float:
    total_time = _to_float(total_time_min)
    if total_time is None:
        return 0.0

    if slot == "snack":
        if total_time <= 10:
            return 1.00
        if total_time <= 20:
            return 0.60
        return 0.10

    if total_time <= 15:
        return 1.00
    if total_time <= 30:
        return 0.80
    if total_time <= 45:
        return 0.55
    if total_time <= 60:
        return 0.25
    return 0.05


def household_time_fit(
    total_time_min: object,
    slot: str,
    time_sensitivity: str = "normal",
) -> float:
    base_score = base_time_fit(total_time_min, slot)
    sensitivity = str(time_sensitivity or "normal").strip().lower()
    if sensitivity not in {"low", "normal", "high"}:
        raise ValueError(f"Sensibilitate timp necunoscuta: {time_sensitivity!r}")

    # TODO: ajusteaza low/high dupa ce profilul gospodariei are reguli de timp clare.
    return _clamp_01(base_score)


def _to_float(value: object) -> float | None:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric_value):
        return None
    return numeric_value


def _clamp_01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))

