from __future__ import annotations

from dataclasses import dataclass
from typing import Any


ACTIVITY_MULTIPLIERS = {
    "sedentary": 1.20,
    "lightly_active": 1.35,
    "moderately_active": 1.50,
    "very_active": 1.70,
}

GOAL_ADJUSTMENTS = {
    "maintain": {"slow": 0, "normal": 0, "fast": 0},
    "lose": {"slow": -250, "normal": -400, "fast": -550},
    "gain": {"slow": 200, "normal": 300, "fast": 450},
}

MEAL_SPLITS_3 = {
    "breakfast": 0.25,
    "lunch": 0.40,
    "dinner": 0.35,
}

MEAL_SPLITS_3_WITH_SNACK = {
    "breakfast": 0.22,
    "lunch": 0.33,
    "dinner": 0.30,
    "snack": 0.15,
}


@dataclass(frozen=True)
class NutritionTarget:
    kcal: float
    protein_g: float
    carbs_g: float
    fat_g: float
    slot_targets: dict[str, dict[str, float]]


def build_nutrition_target(profile: dict[str, Any]) -> NutritionTarget:
    weight_kg = _required_float(profile, "weight_kg")
    height_cm = _required_float(profile, "height_cm")
    age = _required_float(profile, "age")
    sex = str(profile.get("sex", "")).strip().lower()
    activity_level = str(profile.get("activity_level", "")).strip().lower()
    goal = str(profile.get("goal", "maintain")).strip().lower()
    goal_speed = str(profile.get("goal_speed", "normal")).strip().lower()

    bmr = _mifflin_st_jeor(weight_kg=weight_kg, height_cm=height_cm, age=age, sex=sex)
    activity_multiplier = _lookup_activity_multiplier(activity_level)
    kcal = bmr * activity_multiplier + _lookup_goal_adjustment(goal, goal_speed)

    protein_g = weight_kg * _protein_g_per_kg(profile, goal)
    fat_g = weight_kg * 0.8
    carbs_g = max((kcal - protein_g * 4 - fat_g * 9) / 4, 0)
    meal_splits = _meal_splits(profile)

    target = NutritionTarget(
        kcal=round(kcal, 1),
        protein_g=round(protein_g, 1),
        carbs_g=round(carbs_g, 1),
        fat_g=round(fat_g, 1),
        slot_targets={},
    )
    return NutritionTarget(
        kcal=target.kcal,
        protein_g=target.protein_g,
        carbs_g=target.carbs_g,
        fat_g=target.fat_g,
        slot_targets=_build_slot_targets(target, meal_splits),
    )


def _mifflin_st_jeor(weight_kg: float, height_cm: float, age: float, sex: str) -> float:
    base = 10 * weight_kg + 6.25 * height_cm - 5 * age
    if sex == "male":
        return base + 5
    if sex == "female":
        return base - 161
    raise ValueError(f"Sex neacceptat pentru Mifflin-St Jeor: {sex!r}")


def _lookup_activity_multiplier(activity_level: str) -> float:
    if activity_level not in ACTIVITY_MULTIPLIERS:
        raise ValueError(f"Nivel de activitate necunoscut: {activity_level!r}")
    return ACTIVITY_MULTIPLIERS[activity_level]


def _lookup_goal_adjustment(goal: str, goal_speed: str) -> int:
    if goal not in GOAL_ADJUSTMENTS:
        raise ValueError(f"Obiectiv necunoscut: {goal!r}")
    if goal_speed not in GOAL_ADJUSTMENTS[goal]:
        raise ValueError(f"Viteza obiectiv necunoscuta: {goal_speed!r}")
    return GOAL_ADJUSTMENTS[goal][goal_speed]


def _protein_g_per_kg(profile: dict[str, Any], goal: str) -> float:
    if goal == "lose":
        return 2.0
    if goal == "gain":
        return 1.8
    if goal == "maintain" and _uses_weights(profile):
        return 1.8
    if goal == "maintain":
        return 1.6
    raise ValueError(f"Obiectiv necunoscut: {goal!r}")


def _uses_weights(profile: dict[str, Any]) -> bool:
    training = profile.get("training") or {}
    training_type = str(training.get("type", "")).strip().lower()
    sessions_per_week = training.get("sessions_per_week") or 0
    return training_type == "weights" and float(sessions_per_week) > 0


def _meal_splits(profile: dict[str, Any]) -> dict[str, float]:
    meal_config = profile.get("meal_config") or {}
    day_structure = str(meal_config.get("day_structure", "")).strip().lower()
    include_snacks = bool(meal_config.get("include_snacks", False))
    meals_per_day = int(meal_config.get("meals_per_day", 3))

    if day_structure == "3_meals_plus_snack" or (meals_per_day == 3 and include_snacks):
        return MEAL_SPLITS_3_WITH_SNACK
    if meals_per_day == 3:
        return MEAL_SPLITS_3
    raise ValueError("Generator v1 suporta momentan doar 3 mese sau 3 mese + gustare.")


def _build_slot_targets(
    target: NutritionTarget,
    meal_splits: dict[str, float],
) -> dict[str, dict[str, float]]:
    return {
        slot: {
            "kcal": round(target.kcal * split, 1),
            "protein_g": round(target.protein_g * split, 1),
            "carbs_g": round(target.carbs_g * split, 1),
            "fat_g": round(target.fat_g * split, 1),
        }
        for slot, split in meal_splits.items()
    }


def _required_float(profile: dict[str, Any], key: str) -> float:
    if key not in profile:
        raise ValueError(f"Lipseste campul obligatoriu din profil: {key}")
    return float(profile[key])

