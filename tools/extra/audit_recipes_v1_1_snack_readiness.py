from __future__ import annotations

import csv
import argparse
import json
import math
import re
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

IN_DIR = REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_generator_ready_slot_checked_time_enriched"
RECIPES_PATH = IN_DIR / "recipes.csv"
NUTRITION_PATH = IN_DIR / "recipe_nutrition_cache.csv"
INGREDIENTS_PATH = IN_DIR / "recipe_ingredients.csv"

OUT_AUDIT = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_snack_readiness_audit.csv"
OUT_SUMMARY = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_snack_readiness_summary.txt"

AUDIT_COLUMNS = [
    "recipe_id",
    "display_name",
    "recipe_kind",
    "allowed_slots_json",
    "kcal_per_serving",
    "protein_g_per_serving",
    "carbs_g_per_serving",
    "fat_g_per_serving",
    "total_time_min",
    "effective_time_min_for_scoring",
    "snack_readiness_status",
    "snack_reason",
    "is_meal_like",
    "is_too_heavy_for_snack",
    "is_too_low_nutrition",
    "is_pet_food_or_out_of_scope",
    "is_breakfast_like",
    "is_appetizer_like",
    "ingredient_count",
]

PET_OR_OUT_OF_SCOPE = [
    "dog biscuit",
    "dog treat",
    "cat food",
    "pet food",
    "cocktail",
    "drink",
]
MEAL_LIKE = [
    "casserole",
    "stew",
    "lasagna",
    "stuffed pepper",
    "stuffed cabbage",
    "fried rice",
    "skillet",
    "sandwich",
    "meatloaf",
    "chili",
    "soup",
    "main",
]
BREAKFAST_LIKE = [
    "breakfast",
    "waffle",
    "pancake",
    "omelet",
    "omelette",
    "frittata",
    "oatmeal",
    "french toast",
]
APPETIZER_LIKE = [
    "hummus",
    "deviled egg",
    "chickpea",
    "butter beans",
    "appetizer",
    "snack",
]


def read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"Lipseste fisierul asteptat: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader), list(reader.fieldnames or [])


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def clean_text(value: object) -> str:
    return str(value or "").strip()


def parse_float(value: object) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def normalize_text(value: object) -> str:
    text = clean_text(value).lower().replace("_", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def contains_phrase(text: str, phrase: str) -> bool:
    normalized = normalize_text(phrase)
    if not normalized:
        return False
    pattern = rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])"
    return re.search(pattern, text) is not None


def has_any(text: str, phrases: list[str]) -> bool:
    return any(contains_phrase(text, phrase) for phrase in phrases)


def index_by_recipe_id(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {clean_text(row.get("recipe_id")): row for row in rows if clean_text(row.get("recipe_id"))}


def ingredient_counts(rows: list[dict[str, str]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        recipe_id = clean_text(row.get("recipe_id"))
        if recipe_id:
            counts[recipe_id] += 1
    return counts


def allowed_slots(row: dict[str, str]) -> list[str]:
    text = clean_text(row.get("allowed_slots_json"))
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [clean_text(item).lower() for item in parsed if clean_text(item)]


def snack_rows(recipes: list[dict[str, str]]) -> list[dict[str, str]]:
    return [row for row in recipes if "snack" in allowed_slots(row)]


def classify_snack(
    recipe: dict[str, str],
    nutrition: dict[str, str],
    ingredient_count: int,
) -> dict[str, object]:
    recipe_id = clean_text(recipe.get("recipe_id"))
    display_name = clean_text(recipe.get("display_name"))
    recipe_kind = clean_text(recipe.get("recipe_kind"))
    search_text = normalize_text(
        " ".join(
            [
                display_name,
                recipe_kind,
                clean_text(recipe.get("recipe_category")),
                clean_text(recipe.get("recipe_subcategory")),
                clean_text(recipe.get("slot_policy_reason")),
                clean_text(recipe.get("qc_notes")),
            ]
        )
    )
    kcal = parse_float(nutrition.get("energy_kcal_per_serving"))
    protein = parse_float(nutrition.get("protein_g_per_serving"))
    carbs = parse_float(nutrition.get("carbs_g_per_serving"))
    fat = parse_float(nutrition.get("fat_g_per_serving"))
    total_time = parse_float(recipe.get("total_time_min"))
    effective_time = parse_float(recipe.get("effective_time_min_for_scoring") or recipe.get("total_time_min"))

    is_pet_or_out = has_any(search_text, PET_OR_OUT_OF_SCOPE)
    is_manual_curated = (
        contains_phrase(search_text, "manual snack v1 1 round12")
        or clean_text(recipe.get("recipe_subcategory")) == "manual_curated"
    )
    raw_meal_like = has_any(search_text, MEAL_LIKE)
    is_breakfast_like = has_any(search_text, BREAKFAST_LIKE)
    is_appetizer_like = has_any(search_text, APPETIZER_LIKE) or recipe_kind == "snack"
    is_too_heavy = bool((kcal is not None and kcal > 350) or (effective_time is not None and effective_time > 30))
    is_meal_like = raw_meal_like and not (
        is_manual_curated
        and recipe_kind == "snack"
        and not is_too_heavy
    )
    is_too_low = bool(
        kcal is None
        or kcal < 80
        or (
            protein is not None
            and protein < 1
            and (carbs is None or carbs < 5)
        )
    )
    reasons: list[str] = []
    status = "snack_ready"

    if is_pet_or_out:
        status = "snack_exclude"
        reasons.append("out_of_scope")
    elif is_meal_like:
        status = "snack_exclude"
        reasons.append("meal_like")
    elif is_too_heavy and not is_appetizer_like:
        status = "snack_exclude"
        reasons.append("too_heavy_not_appetizer_like")
    elif is_too_heavy:
        status = "snack_review"
        reasons.append("snack_semantics_but_heavy_or_slow")
    elif is_too_low:
        status = "snack_review"
        reasons.append("nutrition_or_cache_suspicious")
    elif is_breakfast_like:
        status = "snack_review"
        reasons.append("breakfast_like_can_work_as_snack_but_review")
    else:
        reasons.append("snack_semantics_and_basic_thresholds_ok")

    return {
        "recipe_id": recipe_id,
        "display_name": display_name,
        "recipe_kind": recipe_kind,
        "allowed_slots_json": clean_text(recipe.get("allowed_slots_json")),
        "kcal_per_serving": clean_text(nutrition.get("energy_kcal_per_serving")),
        "protein_g_per_serving": clean_text(nutrition.get("protein_g_per_serving")),
        "carbs_g_per_serving": clean_text(nutrition.get("carbs_g_per_serving")),
        "fat_g_per_serving": clean_text(nutrition.get("fat_g_per_serving")),
        "total_time_min": clean_text(recipe.get("total_time_min")),
        "effective_time_min_for_scoring": clean_text(recipe.get("effective_time_min_for_scoring")),
        "snack_readiness_status": status,
        "snack_reason": "|".join(reasons),
        "is_meal_like": str(is_meal_like),
        "is_too_heavy_for_snack": str(is_too_heavy),
        "is_too_low_nutrition": str(is_too_low),
        "is_pet_food_or_out_of_scope": str(is_pet_or_out),
        "is_breakfast_like": str(is_breakfast_like),
        "is_appetizer_like": str(is_appetizer_like),
        "ingredient_count": ingredient_count,
    }


def counter_text(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key}: {value}" for key, value in counter.most_common()]


def build_summary(rows: list[dict[str, object]]) -> str:
    status_counts = Counter(clean_text(row.get("snack_readiness_status")) for row in rows)
    ready_count = status_counts.get("snack_ready", 0)
    lines = [
        "Recipes_DB v1.1 snack readiness audit",
        "=" * 39,
        "",
        f"snack_candidates_audited: {len(rows)}",
        "",
        "Snack readiness counts:",
    ]
    lines.extend(counter_text(status_counts))
    lines.extend(["", "Snack candidates:"])
    for row in rows:
        lines.append(
            "- "
            + clean_text(row.get("recipe_id"))
            + " | "
            + clean_text(row.get("display_name"))
            + " | "
            + clean_text(row.get("snack_readiness_status"))
            + " | "
            + clean_text(row.get("snack_reason"))
        )
    lines.extend(["", "Recommendation:"])
    if ready_count < 6:
        lines.append("- snack_ready_count_below_6: snack pool is too small; add/review snack recipes manually later.")
    else:
        lines.append("- snack_ready_count_at_least_6: pool is minimally usable, but still small.")
    lines.append("- no rows were removed by this audit.")
    return "\n".join(lines) + "\n"


def output_paths(output_suffix: str) -> tuple[Path, Path]:
    suffix = clean_text(output_suffix)
    if suffix == "snack_curated":
        return (
            REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_snack_curated_readiness_audit.csv",
            REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_snack_curated_readiness_summary.txt",
        )
    if suffix:
        safe_suffix = re.sub(r"[^a-zA-Z0-9_]+", "_", suffix).strip("_")
        return (
            REPO_ROOT / "data" / "recipesdb" / "audit" / f"recipes_v1_1_{safe_suffix}_snack_readiness_audit.csv",
            REPO_ROOT / "data" / "recipesdb" / "audit" / f"recipes_v1_1_{safe_suffix}_snack_readiness_summary.txt",
        )
    return OUT_AUDIT, OUT_SUMMARY


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit snack readiness pentru Recipes_DB v1.1 draft.")
    parser.add_argument("--recipes", default=RECIPES_PATH, type=Path)
    parser.add_argument("--nutrition", default=NUTRITION_PATH, type=Path)
    parser.add_argument("--ingredients", default=INGREDIENTS_PATH, type=Path)
    parser.add_argument("--output_suffix", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out_audit, out_summary = output_paths(args.output_suffix)
    recipes, _ = read_csv(args.recipes)
    nutrition_rows, _ = read_csv(args.nutrition)
    ingredient_rows, _ = read_csv(args.ingredients)
    nutrition_by_id = index_by_recipe_id(nutrition_rows)
    ingredient_count_by_id = ingredient_counts(ingredient_rows)

    rows = [
        classify_snack(
            recipe,
            nutrition_by_id.get(clean_text(recipe.get("recipe_id")), {}),
            ingredient_count_by_id.get(clean_text(recipe.get("recipe_id")), 0),
        )
        for recipe in snack_rows(recipes)
    ]
    write_csv(out_audit, rows, AUDIT_COLUMNS)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_summary.write_text(build_summary(rows), encoding="utf-8")

    counts = Counter(clean_text(row.get("snack_readiness_status")) for row in rows)
    print("Recipes_DB v1.1 snack readiness audit written")
    print(f"snack_candidates_audited={len(rows)}")
    print(f"snack_readiness_counts={dict(counts)}")
    print(f"written_audit={out_audit}")
    print(f"written_summary={out_summary}")


if __name__ == "__main__":
    main()
