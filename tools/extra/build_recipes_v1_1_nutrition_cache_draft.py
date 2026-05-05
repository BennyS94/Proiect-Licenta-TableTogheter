from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_RECIPES = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_curated_200.csv"
DEFAULT_PARSED_INGREDIENTS = (
    REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_ingredients_parsed_unit_rules.csv"
)
DEFAULT_MAPPING = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "draft"
    / "recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_round2_unit_rules_review_promotions.csv"
)
DEFAULT_FOODDB = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round2.csv"

OUT_CACHE = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_nutrition_cache_draft.csv"
OUT_SUMMARY = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_nutrition_cache_summary.txt"
OUT_RECIPE_AUDIT = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_nutrition_cache_recipe_audit.csv"
OUT_LOW_COVERAGE = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_nutrition_cache_low_coverage.csv"
OUT_CONTRIBUTIONS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_ingredient_nutrition_contributions.csv"
)

CACHE_VERSION = "recipes_v1_1_draft_001"
NUTRITION_BASIS = "v1_1_mapped_ingredients_draft"

USABLE_MIN_ACCEPTED_WITH_GRAMS = 3
USABLE_MIN_MAPPED_WEIGHT_RATIO = 0.50
USABLE_MIN_KCAL_PER_SERVING = 100.0
LOW_RATIO_THRESHOLD = 0.40
LOW_MAIN_KCAL_THRESHOLD = 150.0
LOW_PROTEIN_THRESHOLD = 10.0
HIGH_KCAL_PER_SERVING_THRESHOLD = 1200.0
MAPPING_REVIEW_BLOCKER_COUNT = 3

MAIN_KINDS = {"complete_main", "near_complete_main", "soup", "salad"}
COMPONENT_KINDS = {"protein_component", "carb_side", "veg_side", "component"}
PROTEIN_SIGNALS = {"chicken", "turkey", "fish", "beef", "pork", "egg"}

CACHE_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "recipe_kind_guess",
    "primary_protein",
    "nutrition_basis",
    "servings_basis",
    "uses_pilot_servings_fallback",
    "servings_estimation_method",
    "servings_estimation_reasons",
    "total_weight_grams_estimated",
    "mapped_weight_grams",
    "known_weight_grams_sum",
    "mapped_weight_ratio",
    "energy_kcal_total",
    "protein_g_total",
    "carbs_g_total",
    "fat_g_total",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "carbs_g_per_serving",
    "fat_g_per_serving",
    "ingredient_count",
    "accepted_auto_count",
    "accepted_auto_with_grams_count",
    "review_needed_count",
    "review_needed_with_grams_count",
    "unmapped_count",
    "unmapped_with_grams_count",
    "cache_status",
    "quality_flags",
    "cache_version",
]

CONTRIBUTION_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "ingredient_position",
    "ingredient_raw_text",
    "ingredient_name_normalized",
    "quantity_grams_estimated",
    "mapped_food_id",
    "mapped_food_canonical_name",
    "energy_kcal_contribution",
    "protein_g_contribution",
    "carbs_g_contribution",
    "fat_g_contribution",
    "contribution_status",
]


def read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"Lipseste fisierul asteptat: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return rows, list(reader.fieldnames or [])


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def add_output_suffix(path: Path, output_suffix: str) -> Path:
    suffix = clean_text(output_suffix)
    if not suffix:
        return path
    return path.with_name(f"{path.stem}_{suffix}{path.suffix}")


def resolve_output_path(explicit_path: str, default_path: Path, output_suffix: str) -> Path:
    if clean_text(explicit_path):
        return Path(explicit_path)
    return add_output_suffix(default_path, output_suffix)


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


def parse_positive_float(value: object) -> float | None:
    parsed = parse_float(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def format_number(value: float | None, digits: int = 4) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}".rstrip("0").rstrip(".")


def is_true(value: object) -> bool:
    return clean_text(value).casefold() in {"1", "true", "yes", "y"}


def get_recipe_kind(recipe_row: dict[str, str]) -> str:
    return clean_text(recipe_row.get("recipe_kind_guess"))


def is_complete_or_near_complete(recipe_row: dict[str, str]) -> bool:
    kind = get_recipe_kind(recipe_row)
    return kind in {"complete_main", "near_complete_main"} or is_true(
        recipe_row.get("is_complete_or_near_complete_meal")
    )


def is_protein_relevant_recipe(recipe_row: dict[str, str]) -> bool:
    kind = get_recipe_kind(recipe_row)
    primary_protein = clean_text(recipe_row.get("primary_protein")).casefold()
    return kind in MAIN_KINDS or kind == "protein_component" or primary_protein in PROTEIN_SIGNALS


def first_explicit_servings(recipe_row: dict[str, str]) -> tuple[float | None, str]:
    for column in ("servings_normalized", "servings_basis", "servings"):
        value = parse_positive_float(recipe_row.get(column))
        if value is not None:
            return value, f"explicit_{column}"

    declared = clean_text(recipe_row.get("servings_declared"))
    if declared:
        parts = declared.replace("-", " ").replace("to", " ").split()
        for part in parts:
            value = parse_positive_float(part)
            if value is not None:
                return value, "explicit_servings_declared_parse"
    return None, ""


def estimate_servings(recipe_row: dict[str, str], known_weight_grams_sum: float) -> tuple[float, bool, str, list[str]]:
    explicit_servings, explicit_method = first_explicit_servings(recipe_row)
    if explicit_servings is not None:
        return explicit_servings, False, explicit_method, [explicit_method]

    kind = get_recipe_kind(recipe_row)
    reasons = ["pilot_servings_fallback"]

    if known_weight_grams_sum >= 2400:
        servings = 8.0
        reasons.append("known_weight_grams_sum_gte_2400")
    elif known_weight_grams_sum >= 1600:
        servings = 6.0
        reasons.append("known_weight_grams_sum_gte_1600")
    elif kind in MAIN_KINDS:
        servings = 4.0
        reasons.append(f"default_{kind}_4_servings")
    elif kind == "breakfast":
        servings = 2.0
        reasons.append("default_breakfast_2_servings")
    elif kind == "snack":
        if known_weight_grams_sum <= 350:
            servings = 1.0
            reasons.append("snack_weight_lte_350g_1_serving")
        else:
            servings = 2.0
            reasons.append("snack_weight_gt_350g_2_servings")
    elif kind in COMPONENT_KINDS:
        servings = 4.0
        reasons.append(f"default_{kind}_4_servings")
    else:
        servings = 4.0
        reasons.append("default_unknown_kind_4_servings")

    if known_weight_grams_sum <= 300 and kind not in MAIN_KINDS:
        servings = 1.0
        reasons.append("small_non_main_weight_lte_300g_1_serving")

    return servings, True, "pilot_fallback_recipe_kind_weight", reasons


def build_fooddb_lookup(food_rows: list[dict[str, str]]) -> dict[str, dict[str, float]]:
    lookup: dict[str, dict[str, float]] = {}
    for row in food_rows:
        food_id = clean_text(row.get("food_id"))
        if not food_id:
            continue
        macros = {
            "energy": parse_float(row.get("energy_kcal_100g")),
            "protein": parse_float(row.get("protein_g_100g")),
            "carbs": parse_float(row.get("carbs_g_100g")),
            "fat": parse_float(row.get("fat_g_100g")),
        }
        if any(value is None for value in macros.values()):
            continue
        lookup[food_id] = {
            "energy": float(macros["energy"]),
            "protein": float(macros["protein"]),
            "carbs": float(macros["carbs"]),
            "fat": float(macros["fat"]),
        }
    return lookup


def contribution_status(
    mapping_row: dict[str, str],
    grams: float | None,
    fooddb_lookup: dict[str, dict[str, float]],
) -> str:
    mapping_status = clean_text(mapping_row.get("mapping_status"))
    mapped_food_id = clean_text(mapping_row.get("mapped_food_id"))
    if mapping_status != "accepted_auto":
        if not mapped_food_id:
            return "skipped_no_mapping"
        return "skipped_not_accepted_auto"
    if not mapped_food_id:
        return "skipped_no_mapping"
    if grams is None or grams <= 0:
        return "skipped_no_grams"
    if mapped_food_id not in fooddb_lookup:
        return "skipped_no_fooddb_macros"
    return "used"


def compute_contributions(
    mapping_rows: list[dict[str, str]],
    fooddb_lookup: dict[str, dict[str, float]],
) -> tuple[list[dict[str, object]], dict[str, dict[str, float]]]:
    contribution_rows: list[dict[str, object]] = []
    recipe_totals: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "energy_kcal_total": 0.0,
            "protein_g_total": 0.0,
            "carbs_g_total": 0.0,
            "fat_g_total": 0.0,
            "mapped_weight_grams": 0.0,
            "accepted_mapped_ingredient_count": 0.0,
            "accepted_mapped_with_grams_count": 0.0,
        }
    )

    for row in mapping_rows:
        recipe_id = clean_text(row.get("recipe_id_candidate"))
        grams = parse_positive_float(row.get("quantity_grams_estimated"))
        status = contribution_status(row, grams, fooddb_lookup)
        energy = protein = carbs = fat = None

        if clean_text(row.get("mapping_status")) == "accepted_auto":
            recipe_totals[recipe_id]["accepted_mapped_ingredient_count"] += 1

        if status == "used":
            food_id = clean_text(row.get("mapped_food_id"))
            assert grams is not None
            macros = fooddb_lookup[food_id]
            energy = grams * macros["energy"] / 100
            protein = grams * macros["protein"] / 100
            carbs = grams * macros["carbs"] / 100
            fat = grams * macros["fat"] / 100

            recipe_totals[recipe_id]["energy_kcal_total"] += energy
            recipe_totals[recipe_id]["protein_g_total"] += protein
            recipe_totals[recipe_id]["carbs_g_total"] += carbs
            recipe_totals[recipe_id]["fat_g_total"] += fat
            recipe_totals[recipe_id]["mapped_weight_grams"] += grams
            recipe_totals[recipe_id]["accepted_mapped_with_grams_count"] += 1

        contribution_rows.append(
            {
                "recipe_id_candidate": recipe_id,
                "display_name": clean_text(row.get("display_name")),
                "ingredient_position": clean_text(row.get("ingredient_position")),
                "ingredient_raw_text": clean_text(row.get("ingredient_raw_text")),
                "ingredient_name_normalized": clean_text(row.get("ingredient_name_normalized")),
                "quantity_grams_estimated": format_number(grams),
                "mapped_food_id": clean_text(row.get("mapped_food_id")),
                "mapped_food_canonical_name": clean_text(row.get("mapped_food_canonical_name")),
                "energy_kcal_contribution": format_number(energy),
                "protein_g_contribution": format_number(protein),
                "carbs_g_contribution": format_number(carbs),
                "fat_g_contribution": format_number(fat),
                "contribution_status": status,
            }
        )

    return contribution_rows, recipe_totals


def build_recipe_coverage(mapping_rows: list[dict[str, str]]) -> dict[str, dict[str, float]]:
    coverage: dict[str, dict[str, float]] = defaultdict(
        lambda: {
            "ingredient_count": 0.0,
            "accepted_auto_count": 0.0,
            "review_needed_count": 0.0,
            "unmapped_count": 0.0,
            "ingredients_with_grams_count": 0.0,
            "accepted_auto_with_grams_count": 0.0,
            "review_needed_with_grams_count": 0.0,
            "unmapped_with_grams_count": 0.0,
            "known_weight_grams_sum": 0.0,
        }
    )
    for row in mapping_rows:
        recipe_id = clean_text(row.get("recipe_id_candidate"))
        status = clean_text(row.get("mapping_status"))
        grams = parse_positive_float(row.get("quantity_grams_estimated"))
        coverage[recipe_id]["ingredient_count"] += 1
        if status == "accepted_auto":
            coverage[recipe_id]["accepted_auto_count"] += 1
        elif status == "review_needed":
            coverage[recipe_id]["review_needed_count"] += 1
        elif status == "unmapped":
            coverage[recipe_id]["unmapped_count"] += 1

        if grams is not None:
            coverage[recipe_id]["ingredients_with_grams_count"] += 1
            coverage[recipe_id]["known_weight_grams_sum"] += grams
            if status == "accepted_auto":
                coverage[recipe_id]["accepted_auto_with_grams_count"] += 1
            elif status == "review_needed":
                coverage[recipe_id]["review_needed_with_grams_count"] += 1
            elif status == "unmapped":
                coverage[recipe_id]["unmapped_with_grams_count"] += 1
    return coverage


def choose_cache_status(coverage: dict[str, float], mapped_weight_ratio: float | None, kcal_per_serving: float) -> str:
    accepted_count = int(coverage["accepted_auto_count"])
    accepted_with_grams = int(coverage["accepted_auto_with_grams_count"])
    if accepted_count == 0:
        return "no_accepted_mapped_ingredients"
    if accepted_with_grams == 0:
        return "mapped_without_weight_estimates"
    if (
        accepted_with_grams >= USABLE_MIN_ACCEPTED_WITH_GRAMS
        and mapped_weight_ratio is not None
        and mapped_weight_ratio >= USABLE_MIN_MAPPED_WEIGHT_RATIO
        and kcal_per_serving >= USABLE_MIN_KCAL_PER_SERVING
    ):
        return "usable_from_mapped_ingredients"
    return "partial_from_mapped_ingredients"


def build_quality_flags(
    recipe_row: dict[str, str],
    coverage: dict[str, float],
    mapped_weight_ratio: float | None,
    kcal_per_serving: float,
    protein_per_serving: float,
) -> list[str]:
    flags: list[str] = []
    if is_complete_or_near_complete(recipe_row) and kcal_per_serving < LOW_MAIN_KCAL_THRESHOLD:
        flags.append("is_low_kcal_suspicious")
    if is_protein_relevant_recipe(recipe_row) and protein_per_serving < LOW_PROTEIN_THRESHOLD:
        flags.append("is_low_protein_suspicious")
    if mapped_weight_ratio is None:
        flags.append("mapped_weight_ratio_missing")
    elif mapped_weight_ratio < LOW_RATIO_THRESHOLD:
        flags.append("is_low_mapped_weight_ratio")
    if kcal_per_serving > HIGH_KCAL_PER_SERVING_THRESHOLD:
        flags.append("is_high_macro_suspicious")
    blocker_count = int(coverage["review_needed_with_grams_count"] + coverage["unmapped_with_grams_count"])
    if blocker_count >= MAPPING_REVIEW_BLOCKER_COUNT:
        flags.append("needs_mapping_review")
    return flags


def build_cache_rows(
    recipes: list[dict[str, str]],
    mapping_rows: list[dict[str, str]],
    fooddb_lookup: dict[str, dict[str, float]],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    contribution_rows, recipe_totals = compute_contributions(mapping_rows, fooddb_lookup)
    coverage_by_recipe = build_recipe_coverage(mapping_rows)
    cache_rows: list[dict[str, object]] = []
    recipe_audit_rows: list[dict[str, object]] = []
    low_coverage_rows: list[dict[str, object]] = []

    for recipe in recipes:
        recipe_id = clean_text(recipe.get("recipe_id_candidate"))
        totals = recipe_totals[recipe_id]
        coverage = coverage_by_recipe[recipe_id]
        known_weight = coverage["known_weight_grams_sum"]
        mapped_weight = totals["mapped_weight_grams"]
        mapped_weight_ratio = mapped_weight / known_weight if known_weight > 0 else None

        servings, uses_fallback, servings_method, servings_reasons = estimate_servings(recipe, known_weight)
        energy_total = totals["energy_kcal_total"]
        protein_total = totals["protein_g_total"]
        carbs_total = totals["carbs_g_total"]
        fat_total = totals["fat_g_total"]

        energy_per_serving = energy_total / servings if servings > 0 else 0.0
        protein_per_serving = protein_total / servings if servings > 0 else 0.0
        carbs_per_serving = carbs_total / servings if servings > 0 else 0.0
        fat_per_serving = fat_total / servings if servings > 0 else 0.0

        cache_status = choose_cache_status(coverage, mapped_weight_ratio, energy_per_serving)
        quality_flags = build_quality_flags(recipe, coverage, mapped_weight_ratio, energy_per_serving, protein_per_serving)

        row = {
            "recipe_id_candidate": recipe_id,
            "display_name": clean_text(recipe.get("display_name")),
            "recipe_kind_guess": clean_text(recipe.get("recipe_kind_guess")),
            "primary_protein": clean_text(recipe.get("primary_protein")),
            "nutrition_basis": NUTRITION_BASIS,
            "servings_basis": format_number(servings),
            "uses_pilot_servings_fallback": str(bool(uses_fallback)),
            "servings_estimation_method": servings_method,
            "servings_estimation_reasons": "; ".join(servings_reasons),
            "total_weight_grams_estimated": format_number(known_weight),
            "mapped_weight_grams": format_number(mapped_weight),
            "known_weight_grams_sum": format_number(known_weight),
            "mapped_weight_ratio": format_number(mapped_weight_ratio),
            "energy_kcal_total": format_number(energy_total),
            "protein_g_total": format_number(protein_total),
            "carbs_g_total": format_number(carbs_total),
            "fat_g_total": format_number(fat_total),
            "energy_kcal_per_serving": format_number(energy_per_serving),
            "protein_g_per_serving": format_number(protein_per_serving),
            "carbs_g_per_serving": format_number(carbs_per_serving),
            "fat_g_per_serving": format_number(fat_per_serving),
            "ingredient_count": str(int(coverage["ingredient_count"])),
            "accepted_auto_count": str(int(coverage["accepted_auto_count"])),
            "accepted_auto_with_grams_count": str(int(coverage["accepted_auto_with_grams_count"])),
            "review_needed_count": str(int(coverage["review_needed_count"])),
            "review_needed_with_grams_count": str(int(coverage["review_needed_with_grams_count"])),
            "unmapped_count": str(int(coverage["unmapped_count"])),
            "unmapped_with_grams_count": str(int(coverage["unmapped_with_grams_count"])),
            "cache_status": cache_status,
            "quality_flags": "; ".join(quality_flags),
            "cache_version": CACHE_VERSION,
        }
        cache_rows.append(row)

        audit_row = dict(row)
        audit_row["is_complete_or_near_complete_meal"] = str(is_complete_or_near_complete(recipe))
        audit_row["blocking_with_grams_count"] = str(
            int(coverage["review_needed_with_grams_count"] + coverage["unmapped_with_grams_count"])
        )
        recipe_audit_rows.append(audit_row)

        if cache_status != "usable_from_mapped_ingredients" or quality_flags:
            low_coverage_rows.append(audit_row)

    return cache_rows, recipe_audit_rows, low_coverage_rows, contribution_rows


def median_or_none(values: list[float]) -> float | None:
    clean_values = [value for value in values if not math.isnan(value) and not math.isinf(value)]
    if not clean_values:
        return None
    return statistics.median(clean_values)


def numeric_column(rows: list[dict[str, object]], column: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        parsed = parse_float(row.get(column))
        if parsed is not None:
            values.append(parsed)
    return values


def grouped_remaining_blockers(contribution_rows: list[dict[str, object]], mapping_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    mapping_by_key = {
        (
            clean_text(row.get("recipe_id_candidate")),
            clean_text(row.get("ingredient_position")),
        ): row
        for row in mapping_rows
    }
    grouped: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "ingredient_name_normalized": "",
            "total_grams": 0.0,
            "row_count": 0,
            "example_recipes": [],
            "statuses": Counter(),
        }
    )
    for row in contribution_rows:
        status = clean_text(row.get("contribution_status"))
        grams = parse_positive_float(row.get("quantity_grams_estimated"))
        if grams is None or status == "used":
            continue
        key = (
            clean_text(row.get("recipe_id_candidate")),
            clean_text(row.get("ingredient_position")),
        )
        mapping_status = clean_text(mapping_by_key.get(key, {}).get("mapping_status"))
        if mapping_status == "accepted_auto":
            continue
        ingredient = clean_text(row.get("ingredient_name_normalized"))
        grouped[ingredient]["ingredient_name_normalized"] = ingredient
        grouped[ingredient]["total_grams"] = float(grouped[ingredient]["total_grams"]) + grams
        grouped[ingredient]["row_count"] = int(grouped[ingredient]["row_count"]) + 1
        grouped[ingredient]["statuses"][mapping_status] += 1
        examples = grouped[ingredient]["example_recipes"]
        display_name = clean_text(row.get("display_name"))
        if display_name and display_name not in examples and len(examples) < 5:
            examples.append(display_name)

    output: list[dict[str, object]] = []
    for item in grouped.values():
        status_text = "; ".join(f"{status}:{count}" for status, count in item["statuses"].most_common())
        output.append(
            {
                "ingredient_name_normalized": item["ingredient_name_normalized"],
                "total_grams": item["total_grams"],
                "row_count": item["row_count"],
                "mapping_status_counts": status_text,
                "example_recipes": " | ".join(item["example_recipes"]),
            }
        )
    return sorted(output, key=lambda row: (-float(row["total_grams"]), clean_text(row["ingredient_name_normalized"])))


def sort_low_coverage(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    def sort_key(row: dict[str, object]) -> tuple[float, int, float, str]:
        ratio = parse_float(row.get("mapped_weight_ratio"))
        ratio_sort = ratio if ratio is not None else -1.0
        blockers = int(parse_float(row.get("blocking_with_grams_count")) or 0)
        kcal = parse_float(row.get("energy_kcal_per_serving")) or 0.0
        return (ratio_sort, -blockers, kcal, clean_text(row.get("display_name")))

    return sorted(rows, key=sort_key)


def build_summary(
    cache_rows: list[dict[str, object]],
    recipe_audit_rows: list[dict[str, object]],
    contribution_rows: list[dict[str, object]],
    mapping_rows: list[dict[str, str]],
) -> str:
    status_counts = Counter(clean_text(row.get("cache_status")) for row in cache_rows)
    ratios = numeric_column(cache_rows, "mapped_weight_ratio")
    kcal_values = numeric_column(cache_rows, "energy_kcal_per_serving")
    protein_values = numeric_column(cache_rows, "protein_g_per_serving")
    carbs_values = numeric_column(cache_rows, "carbs_g_per_serving")
    fat_values = numeric_column(cache_rows, "fat_g_per_serving")

    complete_rows = [row for row in recipe_audit_rows if clean_text(row.get("is_complete_or_near_complete_meal")) == "True"]
    complete_kcal_300 = sum(1 for row in complete_rows if (parse_float(row.get("energy_kcal_per_serving")) or 0) >= 300)
    complete_protein_20 = sum(1 for row in complete_rows if (parse_float(row.get("protein_g_per_serving")) or 0) >= 20)

    low_coverage_rows = [
        row for row in recipe_audit_rows
        if clean_text(row.get("cache_status")) != "usable_from_mapped_ingredients"
        or "is_low_mapped_weight_ratio" in clean_text(row.get("quality_flags"))
        or "mapped_weight_ratio_missing" in clean_text(row.get("quality_flags"))
    ]
    suspicious_low_rows = [
        row for row in recipe_audit_rows
        if "is_low_kcal_suspicious" in clean_text(row.get("quality_flags"))
        or "is_low_protein_suspicious" in clean_text(row.get("quality_flags"))
    ]
    high_macro_rows = [
        row for row in recipe_audit_rows
        if "is_high_macro_suspicious" in clean_text(row.get("quality_flags"))
    ]
    blockers = grouped_remaining_blockers(contribution_rows, mapping_rows)

    recommendation = "A. proceed to materialize v1.1 tables"
    if status_counts["usable_from_mapped_ingredients"] < 150 or blockers:
        recommendation = "B. one more targeted mapping pass"
    if len(high_macro_rows) > 20:
        recommendation = "C. serving-estimation adjustment"

    lines: list[str] = []
    lines.append("Recipes_DB v1.1 nutrition cache draft summary")
    lines.append("=" * 52)
    lines.append("")
    lines.append(f"Total recipes: {len(cache_rows)}")
    lines.append("")
    lines.append("Cache status counts:")
    for status, count in status_counts.most_common():
        lines.append(f"- {status}: {count}")
    lines.append("")
    lines.append(f"usable_from_mapped_ingredients count: {status_counts['usable_from_mapped_ingredients']}")
    lines.append(f"partial_from_mapped_ingredients count: {status_counts['partial_from_mapped_ingredients']}")
    lines.append(f"low coverage count: {len(low_coverage_rows)}")
    lines.append(f"median mapped_weight_ratio: {format_number(median_or_none(ratios))}")
    lines.append(f"median energy_kcal_per_serving: {format_number(median_or_none(kcal_values))}")
    lines.append(f"median protein_g_per_serving: {format_number(median_or_none(protein_values))}")
    lines.append(f"median carbs_g_per_serving: {format_number(median_or_none(carbs_values))}")
    lines.append(f"median fat_g_per_serving: {format_number(median_or_none(fat_values))}")
    lines.append(f"complete/near-complete mains with kcal_per_serving >= 300: {complete_kcal_300}")
    lines.append(f"complete/near-complete mains with protein_per_serving >= 20: {complete_protein_20}")
    lines.append("")

    lines.append("Top 20 low coverage recipes:")
    for row in sort_low_coverage(low_coverage_rows)[:20]:
        lines.append(
            f"- {row['recipe_id_candidate']} | {row['display_name']} | status={row['cache_status']} | "
            f"ratio={row['mapped_weight_ratio']} | kcal={row['energy_kcal_per_serving']} | "
            f"protein={row['protein_g_per_serving']} | flags={row['quality_flags']}"
        )
    lines.append("")

    lines.append("Top 20 suspicious low macro recipes:")
    for row in sorted(
        suspicious_low_rows,
        key=lambda item: (
            parse_float(item.get("energy_kcal_per_serving")) or 0,
            parse_float(item.get("protein_g_per_serving")) or 0,
        ),
    )[:20]:
        lines.append(
            f"- {row['recipe_id_candidate']} | {row['display_name']} | "
            f"kcal={row['energy_kcal_per_serving']} | protein={row['protein_g_per_serving']} | "
            f"flags={row['quality_flags']}"
        )
    lines.append("")

    lines.append("Top 20 high macro suspicious recipes:")
    if high_macro_rows:
        for row in sorted(
            high_macro_rows,
            key=lambda item: -(parse_float(item.get("energy_kcal_per_serving")) or 0),
        )[:20]:
            lines.append(
                f"- {row['recipe_id_candidate']} | {row['display_name']} | "
                f"kcal={row['energy_kcal_per_serving']} | servings={row['servings_basis']} | "
                f"flags={row['quality_flags']}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.append("Top remaining unmapped/review ingredients by total grams:")
    for blocker in blockers[:20]:
        lines.append(
            f"- {blocker['ingredient_name_normalized']}: "
            f"{format_number(float(blocker['total_grams']))}g across {blocker['row_count']} rows | "
            f"{blocker['mapping_status_counts']} | examples={blocker['example_recipes']}"
        )
    lines.append("")
    lines.append(f"Recommendation: {recommendation}")
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build inspectable Recipes_DB v1.1 nutrition cache draft.")
    parser.add_argument("--recipes", default=str(DEFAULT_RECIPES))
    parser.add_argument("--parsed_ingredients", default=str(DEFAULT_PARSED_INGREDIENTS))
    parser.add_argument("--mapping", "--mapping_path", dest="mapping", default=str(DEFAULT_MAPPING))
    parser.add_argument("--fooddb", "--fooddb_path", dest="fooddb", default=str(DEFAULT_FOODDB))
    parser.add_argument("--output_suffix", "--output-suffix", dest="output_suffix", default="")
    parser.add_argument("--out_cache", default="")
    parser.add_argument("--out_summary", default="")
    parser.add_argument("--out_recipe_audit", default="")
    parser.add_argument("--out_low_coverage", default="")
    parser.add_argument("--out_contributions", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    recipes, _ = read_csv(Path(args.recipes))
    read_csv(Path(args.parsed_ingredients))
    mapping_rows, _ = read_csv(Path(args.mapping))
    food_rows, _ = read_csv(Path(args.fooddb))
    out_cache = resolve_output_path(args.out_cache, OUT_CACHE, args.output_suffix)
    out_summary = resolve_output_path(args.out_summary, OUT_SUMMARY, args.output_suffix)
    out_recipe_audit = resolve_output_path(args.out_recipe_audit, OUT_RECIPE_AUDIT, args.output_suffix)
    out_low_coverage = resolve_output_path(args.out_low_coverage, OUT_LOW_COVERAGE, args.output_suffix)
    out_contributions = resolve_output_path(args.out_contributions, OUT_CONTRIBUTIONS, args.output_suffix)

    fooddb_lookup = build_fooddb_lookup(food_rows)
    cache_rows, recipe_audit_rows, low_coverage_rows, contribution_rows = build_cache_rows(
        recipes,
        mapping_rows,
        fooddb_lookup,
    )

    audit_columns = CACHE_COLUMNS + ["is_complete_or_near_complete_meal", "blocking_with_grams_count"]
    write_csv(out_cache, cache_rows, CACHE_COLUMNS)
    write_csv(out_recipe_audit, recipe_audit_rows, audit_columns)
    write_csv(out_low_coverage, sort_low_coverage(low_coverage_rows), audit_columns)
    write_csv(out_contributions, contribution_rows, CONTRIBUTION_COLUMNS)

    summary = build_summary(cache_rows, recipe_audit_rows, contribution_rows, mapping_rows)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_summary.write_text(summary, encoding="utf-8")

    status_counts = Counter(clean_text(row.get("cache_status")) for row in cache_rows)
    print("Recipes_DB v1.1 nutrition cache draft built")
    print(f"total_recipes={len(cache_rows)}")
    for status, count in status_counts.most_common():
        print(f"{status}={count}")
    print(f"written_cache={out_cache}")
    print(f"written_summary={out_summary}")


if __name__ == "__main__":
    main()
