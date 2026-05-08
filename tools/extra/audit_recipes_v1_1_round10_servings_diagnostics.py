from __future__ import annotations

import csv
import math
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

CURATED_RECIPES = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_curated_200.csv"
ROUND9_CACHE = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_nutrition_cache_draft_round9.csv"
ROUND9_RECIPE_AUDIT = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_nutrition_cache_recipe_audit_round9.csv"
)
ROUND9_CONTRIBUTIONS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_ingredient_nutrition_contributions_round9.csv"
)
ROUND9_MAPPING = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "draft"
    / "recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_round9_final_mapping.csv"
)
ROUND9_INGREDIENTS = (
    REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_ingredients_parsed_unit_rules_round9.csv"
)

OUT_DIAGNOSTICS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_servings_diagnostics.csv"
)
OUT_SUMMARY = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_servings_summary.txt"
OUT_CANDIDATES = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_servings_adjustment_candidates.csv"
)
OUT_NO_ADJUST = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_servings_no_adjust.csv"
)

SERVING_BANDS = {
    "complete_main": (300.0, 650.0),
    "near_complete_main": (300.0, 650.0),
    "soup": (300.0, 700.0),
    "salad": (250.0, 550.0),
    "breakfast": (150.0, 400.0),
    "snack": (80.0, 300.0),
    "protein_component": (100.0, 250.0),
    "carb_side": (100.0, 300.0),
    "veg_side": (100.0, 300.0),
}

PER_SERVING_MIN_KCAL = {
    "complete_main": 250.0,
    "near_complete_main": 250.0,
    "soup": 250.0,
    "salad": 250.0,
    "breakfast": 150.0,
    "snack": 80.0,
    "protein_component": 80.0,
    "carb_side": 80.0,
    "veg_side": 40.0,
}

PER_SERVING_MIN_PROTEIN = {
    "complete_main": 10.0,
    "near_complete_main": 10.0,
    "soup": 10.0,
    "salad": 10.0,
    "breakfast": 5.0,
    "snack": 0.0,
    "protein_component": 10.0,
    "carb_side": 0.0,
    "veg_side": 0.0,
}

MAINLIKE_KINDS = {"complete_main", "near_complete_main", "soup", "salad"}
ALLOWED_SERVINGS = [1.0, 2.0, 3.0, 4.0, 6.0, 8.0]

OUTPUT_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "recipe_kind_guess",
    "primary_protein",
    "cache_status",
    "current_servings_basis",
    "current_servings_estimation_method",
    "known_weight_grams_sum",
    "mapped_weight_grams",
    "macro_relevant_mapped_weight_grams",
    "low_or_no_macro_mapped_weight_grams",
    "mapped_weight_ratio",
    "macro_relevant_mapped_weight_ratio",
    "serving_weight_g_estimated",
    "macro_relevant_serving_weight_g",
    "energy_kcal_total",
    "protein_g_total",
    "carbs_g_total",
    "fat_g_total",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "carbs_g_per_serving",
    "fat_g_per_serving",
    "accepted_auto_with_grams_count",
    "review_needed_with_grams_count",
    "unmapped_with_grams_count",
    "expected_serving_weight_lower_g",
    "expected_serving_weight_upper_g",
    "low_or_no_macro_mapped_weight_share",
    "mostly_water_broth_or_seasonings",
    "servings_diagnosis",
    "servings_adjustment_allowed",
    "recommended_adjusted_servings_basis",
    "adjusted_serving_weight_g_estimated",
    "servings_adjustment_reason",
    "servings_adjustment_method",
    "no_adjust_reason",
    "risk_notes",
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


def parse_positive_float(value: object) -> float | None:
    parsed = parse_float(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def parse_count(value: object) -> int:
    parsed = parse_float(value)
    if parsed is None:
        return 0
    return int(parsed)


def format_number(value: float | None, digits: int = 4) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}".rstrip("0").rstrip(".")


def index_by_recipe_id(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {clean_text(row.get("recipe_id_candidate")): row for row in rows}


def expected_band(kind: str) -> tuple[float, float]:
    return SERVING_BANDS.get(clean_text(kind), (150.0, 500.0))


def current_serving_is_low(
    kind: str,
    primary_protein: str,
    kcal: float,
    protein: float,
    carbs: float,
    fat: float,
) -> bool:
    kcal_min = PER_SERVING_MIN_KCAL.get(kind, 100.0)
    protein_min = PER_SERVING_MIN_PROTEIN.get(kind, 0.0)
    protein_text = clean_text(primary_protein).casefold()
    if kind in MAINLIKE_KINDS and protein_text in {"vegetarian", "egg", "unknown"} and kcal >= kcal_min:
        return False
    if kind in {"breakfast", "snack"}:
        return kcal < kcal_min and protein < protein_min and carbs < 20.0 and fat < 8.0
    if protein_min > 0:
        return kcal < kcal_min or protein < protein_min
    return kcal < kcal_min


def total_macros_are_near_zero(energy: float, protein: float, carbs: float, fat: float) -> bool:
    return energy < 50.0 and (protein + carbs + fat) < 5.0


def total_macros_look_plausible(kind: str, energy: float, protein: float, carbs: float, fat: float) -> bool:
    if total_macros_are_near_zero(energy, protein, carbs, fat):
        return False
    if kind in MAINLIKE_KINDS:
        return energy >= 350.0 or protein >= 20.0 or carbs >= 70.0 or fat >= 20.0
    if kind == "breakfast":
        return energy >= 180.0 or protein >= 8.0 or carbs >= 30.0 or fat >= 10.0
    if kind == "snack":
        return energy >= 80.0 or protein >= 5.0 or carbs >= 15.0 or fat >= 5.0
    return energy >= 80.0 or protein >= 5.0 or carbs >= 15.0 or fat >= 5.0


def is_mostly_water_or_seasoning(
    known_weight: float,
    mapped_weight: float,
    low_or_no_macro_weight: float,
) -> bool:
    if mapped_weight <= 0:
        return False
    low_share = low_or_no_macro_weight / mapped_weight
    known_share = low_or_no_macro_weight / known_weight if known_weight > 0 else 0.0
    return low_share >= 0.65 or (low_or_no_macro_weight >= 700.0 and known_share >= 0.45)


def min_servings_for_kind(kind: str, known_weight: float) -> float:
    if kind in {"complete_main", "near_complete_main", "soup", "salad"}:
        return 2.0
    return 1.0


def choose_adjusted_servings(kind: str, known_weight: float, current_servings: float) -> tuple[float | None, float | None]:
    lower, upper = expected_band(kind)
    min_servings = min_servings_for_kind(kind, known_weight)
    close_lower = lower * 0.80
    max_reasonable = upper * 1.15
    candidates = sorted(ALLOWED_SERVINGS, reverse=True)
    for candidate in candidates:
        if candidate >= current_servings or candidate < min_servings:
            continue
        serving_weight = known_weight / candidate if candidate > 0 else 0.0
        if close_lower <= serving_weight <= max_reasonable:
            return candidate, serving_weight
    return None, None


def build_diagnostic_row(
    recipe: dict[str, str],
    cache_by_id: dict[str, dict[str, str]],
) -> dict[str, object]:
    recipe_id = clean_text(recipe.get("recipe_id_candidate"))
    cache = cache_by_id.get(recipe_id, {})
    kind = clean_text(cache.get("recipe_kind_guess")) or clean_text(recipe.get("recipe_kind_guess"))
    primary_protein = clean_text(cache.get("primary_protein")) or clean_text(recipe.get("primary_protein"))
    lower, upper = expected_band(kind)

    current_servings = parse_positive_float(cache.get("servings_basis"))
    known_weight = parse_float(cache.get("known_weight_grams_sum")) or 0.0
    mapped_weight = parse_float(cache.get("mapped_weight_grams")) or 0.0
    macro_weight = parse_float(cache.get("macro_relevant_mapped_weight_grams")) or 0.0
    low_or_no_weight = parse_float(cache.get("low_or_no_macro_mapped_weight_grams")) or 0.0
    mapped_ratio = parse_float(cache.get("mapped_weight_ratio"))
    macro_ratio = parse_float(cache.get("macro_relevant_mapped_weight_ratio"))
    energy_total = parse_float(cache.get("energy_kcal_total")) or 0.0
    protein_total = parse_float(cache.get("protein_g_total")) or 0.0
    carbs_total = parse_float(cache.get("carbs_g_total")) or 0.0
    fat_total = parse_float(cache.get("fat_g_total")) or 0.0
    energy_per = parse_float(cache.get("energy_kcal_per_serving")) or 0.0
    protein_per = parse_float(cache.get("protein_g_per_serving")) or 0.0
    carbs_per = parse_float(cache.get("carbs_g_per_serving")) or 0.0
    fat_per = parse_float(cache.get("fat_g_per_serving")) or 0.0
    accepted_with_grams = parse_count(cache.get("accepted_auto_with_grams_count"))
    review_with_grams = parse_count(cache.get("review_needed_with_grams_count"))
    unmapped_with_grams = parse_count(cache.get("unmapped_with_grams_count"))
    cache_status = clean_text(cache.get("cache_status"))

    serving_weight = known_weight / current_servings if current_servings and known_weight > 0 else None
    macro_serving_weight = macro_weight / current_servings if current_servings and macro_weight > 0 else None
    low_share = low_or_no_weight / mapped_weight if mapped_weight > 0 else None
    mostly_water = is_mostly_water_or_seasoning(known_weight, mapped_weight, low_or_no_weight)

    total_near_zero = total_macros_are_near_zero(energy_total, protein_total, carbs_total, fat_total)
    total_plausible = total_macros_look_plausible(kind, energy_total, protein_total, carbs_total, fat_total)
    good_mapping_for_adjustment = (
        macro_ratio is not None
        and macro_ratio >= 0.65
        and accepted_with_grams >= 3
        and cache_status != "no_accepted_mapped_ingredients"
        and not total_near_zero
        and not mostly_water
    )

    target_servings: float | None = None
    adjusted_serving_weight: float | None = None
    diagnosis = "needs_manual_review"
    adjustment_allowed = False
    adjustment_reason = ""
    no_adjust_reason = ""
    risk_notes = ""

    if current_servings is None or current_servings <= 0 or known_weight <= 0:
        diagnosis = "impossible_to_adjust_safely"
        no_adjust_reason = "missing_or_zero_servings_or_known_weight"
        risk_notes = "Nu exista o baza suficienta pentru calculul greutatii pe portie."
    elif cache_status == "no_accepted_mapped_ingredients" or accepted_with_grams == 0:
        diagnosis = "low_macro_due_to_low_mapping_coverage"
        no_adjust_reason = "no_accepted_mapped_ingredients_with_grams"
        risk_notes = "Reteta are lipsuri de mapare, nu o problema clara de portii."
    elif mostly_water:
        diagnosis = "impossible_to_adjust_safely"
        no_adjust_reason = "mostly_water_broth_or_seasonings"
        risk_notes = "Greutatea mapata este dominata de apa, supa sau condimente."
    elif macro_ratio is None or macro_ratio < 0.45 or accepted_with_grams < 2:
        if kind in MAINLIKE_KINDS and (energy_per < 120.0 or accepted_with_grams < 2):
            diagnosis = "recipe_replacement_candidate"
            risk_notes = "Reteta principala ramane prea slaba pentru ajustare automata."
        else:
            diagnosis = "low_macro_due_to_low_mapping_coverage"
            risk_notes = "Acoperirea macro este prea mica pentru a schimba portiile."
        no_adjust_reason = "poor_macro_mapping_coverage"
    elif total_near_zero:
        diagnosis = "low_macro_due_to_low_mapping_coverage"
        no_adjust_reason = "near_zero_total_macros"
        risk_notes = "Totalurile nutritionale sunt prea mici ca ajustarea portiilor sa fie sigura."
    elif good_mapping_for_adjustment and serving_weight is not None and serving_weight < lower:
        current_low = current_serving_is_low(kind, primary_protein, energy_per, protein_per, carbs_per, fat_per)
        target_servings, adjusted_serving_weight = choose_adjusted_servings(kind, known_weight, current_servings)
        if total_plausible and current_low and target_servings is not None:
            diagnosis = "likely_servings_overestimated"
            adjustment_allowed = True
            adjustment_reason = (
                "current_serving_weight_below_expected_band_with_good_macro_coverage"
            )
        else:
            diagnosis = "needs_manual_review"
            no_adjust_reason = "serving_weight_low_but_no_safe_target_or_total_not_plausible"
            risk_notes = "Greutatea pe portie este joasa, dar totalurile sau tinta de portii nu sunt suficient de clare."
    elif good_mapping_for_adjustment and serving_weight is not None and serving_weight >= lower:
        if current_serving_is_low(kind, primary_protein, energy_per, protein_per, carbs_per, fat_per):
            diagnosis = "low_macro_despite_good_serving"
            no_adjust_reason = "serving_weight_in_band_but_macros_low"
        else:
            diagnosis = "serving_ok"
            no_adjust_reason = "serving_weight_and_macros_are_not_obvious_blockers"
    elif macro_ratio is not None and macro_ratio < 0.65:
        diagnosis = "low_macro_due_to_low_mapping_coverage"
        no_adjust_reason = "macro_relevant_mapping_ratio_below_adjustment_threshold"
        risk_notes = "Sub pragul de 0.65 pentru ajustare conservatoare."
    else:
        diagnosis = "needs_manual_review"
        no_adjust_reason = "does_not_meet_all_adjustment_rules"

    if adjustment_allowed:
        no_adjust_reason = ""
        risk_notes = "Ajustare conservatoare; totalurile nutritionale raman neschimbate."

    return {
        "recipe_id_candidate": recipe_id,
        "display_name": clean_text(cache.get("display_name")) or clean_text(recipe.get("display_name")),
        "recipe_kind_guess": kind,
        "primary_protein": primary_protein,
        "cache_status": cache_status,
        "current_servings_basis": format_number(current_servings),
        "current_servings_estimation_method": clean_text(cache.get("servings_estimation_method")),
        "known_weight_grams_sum": format_number(known_weight),
        "mapped_weight_grams": format_number(mapped_weight),
        "macro_relevant_mapped_weight_grams": format_number(macro_weight),
        "low_or_no_macro_mapped_weight_grams": format_number(low_or_no_weight),
        "mapped_weight_ratio": format_number(mapped_ratio),
        "macro_relevant_mapped_weight_ratio": format_number(macro_ratio),
        "serving_weight_g_estimated": format_number(serving_weight),
        "macro_relevant_serving_weight_g": format_number(macro_serving_weight),
        "energy_kcal_total": format_number(energy_total),
        "protein_g_total": format_number(protein_total),
        "carbs_g_total": format_number(carbs_total),
        "fat_g_total": format_number(fat_total),
        "energy_kcal_per_serving": format_number(energy_per),
        "protein_g_per_serving": format_number(protein_per),
        "carbs_g_per_serving": format_number(carbs_per),
        "fat_g_per_serving": format_number(fat_per),
        "accepted_auto_with_grams_count": accepted_with_grams,
        "review_needed_with_grams_count": review_with_grams,
        "unmapped_with_grams_count": unmapped_with_grams,
        "expected_serving_weight_lower_g": format_number(lower),
        "expected_serving_weight_upper_g": format_number(upper),
        "low_or_no_macro_mapped_weight_share": format_number(low_share),
        "mostly_water_broth_or_seasonings": str(bool(mostly_water)),
        "servings_diagnosis": diagnosis,
        "servings_adjustment_allowed": str(bool(adjustment_allowed)),
        "recommended_adjusted_servings_basis": format_number(target_servings),
        "adjusted_serving_weight_g_estimated": format_number(adjusted_serving_weight),
        "servings_adjustment_reason": adjustment_reason,
        "servings_adjustment_method": (
            "round10_conservative_serving_weight_band" if adjustment_allowed else ""
        ),
        "no_adjust_reason": no_adjust_reason,
        "risk_notes": risk_notes,
    }


def sort_diagnostics(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    def key(row: dict[str, object]) -> tuple[int, float, str]:
        diagnosis = clean_text(row.get("servings_diagnosis"))
        priority = 0 if diagnosis == "likely_servings_overestimated" else 1
        weight = parse_float(row.get("known_weight_grams_sum")) or 0.0
        return (priority, -weight, clean_text(row.get("display_name")))

    return sorted(rows, key=key)


def write_summary(path: Path, rows: list[dict[str, object]], candidates: list[dict[str, object]]) -> None:
    diagnosis_counts = Counter(clean_text(row.get("servings_diagnosis")) for row in rows)
    kind_counts = Counter(clean_text(row.get("recipe_kind_guess")) for row in candidates)
    low_coverage = [row for row in rows if clean_text(row.get("servings_diagnosis")) == "low_macro_due_to_low_mapping_coverage"]
    replacement = [row for row in rows if clean_text(row.get("servings_diagnosis")) == "recipe_replacement_candidate"]

    lines = [
        "Recipes_DB v1.1 round10 servings diagnostics",
        "=" * 52,
        "",
        f"Total recipes audited: {len(rows)}",
        f"Conservative adjustment candidates: {len(candidates)}",
        "",
        "Servings diagnosis counts:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in diagnosis_counts.most_common())
    lines.extend(["", "Adjustment candidates by kind:"])
    if kind_counts:
        lines.extend(f"- {name}: {count}" for name, count in kind_counts.most_common())
    else:
        lines.append("- none")

    lines.extend(["", "Top adjustment candidates:"])
    if candidates:
        for row in candidates[:20]:
            lines.append(
                f"- {row['recipe_id_candidate']} | {row['display_name']} | "
                f"{row['current_servings_basis']} -> {row['recommended_adjusted_servings_basis']} servings | "
                f"serving_weight={row['serving_weight_g_estimated']}g -> "
                f"{row['adjusted_serving_weight_g_estimated']}g | "
                f"macro_ratio={row['macro_relevant_mapped_weight_ratio']}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "Low coverage kept out of servings adjustment:"])
    if low_coverage:
        for row in sorted(low_coverage, key=lambda item: parse_float(item.get("macro_relevant_mapped_weight_ratio")) or 0.0)[:20]:
            lines.append(
                f"- {row['recipe_id_candidate']} | {row['display_name']} | "
                f"macro_ratio={row['macro_relevant_mapped_weight_ratio']} | "
                f"accepted_with_grams={row['accepted_auto_with_grams_count']} | "
                f"reason={row['no_adjust_reason']}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "Recipe replacement signals from servings pass:"])
    if replacement:
        for row in replacement[:20]:
            lines.append(
                f"- {row['recipe_id_candidate']} | {row['display_name']} | "
                f"kcal={row['energy_kcal_per_serving']} | protein={row['protein_g_per_serving']} | "
                f"macro_ratio={row['macro_relevant_mapped_weight_ratio']}"
            )
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "Decision:",
            "- Adjust only rows marked likely_servings_overestimated with servings_adjustment_allowed=True.",
            "- Keep low-coverage recipes as mapping/data problems, not serving problems.",
            "- Continue with round10 adjusted cache and generator-readiness audit.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    recipes, _ = read_csv(CURATED_RECIPES)
    cache_rows, _ = read_csv(ROUND9_CACHE)
    read_csv(ROUND9_RECIPE_AUDIT)
    read_csv(ROUND9_CONTRIBUTIONS)
    read_csv(ROUND9_MAPPING)
    read_csv(ROUND9_INGREDIENTS)

    cache_by_id = index_by_recipe_id(cache_rows)
    diagnostic_rows = [build_diagnostic_row(recipe, cache_by_id) for recipe in recipes]
    diagnostic_rows = sort_diagnostics(diagnostic_rows)
    candidates = [
        row for row in diagnostic_rows if clean_text(row.get("servings_adjustment_allowed")).casefold() == "true"
    ]
    no_adjust = [
        row for row in diagnostic_rows if clean_text(row.get("servings_adjustment_allowed")).casefold() != "true"
    ]

    write_csv(OUT_DIAGNOSTICS, diagnostic_rows, OUTPUT_COLUMNS)
    write_csv(OUT_CANDIDATES, candidates, OUTPUT_COLUMNS)
    write_csv(OUT_NO_ADJUST, no_adjust, OUTPUT_COLUMNS)
    write_summary(OUT_SUMMARY, diagnostic_rows, candidates)

    diagnosis_counts = Counter(clean_text(row.get("servings_diagnosis")) for row in diagnostic_rows)
    print("Round10 servings diagnostics written")
    print(f"total_recipes={len(diagnostic_rows)}")
    print(f"adjustment_candidates={len(candidates)}")
    for diagnosis, count in diagnosis_counts.most_common():
        print(f"{diagnosis}={count}")
    print(f"written_diagnostics={OUT_DIAGNOSTICS}")
    print(f"written_summary={OUT_SUMMARY}")


if __name__ == "__main__":
    main()
