from __future__ import annotations

import csv
import math
from collections import Counter, defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

CURATED_RECIPES = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_curated_200.csv"
ROUND10_CACHE = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_nutrition_cache_draft_round10.csv"
ROUND10_SERVINGS_DIAGNOSTICS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_servings_diagnostics.csv"
)
ROUND10_CONTRIBUTIONS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_ingredient_nutrition_contributions_round10.csv"
)

OUT_READINESS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_generator_readiness.csv"
)
OUT_SUMMARY = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_generator_readiness_summary.txt"
)
OUT_REPLACEMENTS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_recipe_replacement_candidates.csv"
)
OUT_MATERIALIZATION = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_materialization_recommendation.txt"
)

MAINLIKE_KINDS = {"complete_main", "near_complete_main", "soup", "salad"}
COMPONENT_KINDS = {"protein_component", "carb_side", "veg_side", "component"}
VEGETARIAN_PROTEINS = {"vegetarian", "egg"}

OUTPUT_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "recipe_kind_guess",
    "primary_protein",
    "generator_readiness",
    "readiness_reason",
    "cache_status",
    "servings_basis",
    "servings_adjustment_applied",
    "servings_diagnosis",
    "mapped_weight_ratio",
    "macro_relevant_mapped_weight_ratio",
    "low_or_no_macro_mapped_weight_share",
    "mostly_water_broth_or_seasonings",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "carbs_g_per_serving",
    "fat_g_per_serving",
    "used_ingredient_count",
    "quality_flags",
    "high_macro_suspicious",
    "replacement_priority",
    "replacement_reasons",
    "materialization_scope",
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


def format_number(value: float | None, digits: int = 4) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}".rstrip("0").rstrip(".")


def is_true(value: object) -> bool:
    return clean_text(value).casefold() in {"1", "true", "yes", "y"}


def index_by_recipe_id(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {clean_text(row.get("recipe_id_candidate")): row for row in rows}


def contribution_counts_by_recipe(rows: list[dict[str, str]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        if clean_text(row.get("contribution_status")) == "used":
            counts[clean_text(row.get("recipe_id_candidate"))] += 1
    return counts


def low_or_no_macro_share(cache: dict[str, str]) -> float | None:
    mapped_weight = parse_float(cache.get("mapped_weight_grams")) or 0.0
    low_or_no_weight = parse_float(cache.get("low_or_no_macro_mapped_weight_grams")) or 0.0
    if mapped_weight <= 0:
        return None
    return low_or_no_weight / mapped_weight


def mostly_water_or_broth(cache: dict[str, str], diagnostic: dict[str, str]) -> bool:
    if is_true(diagnostic.get("mostly_water_broth_or_seasonings")):
        return True
    share = low_or_no_macro_share(cache)
    low_or_no_weight = parse_float(cache.get("low_or_no_macro_mapped_weight_grams")) or 0.0
    known_weight = parse_float(cache.get("known_weight_grams_sum")) or 0.0
    known_share = low_or_no_weight / known_weight if known_weight > 0 else 0.0
    return bool(share is not None and share >= 0.65) or (low_or_no_weight >= 700.0 and known_share >= 0.45)


def high_macro_suspicious(cache: dict[str, str]) -> bool:
    flags = clean_text(cache.get("quality_flags"))
    kcal = parse_float(cache.get("energy_kcal_per_serving")) or 0.0
    return "is_high_macro_suspicious" in flags or kcal >= 1200.0


def protein_requirement_ok(kind: str, primary_protein: str, protein: float) -> bool:
    primary = clean_text(primary_protein).casefold()
    if primary in VEGETARIAN_PROTEINS and kind != "protein_component":
        return True
    return protein >= 10.0


def snack_is_heavy_main(cache: dict[str, str]) -> bool:
    kcal = parse_float(cache.get("energy_kcal_per_serving")) or 0.0
    protein = parse_float(cache.get("protein_g_per_serving")) or 0.0
    serving_weight = parse_float(cache.get("known_weight_grams_sum"))
    servings = parse_float(cache.get("servings_basis"))
    serving_weight_value = serving_weight / servings if serving_weight and servings else 0.0
    return kcal >= 650.0 or protein >= 35.0 or serving_weight_value > 320.0


def classify_readiness(
    recipe: dict[str, str],
    cache: dict[str, str],
    diagnostic: dict[str, str],
) -> tuple[str, str, str, str, str]:
    kind = clean_text(cache.get("recipe_kind_guess")) or clean_text(recipe.get("recipe_kind_guess"))
    primary_protein = clean_text(cache.get("primary_protein")) or clean_text(recipe.get("primary_protein"))
    cache_status = clean_text(cache.get("cache_status"))
    mapped_ratio = parse_float(cache.get("mapped_weight_ratio")) or 0.0
    macro_ratio = parse_float(cache.get("macro_relevant_mapped_weight_ratio")) or 0.0
    kcal = parse_float(cache.get("energy_kcal_per_serving")) or 0.0
    protein = parse_float(cache.get("protein_g_per_serving")) or 0.0
    carbs = parse_float(cache.get("carbs_g_per_serving")) or 0.0
    fat = parse_float(cache.get("fat_g_per_serving")) or 0.0
    water_heavy = mostly_water_or_broth(cache, diagnostic)
    high_macro = high_macro_suspicious(cache)
    servings_diagnosis = clean_text(diagnostic.get("servings_diagnosis"))
    replacement_reasons: list[str] = []

    if high_macro:
        return (
            "excluded_for_now",
            "high_macro_suspicious",
            "exclude_from_materialization",
            "high",
            "high_macro_suspicious",
        )

    if water_heavy:
        return (
            "excluded_for_now",
            "mostly_water_broth_or_seasonings",
            "exclude_from_materialization",
            "medium",
            "mostly_water_broth_or_seasonings",
        )

    if kind in COMPONENT_KINDS:
        if cache_status == "no_accepted_mapped_ingredients" or macro_ratio < 0.20:
            return (
                "partial_keep_for_future",
                "component_has_low_mapping_coverage",
                "future_component_review",
                "medium",
                "component_low_mapping_coverage",
            )
        return (
            "component_only",
            "side_or_component_not_counted_as_complete_meal",
            "component_or_side_only",
            "",
            "",
        )

    if servings_diagnosis == "recipe_replacement_candidate":
        replacement_reasons.append("round10_servings_diagnostic_replacement_signal")
    if cache_status == "no_accepted_mapped_ingredients":
        replacement_reasons.append("no_accepted_mapped_ingredients")
    if kind in MAINLIKE_KINDS and macro_ratio < 0.35 and (kcal < 120.0 or protein < 5.0):
        replacement_reasons.append("mainlike_macro_coverage_and_per_serving_macros_too_low")
    if kind in MAINLIKE_KINDS and mapped_ratio < 0.25:
        replacement_reasons.append("mainlike_mapped_weight_ratio_too_low")
    if replacement_reasons:
        return (
            "replace_candidate",
            "; ".join(replacement_reasons),
            "replace_before_generator_use",
            "high",
            "; ".join(replacement_reasons),
        )

    if kind in MAINLIKE_KINDS:
        cache_or_weight_ok = cache_status == "usable_from_mapped_ingredients" or mapped_ratio >= 0.50
        protein_ok = protein_requirement_ok(kind, primary_protein, protein)
        if cache_or_weight_ok and macro_ratio >= 0.55 and kcal >= 250.0 and protein_ok:
            return (
                "generator_ready",
                "mainlike_thresholds_met",
                "generator_ready_mainlike",
                "",
                "",
            )
        if macro_ratio >= 0.45 and kcal >= 180.0 and (protein >= 5.0 or primary_protein in VEGETARIAN_PROTEINS):
            return (
                "usable_but_review",
                "mainlike_close_but_below_ready_threshold",
                "review_before_generator_use",
                "",
                "",
            )
        return (
            "partial_keep_for_future",
            "mainlike_below_macro_or_energy_threshold",
            "future_review",
            "medium",
            "mainlike_below_ready_threshold",
        )

    if kind == "breakfast":
        meaningful_macros = protein >= 5.0 or carbs >= 20.0 or fat >= 8.0
        if kcal >= 150.0 and meaningful_macros and macro_ratio >= 0.45:
            return (
                "generator_ready",
                "breakfast_thresholds_met",
                "generator_ready_breakfast",
                "",
                "",
            )
        if kcal >= 100.0 and macro_ratio >= 0.35:
            return (
                "usable_but_review",
                "breakfast_close_but_below_ready_threshold",
                "review_before_generator_use",
                "",
                "",
            )
        return (
            "partial_keep_for_future",
            "breakfast_below_ready_threshold",
            "future_review",
            "medium",
            "breakfast_below_ready_threshold",
        )

    if kind == "snack":
        if kcal >= 80.0 and macro_ratio >= 0.40 and not snack_is_heavy_main(cache):
            return (
                "generator_ready",
                "snack_thresholds_met",
                "generator_ready_snack",
                "",
                "",
            )
        if kcal >= 60.0 and macro_ratio >= 0.30:
            return (
                "usable_but_review",
                "snack_close_or_heavy_review_needed",
                "review_before_generator_use",
                "",
                "",
            )
        return (
            "partial_keep_for_future",
            "snack_below_ready_threshold",
            "future_review",
            "low",
            "snack_below_ready_threshold",
        )

    if cache_status == "usable_from_mapped_ingredients" and macro_ratio >= 0.45 and kcal >= 100.0:
        return (
            "usable_but_review",
            "unknown_kind_with_usable_cache",
            "review_before_generator_use",
            "",
            "",
        )
    return (
        "partial_keep_for_future",
        "unknown_kind_or_low_cache_quality",
        "future_review",
        "medium",
        "unknown_kind_or_low_cache_quality",
    )


def build_readiness_rows(
    recipes: list[dict[str, str]],
    cache_by_id: dict[str, dict[str, str]],
    diagnostics_by_id: dict[str, dict[str, str]],
    used_counts: Counter[str],
) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for recipe in recipes:
        recipe_id = clean_text(recipe.get("recipe_id_candidate"))
        cache = cache_by_id.get(recipe_id, {})
        diagnostic = diagnostics_by_id.get(recipe_id, {})
        kind = clean_text(cache.get("recipe_kind_guess")) or clean_text(recipe.get("recipe_kind_guess"))
        primary_protein = clean_text(cache.get("primary_protein")) or clean_text(recipe.get("primary_protein"))
        readiness, reason, scope, replacement_priority, replacement_reasons = classify_readiness(
            recipe,
            cache,
            diagnostic,
        )
        output.append(
            {
                "recipe_id_candidate": recipe_id,
                "display_name": clean_text(cache.get("display_name")) or clean_text(recipe.get("display_name")),
                "recipe_kind_guess": kind,
                "primary_protein": primary_protein,
                "generator_readiness": readiness,
                "readiness_reason": reason,
                "cache_status": clean_text(cache.get("cache_status")),
                "servings_basis": clean_text(cache.get("servings_basis")),
                "servings_adjustment_applied": clean_text(cache.get("servings_adjustment_applied")),
                "servings_diagnosis": clean_text(diagnostic.get("servings_diagnosis")),
                "mapped_weight_ratio": clean_text(cache.get("mapped_weight_ratio")),
                "macro_relevant_mapped_weight_ratio": clean_text(cache.get("macro_relevant_mapped_weight_ratio")),
                "low_or_no_macro_mapped_weight_share": format_number(low_or_no_macro_share(cache)),
                "mostly_water_broth_or_seasonings": str(mostly_water_or_broth(cache, diagnostic)),
                "energy_kcal_per_serving": clean_text(cache.get("energy_kcal_per_serving")),
                "protein_g_per_serving": clean_text(cache.get("protein_g_per_serving")),
                "carbs_g_per_serving": clean_text(cache.get("carbs_g_per_serving")),
                "fat_g_per_serving": clean_text(cache.get("fat_g_per_serving")),
                "used_ingredient_count": used_counts[recipe_id],
                "quality_flags": clean_text(cache.get("quality_flags")),
                "high_macro_suspicious": str(high_macro_suspicious(cache)),
                "replacement_priority": replacement_priority,
                "replacement_reasons": replacement_reasons,
                "materialization_scope": scope,
            }
        )
    return sorted(output, key=lambda row: (clean_text(row.get("generator_readiness")), clean_text(row.get("display_name"))))


def replacement_sort_key(row: dict[str, object]) -> tuple[int, float, str]:
    priority_order = {"high": 0, "medium": 1, "low": 2, "": 3}
    macro_ratio = parse_float(row.get("macro_relevant_mapped_weight_ratio")) or 0.0
    return (
        priority_order.get(clean_text(row.get("replacement_priority")), 3),
        macro_ratio,
        clean_text(row.get("display_name")),
    )


def build_materialization_text(rows: list[dict[str, object]]) -> str:
    counts = Counter(clean_text(row.get("generator_readiness")) for row in rows)
    ready_rows = [row for row in rows if clean_text(row.get("generator_readiness")) == "generator_ready"]
    ready_mainlike = [
        row for row in ready_rows if clean_text(row.get("recipe_kind_guess")) in MAINLIKE_KINDS
    ]
    ready_complete_near = [
        row for row in ready_rows if clean_text(row.get("recipe_kind_guess")) in {"complete_main", "near_complete_main"}
    ]
    replacement_count = counts["replace_candidate"]
    partial_count = counts["partial_keep_for_future"]

    full_materialization = "no"
    subset_materialization = "yes" if ready_rows else "no"
    minimum_next_action = "A. materialize generator_ready subset"
    if len(ready_mainlike) < 50 or replacement_count >= 25:
        minimum_next_action = "B. replace weak recipes"
    elif partial_count > 70:
        minimum_next_action = "D. servings adjustment review"

    lines = [
        "Recipes_DB v1.1 round10 materialization recommendation",
        "=" * 60,
        "",
        f"Full v1.1 materialization recommended: {full_materialization}",
        f"Generator-ready subset materialization possible: {subset_materialization}",
        f"Generator-ready total: {len(ready_rows)}",
        f"Generator-ready mainlike meals: {len(ready_mainlike)}",
        f"Generator-ready complete/near-complete meals: {len(ready_complete_near)}",
        f"Replace candidates: {replacement_count}",
        f"Partial keep for future: {partial_count}",
        "",
        f"Minimum next action: {minimum_next_action}",
        "",
        "Notes:",
        "- Round10 does not support full production materialization.",
        "- Broad mapping passes should remain stopped for now.",
        "- Materialize only a reviewed generator_ready subset after project approval.",
    ]
    return "\n".join(lines) + "\n"


def write_summary(path: Path, rows: list[dict[str, object]]) -> None:
    counts = Counter(clean_text(row.get("generator_readiness")) for row in rows)
    ready_rows = [row for row in rows if clean_text(row.get("generator_readiness")) == "generator_ready"]
    ready_complete_near = [
        row for row in ready_rows if clean_text(row.get("recipe_kind_guess")) in {"complete_main", "near_complete_main"}
    ]
    ready_breakfast = [row for row in ready_rows if clean_text(row.get("recipe_kind_guess")) == "breakfast"]
    ready_snack = [row for row in ready_rows if clean_text(row.get("recipe_kind_guess")) == "snack"]
    replacements = [row for row in rows if clean_text(row.get("generator_readiness")) == "replace_candidate"]
    replacement_sorted = sorted(replacements, key=replacement_sort_key)
    materialization_text = build_materialization_text(rows)
    minimum_next_action = ""
    for line in materialization_text.splitlines():
        if line.startswith("Minimum next action:"):
            minimum_next_action = line.replace("Minimum next action:", "").strip()

    lines = [
        "Recipes_DB v1.1 round10 generator readiness audit",
        "=" * 58,
        "",
        f"generator_ready total: {len(ready_rows)}",
        f"generator_ready complete/near-complete meals: {len(ready_complete_near)}",
        f"generator_ready breakfast: {len(ready_breakfast)}",
        f"generator_ready snack: {len(ready_snack)}",
        f"usable_but_review count: {counts['usable_but_review']}",
        f"replace_candidate count: {counts['replace_candidate']}",
        f"partial_keep_for_future count: {counts['partial_keep_for_future']}",
        f"component_only count: {counts['component_only']}",
        f"excluded_for_now count: {counts['excluded_for_now']}",
        "",
        "Readiness counts:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in counts.most_common())
    lines.extend(["", "Top replacement candidates:"])
    if replacement_sorted:
        for row in replacement_sorted[:25]:
            lines.append(
                f"- {row['recipe_id_candidate']} | {row['display_name']} | "
                f"kind={row['recipe_kind_guess']} | kcal={row['energy_kcal_per_serving']} | "
                f"protein={row['protein_g_per_serving']} | macro_ratio={row['macro_relevant_mapped_weight_ratio']} | "
                f"reason={row['replacement_reasons']}"
            )
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "Materialization decision:",
            "- Full v1.1 materialization: no.",
            "- Subset materialization: possible for generator_ready rows only, after review.",
            f"- Minimum next action: {minimum_next_action}",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    recipes, _ = read_csv(CURATED_RECIPES)
    cache_rows, _ = read_csv(ROUND10_CACHE)
    diagnostics, _ = read_csv(ROUND10_SERVINGS_DIAGNOSTICS)
    contributions, _ = read_csv(ROUND10_CONTRIBUTIONS)

    cache_by_id = index_by_recipe_id(cache_rows)
    diagnostics_by_id = index_by_recipe_id(diagnostics)
    used_counts = contribution_counts_by_recipe(contributions)
    readiness_rows = build_readiness_rows(recipes, cache_by_id, diagnostics_by_id, used_counts)
    replacements = [
        row for row in readiness_rows if clean_text(row.get("generator_readiness")) == "replace_candidate"
    ]
    replacements = sorted(replacements, key=replacement_sort_key)

    write_csv(OUT_READINESS, readiness_rows, OUTPUT_COLUMNS)
    write_csv(OUT_REPLACEMENTS, replacements, OUTPUT_COLUMNS)
    write_summary(OUT_SUMMARY, readiness_rows)
    OUT_MATERIALIZATION.parent.mkdir(parents=True, exist_ok=True)
    OUT_MATERIALIZATION.write_text(build_materialization_text(readiness_rows), encoding="utf-8")

    counts = Counter(clean_text(row.get("generator_readiness")) for row in readiness_rows)
    print("Round10 generator readiness audit written")
    print(f"total_recipes={len(readiness_rows)}")
    for readiness, count in counts.most_common():
        print(f"{readiness}={count}")
    print(f"replacement_candidates={len(replacements)}")
    print(f"written_readiness={OUT_READINESS}")
    print(f"written_summary={OUT_SUMMARY}")
    print(f"written_materialization={OUT_MATERIALIZATION}")


if __name__ == "__main__":
    main()
