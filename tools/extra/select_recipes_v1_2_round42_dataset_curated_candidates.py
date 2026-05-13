from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra import select_recipes_v1_2_round28_targeted_plus30 as base
from tools.extra import select_recipes_v1_2_round37_targeted_plus100 as round37


SOURCE_RECIPES = Path("data/recipesdb/source/1_Recipe_csv.csv")
BASELINE_RECIPES = Path(
    "data/recipesdb/draft/v1_2_generator_ready_round41_manual_curated/recipes.csv"
)
CURATED_200 = Path("data/recipesdb/draft/recipes_v1_1_curated_200.csv")
MANUAL_REPAIR_QUEUE = Path("data/recipesdb/draft/recipes_v1_2_manual_repair_queue.csv")
ROUND40_DATASET_PLAN = Path("data/recipesdb/audit/recipes_v1_2_round40_dataset_curated_plan.csv")
ROUND40_GAP_ANALYSIS = Path("data/recipesdb/audit/recipes_v1_2_round40_gap_analysis.csv")

OUT_CANDIDATES = Path("data/recipesdb/draft/recipes_v1_2_round42_dataset_candidate_pool.csv")
OUT_SELECTED = Path("data/recipesdb/draft/recipes_v1_2_round42_dataset_curated_selected.csv")
OUT_SUMMARY = Path("data/recipesdb/audit/recipes_v1_2_round42_dataset_selection_summary.txt")
OUT_QUALITY_AUDIT = Path("data/recipesdb/audit/recipes_v1_2_round42_dataset_selection_quality_audit.csv")
OUT_EXCLUSION = Path("data/recipesdb/audit/recipes_v1_2_round42_dataset_selection_exclusion_log.csv")

OUTPUT_COLUMNS = [
    "recipe_id_candidate",
    "source_index",
    "display_name",
    "source_category",
    "source_subcategory",
    "target_bucket",
    "recipe_kind_guess",
    "primary_protein",
    "has_carb_component",
    "has_veg_component",
    "expected_mapping_difficulty",
    "expected_generator_value",
    "duplicate_risk",
    "selection_reason",
    "risk_notes",
    "quality_status",
    "selection_score",
    "title_quality_status",
    "is_weird_or_random",
    "is_too_american_processed",
    "is_dessert_like",
    "is_drink",
    "is_pet_food",
    "is_component_only",
    "has_clear_protein",
    "has_clear_carb_or_veg",
    "ingredient_count",
    "step_count",
    "expected_unit_to_grams_risk",
    "expected_fooddb_gap_risk",
    "expected_servings_risk",
    "manual_review_recommendation",
    "ingredients_json",
    "directions_json",
]

EXCLUSION_COLUMNS = [
    "source_index",
    "display_name",
    "source_category",
    "source_subcategory",
    "exclusion_reason",
    "target_bucket",
    "primary_protein",
    "has_carb_component",
    "has_veg_component",
    "ingredient_count",
    "step_count",
    "selection_score",
]

TARGET_QUOTAS = {
    "breakfast_competitor": 30,
    "lunch_dinner_carb_protein": 100,
    "fish_turkey_pork_main": 20,
    "vegetarian_legume_balanced": 20,
    "flexible_high_value": 10,
}

MAX_CANDIDATE_POOL = 1000
MAX_SELECTED = 200
MAX_EXCLUSION_ROWS = 18000
MIN_CANDIDATE_SCORE = 68.0

ROUND42_STRICT_BAD_TERMS = {
    "air fryer",
    "baby food",
    "bacon",
    "barbecue sauce",
    "bats",
    "bean-hole",
    "beignet",
    "beignets",
    "bbq",
    "bbq sauce",
    "bloody mary",
    "boursin",
    "bourbon",
    "bratwurst",
    "brownie",
    "brownies",
    "buffalo",
    "burrito",
    "burritos",
    "butterscotch",
    "cake",
    "cajun",
    "candy",
    "campfire",
    "casserole",
    "cheesecake",
    "chilaquiles",
    "chocolate",
    "christmas",
    "cocktail",
    "cobwebs",
    "corned beef",
    "cowboy",
    "crisp",
    "cookie",
    "cookies",
    "cupcake",
    "dessert",
    "dog",
    "frosting",
    "foil",
    "granola",
    "hummus",
    "ice cream",
    "kielbasa",
    "loaded",
    "mac and cheese",
    "margarita",
    "martini",
    "pet",
    "pie",
    "pies",
    "pizza",
    "protein packed",
    "popsicle",
    "potica",
    "pudding",
    "sangria",
    "sausage",
    "scotch",
    "sloppy joe",
    "smoothie",
    "spaghetti-o",
    "sushi",
    "taco",
    "tacos",
    "tropical",
    "wisconsin",
    "whoopie",
}


def main() -> None:
    source_rows = read_csv(SOURCE_RECIPES)
    curated_rows = read_csv(CURATED_200)
    baseline_rows = read_csv(BASELINE_RECIPES)
    repair_queue_rows = read_csv(MANUAL_REPAIR_QUEUE)
    exclusion_indices, exclusion_titles = existing_recipe_keys(
        curated_rows=curated_rows,
        baseline_rows=baseline_rows,
        repair_queue_rows=repair_queue_rows,
    )

    candidates: list[dict[str, object]] = []
    exclusion_rows: list[dict[str, object]] = []
    seen_titles: set[str] = set()
    for zero_index, row in enumerate(source_rows):
        source_index = zero_index + 1
        ingredients = base.load_json_list(row.get("ingredients"))
        directions = base.load_json_list(row.get("directions"))
        analysis = adapt_analysis(base.analyze_recipe(row, ingredients, directions), row, ingredients)
        title_key = base.normalize_title_key(row.get("recipe_title"))
        exclusion_reason = exclusion_reason_for_recipe(
            source_index=source_index,
            title_key=title_key,
            row=row,
            ingredients=ingredients,
            directions=directions,
            analysis=analysis,
            exclusion_indices=exclusion_indices,
            exclusion_titles=exclusion_titles,
            seen_titles=seen_titles,
        )
        if exclusion_reason:
            if len(exclusion_rows) < MAX_EXCLUSION_ROWS:
                exclusion_rows.append(exclusion_row(source_index, row, analysis, exclusion_reason))
            continue
        seen_titles.add(title_key)
        candidates.append(candidate_row(source_index, row, ingredients, directions, analysis))

    candidate_pool = sorted(
        candidates,
        key=lambda item: (
            str(item["quality_status"]) != "keep",
            -float(item["selection_score"]),
            bucket_rank(str(item["target_bucket"])),
            str(item["display_name"]),
        ),
    )[:MAX_CANDIDATE_POOL]
    selected = select_dataset_curated(candidate_pool)
    selected_rows = [finalize_selected_row(index, row) for index, row in enumerate(selected, start=1)]
    candidate_rows = [
        finalize_candidate_pool_row(row, index) for index, row in enumerate(candidate_pool, start=1)
    ]

    write_csv(OUT_CANDIDATES, candidate_rows, OUTPUT_COLUMNS)
    write_csv(OUT_SELECTED, selected_rows, OUTPUT_COLUMNS)
    write_csv(OUT_QUALITY_AUDIT, candidate_rows, OUTPUT_COLUMNS)
    write_csv(OUT_EXCLUSION, exclusion_rows, EXCLUSION_COLUMNS)
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(
        build_summary(selected_rows, candidate_rows, exclusion_rows),
        encoding="utf-8",
    )

    print("Round42 dataset-curated selection written")
    print(f"candidate_pool={len(candidate_rows)} selected={len(selected_rows)}")
    print(f"selected={OUT_SELECTED}")
    print(f"summary={OUT_SUMMARY}")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def existing_recipe_keys(
    *,
    curated_rows: list[dict[str, str]],
    baseline_rows: list[dict[str, str]],
    repair_queue_rows: list[dict[str, str]],
) -> tuple[set[int], set[str]]:
    indices: set[int] = set()
    titles: set[str] = set()
    for row in curated_rows:
        add_int(indices, row.get("source_index"))
        add_title(titles, row.get("display_name"))
    for row in baseline_rows:
        add_int(indices, row.get("source_recipe_id"))
        add_title(titles, row.get("display_name") or row.get("recipe_name"))
    for row in repair_queue_rows:
        add_int(indices, row.get("source_index"))
        add_title(titles, row.get("display_name"))
    return indices, {title for title in titles if title}


def add_int(values: set[int], value: object) -> None:
    try:
        number = int(float(str(value or "").strip()))
    except ValueError:
        return
    if number > 0:
        values.add(number)


def add_title(values: set[str], value: object) -> None:
    title = base.normalize_title_key(value)
    if title:
        values.add(title)


def adapt_analysis(
    analysis: dict[str, Any],
    row: dict[str, str],
    ingredients: list[str],
) -> dict[str, Any]:
    updated = dict(analysis)
    protein = str(updated.get("primary_protein", ""))
    combined = base.normalize_text(
        " ".join(
            [
                str(row.get("recipe_title", "")),
                str(row.get("category", "")),
                str(row.get("subcategory", "")),
                " ".join(ingredients),
            ]
        )
    )
    extra_protein = round37.detect_extra_primary_protein(combined)
    if protein == "vegetarian" and extra_protein:
        protein = extra_protein
        updated["primary_protein"] = protein

    original_bucket = str(updated.get("target_bucket", ""))
    if original_bucket == "breakfast_competitor":
        bucket = "breakfast_competitor"
    elif original_bucket == "fish_turkey_pork_legume_main":
        if protein == "vegetarian" and not round37.has_meat_term(combined):
            bucket = "vegetarian_legume_balanced"
        elif protein in {"fish", "turkey", "pork"}:
            bucket = "fish_turkey_pork_main"
        else:
            bucket = "lunch_dinner_carb_protein"
    elif original_bucket == "carb_protein_main":
        bucket = "lunch_dinner_carb_protein"
    else:
        bucket = original_bucket

    if bucket == "lunch_dinner_carb_protein" and protein in {"fish", "turkey", "pork"}:
        bucket = "fish_turkey_pork_main"
    if bucket == "lunch_dinner_carb_protein" and protein == "vegetarian":
        bucket = "vegetarian_legume_balanced"

    updated["target_bucket"] = bucket
    updated["selection_score"] = adjusted_score(updated)
    quality = str(updated.get("manual_review_recommendation", "review"))
    updated["quality_status"] = "keep" if quality == "keep" else quality
    return updated


def adjusted_score(analysis: dict[str, Any]) -> float:
    score = float(analysis.get("selection_score", 0.0))
    bucket = str(analysis.get("target_bucket", ""))
    protein = str(analysis.get("primary_protein", ""))
    if bucket == "breakfast_competitor":
        score += 8
    if bucket == "lunch_dinner_carb_protein":
        score += 7
    if bucket in {"fish_turkey_pork_main", "vegetarian_legume_balanced"}:
        score += 12
    if protein in {"fish", "turkey", "pork", "vegetarian"}:
        score += 5
    if analysis.get("expected_mapping_difficulty") == "low":
        score += 4
    if analysis.get("expected_fooddb_gap_risk") == "low":
        score += 3
    if analysis.get("has_carb_component"):
        score += 2
    if analysis.get("has_veg_component"):
        score += 1
    return round(score, 3)


def exclusion_reason_for_recipe(
    *,
    source_index: int,
    title_key: str,
    row: dict[str, str],
    ingredients: list[str],
    directions: list[str],
    analysis: dict[str, Any],
    exclusion_indices: set[int],
    exclusion_titles: set[str],
    seen_titles: set[str],
) -> str:
    if source_index in exclusion_indices or title_key in exclusion_titles:
        return "already_selected_generator_ready_or_repair_queue"
    if title_key in seen_titles:
        return "duplicate_or_near_duplicate_title"
    title_scope = base.normalize_text(
        f"{row.get('recipe_title')} {row.get('category')} {row.get('subcategory')}"
    )
    if any(base.contains_term(title_scope, term) for term in base.HARD_BAD_TERMS):
        return "hard_scope_exclusion"
    if any(base.contains_term(title_scope, term) for term in ROUND42_STRICT_BAD_TERMS):
        return "round42_strict_title_exclusion"
    if not ingredients or not directions:
        return "missing_ingredients_or_directions"
    if analysis.get("quality_status") == "reject":
        return "quality_gate_reject"
    if not analysis.get("target_bucket"):
        return "no_target_bucket"
    if float(analysis.get("selection_score", 0.0)) < MIN_CANDIDATE_SCORE:
        return "selection_score_below_candidate_threshold"
    return ""


def candidate_row(
    source_index: int,
    row: dict[str, str],
    ingredients: list[str],
    directions: list[str],
    analysis: dict[str, Any],
) -> dict[str, object]:
    target_bucket = str(analysis["target_bucket"])
    return {
        "recipe_id_candidate": "",
        "source_index": source_index,
        "display_name": base.clean_text(row.get("recipe_title")),
        "source_category": base.clean_text(row.get("category")),
        "source_subcategory": base.clean_text(row.get("subcategory")),
        "target_bucket": target_bucket,
        "recipe_kind_guess": "breakfast_meal" if target_bucket == "breakfast_competitor" else "complete_main",
        "primary_protein": analysis["primary_protein"],
        "has_carb_component": bool(analysis["has_carb_component"]),
        "has_veg_component": bool(analysis["has_veg_component"]),
        "expected_mapping_difficulty": analysis["expected_mapping_difficulty"],
        "expected_generator_value": expected_generator_value(target_bucket, str(analysis["primary_protein"])),
        "duplicate_risk": "low",
        "selection_reason": selection_reason(analysis),
        "risk_notes": base.risk_notes(analysis),
        "quality_status": analysis["quality_status"],
        "selection_score": analysis["selection_score"],
        "title_quality_status": analysis["title_quality_status"],
        "is_weird_or_random": bool(analysis["is_weird_or_random"]),
        "is_too_american_processed": bool(analysis["is_too_american_processed"]),
        "is_dessert_like": bool(analysis["is_dessert_like"]),
        "is_drink": bool(analysis["is_drink"]),
        "is_pet_food": bool(analysis["is_pet_food"]),
        "is_component_only": bool(analysis["is_component_only"]),
        "has_clear_protein": bool(analysis["has_clear_protein"]),
        "has_clear_carb_or_veg": bool(analysis["has_clear_carb_or_veg"]),
        "ingredient_count": analysis["ingredient_count"],
        "step_count": analysis["step_count"],
        "expected_unit_to_grams_risk": analysis["expected_unit_to_grams_risk"],
        "expected_fooddb_gap_risk": analysis["expected_fooddb_gap_risk"],
        "expected_servings_risk": analysis["expected_servings_risk"],
        "manual_review_recommendation": analysis["manual_review_recommendation"],
        "ingredients_json": json.dumps(ingredients, ensure_ascii=False),
        "directions_json": json.dumps(directions, ensure_ascii=False),
    }


def expected_generator_value(target_bucket: str, primary_protein: str) -> str:
    if target_bucket == "breakfast_competitor":
        return "breakfast_variety"
    if target_bucket == "fish_turkey_pork_main":
        if primary_protein == "fish":
            return "fish_variety"
        if primary_protein == "turkey":
            return "turkey_variety"
        if primary_protein == "pork":
            return "pork_variety"
        return "lunch_dinner_variety"
    if target_bucket == "vegetarian_legume_balanced":
        return "vegetarian_balanced"
    if target_bucket == "lunch_dinner_carb_protein":
        return "carb_protein_main"
    return "no_repeat_support"


def selection_reason(analysis: dict[str, Any]) -> str:
    return ";".join(
        [
            str(analysis["target_bucket"]),
            "protein_clear" if analysis["has_clear_protein"] else "protein_unclear",
            "carb_or_veg_clear" if analysis["has_clear_carb_or_veg"] else "carb_or_veg_unclear",
            f"mapping={analysis['expected_mapping_difficulty']}",
            "round42_dataset_curated_expansion",
        ]
    )


def exclusion_row(
    source_index: int,
    row: dict[str, str],
    analysis: dict[str, Any],
    reason: str,
) -> dict[str, object]:
    return {
        "source_index": source_index,
        "display_name": base.clean_text(row.get("recipe_title")),
        "source_category": base.clean_text(row.get("category")),
        "source_subcategory": base.clean_text(row.get("subcategory")),
        "exclusion_reason": reason,
        "target_bucket": analysis.get("target_bucket", ""),
        "primary_protein": analysis.get("primary_protein", ""),
        "has_carb_component": analysis.get("has_carb_component", False),
        "has_veg_component": analysis.get("has_veg_component", False),
        "ingredient_count": analysis.get("ingredient_count", 0),
        "step_count": analysis.get("step_count", 0),
        "selection_score": analysis.get("selection_score", 0),
    }


def select_dataset_curated(candidates: list[dict[str, object]]) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    used_titles: set[str] = set()
    keepers = [
        row
        for row in candidates
        if row["quality_status"] == "keep" and row["title_quality_status"] == "good"
    ]
    for bucket, quota in TARGET_QUOTAS.items():
        bucket_rows = [row for row in keepers if str(row["target_bucket"]) == bucket]
        for row in sorted(bucket_rows, key=selection_sort_key):
            if bucket_count(selected, bucket) >= quota:
                break
            add_selected(selected, used_titles, row)
    if len(selected) < 150:
        for row in sorted(keepers, key=selection_sort_key):
            if len(selected) >= MAX_SELECTED:
                break
            add_selected(selected, used_titles, row)
    return selected[:MAX_SELECTED]


def add_selected(
    selected: list[dict[str, object]],
    used_titles: set[str],
    row: dict[str, object],
) -> None:
    title_key = base.normalize_title_key(row["display_name"])
    if title_key in used_titles:
        return
    selected.append(row)
    used_titles.add(title_key)


def selection_sort_key(row: dict[str, object]) -> tuple[float, int, str]:
    return (-float(row["selection_score"]), bucket_rank(str(row["target_bucket"])), str(row["display_name"]))


def bucket_rank(bucket: str) -> int:
    return list(TARGET_QUOTAS).index(bucket) if bucket in TARGET_QUOTAS else 99


def bucket_count(rows: list[dict[str, object]], bucket: str) -> int:
    return sum(1 for row in rows if row["target_bucket"] == bucket)


def finalize_selected_row(index: int, row: dict[str, object]) -> dict[str, object]:
    finalized = dict(row)
    finalized["recipe_id_candidate"] = f"recipes_v1_2_round42_dataset_{index:03d}"
    return finalized


def finalize_candidate_pool_row(row: dict[str, object], index: int) -> dict[str, object]:
    finalized = dict(row)
    finalized["recipe_id_candidate"] = f"round42_dataset_pool_{index:04d}"
    return finalized


def build_summary(
    selected_rows: list[dict[str, object]],
    candidate_rows: list[dict[str, object]],
    exclusion_rows: list[dict[str, object]],
) -> str:
    selected_buckets = Counter(str(row["target_bucket"]) for row in selected_rows)
    candidate_buckets = Counter(str(row["target_bucket"]) for row in candidate_rows)
    selected_proteins = Counter(str(row["primary_protein"]) for row in selected_rows)
    quality_counts = Counter(str(row["quality_status"]) for row in candidate_rows)
    exclusions = Counter(str(row["exclusion_reason"]) for row in exclusion_rows)
    lines = [
        "Recipes_DB v1.2 Round42 dataset-curated selection",
        "",
        f"source={SOURCE_RECIPES}",
        f"baseline_dataset={BASELINE_RECIPES}",
        f"manual_repair_queue={MANUAL_REPAIR_QUEUE}",
        f"round40_dataset_plan={ROUND40_DATASET_PLAN}",
        f"round40_gap_analysis={ROUND40_GAP_ANALYSIS}",
        f"candidate_pool_written={len(candidate_rows)}",
        f"selected_count={len(selected_rows)}",
        f"min_candidate_score={MIN_CANDIDATE_SCORE}",
        "",
        "Selected bucket mix:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in selected_buckets.most_common())
    lines.extend(["", "Candidate bucket mix:"])
    lines.extend(f"- {name}: {count}" for name, count in candidate_buckets.most_common())
    lines.extend(["", "Candidate quality mix:"])
    lines.extend(f"- {name}: {count}" for name, count in quality_counts.most_common())
    lines.extend(["", "Selected protein mix:"])
    lines.extend(f"- {name}: {count}" for name, count in selected_proteins.most_common())
    lines.extend(["", "Top exclusion reasons:"])
    lines.extend(f"- {name}: {count}" for name, count in exclusions.most_common(20))
    lines.extend(["", "Selected recipes:"])
    for row in selected_rows:
        lines.append(
            f"- {row['recipe_id_candidate']} | {row['target_bucket']} | "
            f"{row['display_name']} | score={row['selection_score']}"
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
