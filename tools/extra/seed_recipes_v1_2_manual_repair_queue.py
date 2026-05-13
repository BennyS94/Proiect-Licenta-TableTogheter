from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RECIPES_DRAFT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft"
AUDIT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"

QUEUE_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_repair_queue.csv"
SEED_SUMMARY_PATH = AUDIT_DIR / "recipes_v1_2_manual_repair_queue_seed_summary.txt"
SEED_AUDIT_PATH = AUDIT_DIR / "recipes_v1_2_manual_repair_queue_seed_audit.csv"


QUEUE_COLUMNS = [
    "repair_id",
    "recipe_id_candidate",
    "source_index",
    "display_name",
    "recipe_source",
    "recipe_kind_guess",
    "target_slot",
    "target_bucket",
    "priority",
    "problem_type",
    "blocking_ingredient",
    "ingredient_raw_text",
    "ingredient_name_normalized",
    "current_mapping_status",
    "current_quantity_grams_estimated",
    "proposed_fix_type",
    "proposed_fix_detail",
    "needs_fooddb_addition",
    "needs_alias",
    "needs_unit_rule",
    "needs_servings_fix",
    "needs_web_source",
    "source_needed",
    "source_name",
    "source_url",
    "decision_status",
    "decision_notes",
    "expected_generator_value",
    "created_from_round",
    "qc_notes",
]

SEED_AUDIT_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "created_from_round",
    "target_bucket",
    "priority",
    "problem_type",
    "proposed_fix_type",
    "blocking_ingredient",
    "generator_ready_failure_reason",
    "action",
    "skip_reason",
]

ROUND_INPUTS = [
    {
        "round_id": "round28",
        "selection_path": RECIPES_DRAFT_DIR / "recipes_v1_2_round28_targeted_plus30.csv",
        "ready_audit_path": AUDIT_DIR / "recipes_v1_2_round28_plus30_generator_ready_audit.csv",
        "unmapped_path": RECIPES_DRAFT_DIR / "recipes_v1_2_round28_plus30_unmapped.csv",
    },
    {
        "round_id": "round30",
        "selection_path": RECIPES_DRAFT_DIR / "recipes_v1_2_round30_targeted_plus15.csv",
        "ready_audit_path": AUDIT_DIR / "recipes_v1_2_round30_plus15_generator_ready_audit.csv",
        "unmapped_path": RECIPES_DRAFT_DIR / "recipes_v1_2_round30_plus15_unmapped.csv",
    },
]

LIKELY_USEFUL_NAMES = {
    "perfect chicken vegetable soup",
    "white bean and tomato pasta",
    "black bean stuffed peppers",
    "creamy smoked salmon pasta",
    "salmon and spinach fettuccine",
    "skillet pork chops with potatoes and onion",
    "pork fried rice",
    "easy egg fried rice",
}

SEMANTIC_REJECT_FLAGS = [
    "is_weird_or_random",
    "is_too_american_processed",
    "is_dessert_like",
    "is_drink",
    "is_pet_food",
    "is_component_only",
]

TARGET_BUCKET_VALUES = {
    "breakfast_competitor",
    "carb_protein_main",
    "fish_turkey_pork_legume_main",
}

LIKELY_FOODDB_ADDITIONS = {
    "ham",
    "cooked ham",
    "chopped ham",
    "diced ham",
    "pepper jack cheese",
    "jalapeno pepper jack cheese",
    "smoked salmon",
    "pork chop",
    "pork chops",
    "pork",
    "swiss cheese",
    "mozzarella cheese",
}

LIKELY_ALIAS_FIXES = {
    "cheddar cheese",
    "shredded cheddar cheese",
    "hash brown potatoes",
    "hash browns",
    "spinach",
    "fresh spinach",
    "tortilla",
    "flour tortilla",
    "fettuccine",
    "linguini",
    "orzo",
    "black beans",
    "white beans",
    "rice",
    "potatoes",
    "pasta",
}


def main() -> None:
    RECIPES_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    ensure_queue_exists()

    queue_rows = read_csv(QUEUE_PATH)
    existing_keys = {dedupe_key(row) for row in queue_rows}
    next_index = next_repair_index(queue_rows)

    seed_audit_rows: list[dict[str, str]] = []
    added_rows: list[dict[str, str]] = []

    scanned_count = 0
    eligible_count = 0
    skipped_semantic_count = 0
    skipped_not_useful_count = 0
    duplicate_count = 0

    for input_set in ROUND_INPUTS:
        selection_rows = read_csv(input_set["selection_path"])
        audit_rows_by_id = index_by_recipe_id(read_csv(input_set["ready_audit_path"]))
        unmapped_by_id = group_by_recipe_id(read_csv(input_set["unmapped_path"]))

        for selection_row in selection_rows:
            scanned_count += 1
            recipe_id = selection_row.get("recipe_id_candidate", "")
            audit_row = audit_rows_by_id.get(recipe_id, {})
            merged = {**selection_row, **audit_row}
            display_name = merged.get("display_name", "")

            skip_reason = semantic_skip_reason(merged)
            if skip_reason:
                skipped_semantic_count += 1
                seed_audit_rows.append(seed_audit_row(merged, input_set["round_id"], "skipped", skip_reason))
                continue

            if not is_failed_ready_candidate(merged):
                seed_audit_rows.append(seed_audit_row(merged, input_set["round_id"], "skipped", "already_generator_ready"))
                continue

            if not is_worth_manual_repair(merged):
                skipped_not_useful_count += 1
                seed_audit_rows.append(seed_audit_row(merged, input_set["round_id"], "skipped", "not_high_value_for_repair"))
                continue

            eligible_count += 1
            blocking_row = choose_blocking_ingredient(unmapped_by_id.get(recipe_id, []))
            problem_type = infer_problem_type(merged, blocking_row)
            proposed_fix_type = infer_proposed_fix_type(merged, blocking_row)
            queue_row = build_queue_row(
                merged,
                blocking_row,
                input_set["round_id"],
                problem_type,
                proposed_fix_type,
            )
            row_key = dedupe_key(queue_row)
            if row_key in existing_keys:
                duplicate_count += 1
                seed_audit_rows.append(queue_seed_audit_row(queue_row, merged, "skipped", "duplicate_queue_row"))
                continue

            queue_row["repair_id"] = f"repair_v1_2_{next_index:04d}"
            next_index += 1
            existing_keys.add(row_key)
            added_rows.append(queue_row)
            seed_audit_rows.append(queue_seed_audit_row(queue_row, merged, "added", ""))

    if added_rows:
        with QUEUE_PATH.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=QUEUE_COLUMNS)
            for row in added_rows:
                writer.writerow(row)

    write_seed_audit(seed_audit_rows)
    write_seed_summary(
        scanned_count=scanned_count,
        eligible_count=eligible_count,
        added_rows=added_rows,
        duplicate_count=duplicate_count,
        skipped_semantic_count=skipped_semantic_count,
        skipped_not_useful_count=skipped_not_useful_count,
        final_queue_count=len(queue_rows) + len(added_rows),
    )

    print("Recipes_DB v1.2 manual repair queue seeded")
    print(f"- scanned candidates: {scanned_count}")
    print(f"- eligible useful failures: {eligible_count}")
    print(f"- added repair rows: {len(added_rows)}")
    print(f"- final queue rows: {len(queue_rows) + len(added_rows)}")


def ensure_queue_exists() -> None:
    if QUEUE_PATH.exists():
        return
    with QUEUE_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=QUEUE_COLUMNS)
        writer.writeheader()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_seed_audit(rows: list[dict[str, str]]) -> None:
    with SEED_AUDIT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SEED_AUDIT_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def write_seed_summary(
    *,
    scanned_count: int,
    eligible_count: int,
    added_rows: list[dict[str, str]],
    duplicate_count: int,
    skipped_semantic_count: int,
    skipped_not_useful_count: int,
    final_queue_count: int,
) -> None:
    priority_counts = Counter(row.get("priority", "") for row in added_rows)
    problem_counts = Counter(row.get("problem_type", "") for row in added_rows)
    fix_counts = Counter(row.get("proposed_fix_type", "") for row in added_rows)
    bucket_counts = Counter(row.get("target_bucket", "") for row in added_rows)
    blocking_counts = Counter(
        row.get("ingredient_name_normalized", "") or row.get("blocking_ingredient", "")
        for row in added_rows
    )

    lines = [
        "Recipes_DB v1.2 manual repair queue seed summary",
        "",
        f"- scanned candidates: {scanned_count}",
        f"- eligible useful failed candidates: {eligible_count}",
        f"- added repair rows: {len(added_rows)}",
        f"- duplicate rows skipped: {duplicate_count}",
        f"- semantic rejects skipped: {skipped_semantic_count}",
        f"- low-value failures skipped: {skipped_not_useful_count}",
        f"- final repair queue rows: {final_queue_count}",
        "",
        "Priority counts",
        *format_counter(priority_counts),
        "",
        "Problem type counts",
        *format_counter(problem_counts),
        "",
        "Proposed fix type counts",
        *format_counter(fix_counts),
        "",
        "Target bucket counts",
        *format_counter(bucket_counts),
        "",
        "Top blocking ingredients",
        *format_counter(blocking_counts, limit=12),
        "",
        "Interpretation",
        "- Seed rows are draft repair candidates, not approved fixes.",
        "- Food_DB additions still require verified nutrition sources before application.",
        "- Recipes that are semantically bad were skipped instead of repaired.",
    ]
    SEED_SUMMARY_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def format_counter(counter: Counter[str], limit: int | None = None) -> list[str]:
    if not counter:
        return ["- none"]
    items = counter.most_common(limit)
    return [f"- {key or 'blank'}: {value}" for key, value in items]


def index_by_recipe_id(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row.get("recipe_id_candidate", ""): row for row in rows}


def group_by_recipe_id(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[row.get("recipe_id_candidate", "")].append(row)
    return grouped


def next_repair_index(rows: list[dict[str, str]]) -> int:
    max_index = 0
    for row in rows:
        value = row.get("repair_id", "")
        if value.startswith("repair_v1_2_"):
            suffix = value.rsplit("_", 1)[-1]
            if suffix.isdigit():
                max_index = max(max_index, int(suffix))
    return max_index + 1


def dedupe_key(row: dict[str, str]) -> tuple[str, str, str, str]:
    return (
        row.get("recipe_id_candidate", ""),
        row.get("problem_type", ""),
        row.get("ingredient_name_normalized", ""),
        row.get("proposed_fix_type", ""),
    )


def seed_audit_row(row: dict[str, str], round_id: str, action: str, skip_reason: str) -> dict[str, str]:
    return {
        "recipe_id_candidate": row.get("recipe_id_candidate", ""),
        "display_name": row.get("display_name", ""),
        "created_from_round": round_id,
        "target_bucket": row.get("target_bucket", ""),
        "priority": infer_priority(row),
        "problem_type": infer_problem_type(row, {}),
        "proposed_fix_type": infer_proposed_fix_type(row, {}),
        "blocking_ingredient": "",
        "generator_ready_failure_reason": row.get("generator_ready_failure_reason", ""),
        "action": action,
        "skip_reason": skip_reason,
    }


def queue_seed_audit_row(
    queue_row: dict[str, str],
    source_row: dict[str, str],
    action: str,
    skip_reason: str,
) -> dict[str, str]:
    return {
        "recipe_id_candidate": queue_row.get("recipe_id_candidate", ""),
        "display_name": queue_row.get("display_name", ""),
        "created_from_round": queue_row.get("created_from_round", ""),
        "target_bucket": queue_row.get("target_bucket", ""),
        "priority": queue_row.get("priority", ""),
        "problem_type": queue_row.get("problem_type", ""),
        "proposed_fix_type": queue_row.get("proposed_fix_type", ""),
        "blocking_ingredient": queue_row.get("blocking_ingredient", ""),
        "generator_ready_failure_reason": source_row.get("generator_ready_failure_reason", ""),
        "action": action,
        "skip_reason": skip_reason,
    }


def semantic_skip_reason(row: dict[str, str]) -> str:
    if row.get("title_quality_status", "").lower() == "reject":
        return "title_quality_reject"
    if row.get("manual_review_recommendation", "").lower() == "reject":
        return "manual_review_reject"
    for flag in SEMANTIC_REJECT_FLAGS:
        if to_bool(row.get(flag, "")):
            return flag
    has_protein = to_bool(row.get("has_clear_protein", ""))
    has_carb_or_veg = to_bool(row.get("has_clear_carb_or_veg", ""))
    if not has_protein and not has_carb_or_veg:
        return "no_clear_meal_role"
    return ""


def is_failed_ready_candidate(row: dict[str, str]) -> bool:
    ready_value = row.get("generator_ready_candidate", "")
    if ready_value and not to_bool(ready_value):
        return True
    selected_flags = [
        row.get("selected_for_plus30_dataset", ""),
        row.get("selected_for_plus15_dataset", ""),
    ]
    return any(value and not to_bool(value) for value in selected_flags)


def is_worth_manual_repair(row: dict[str, str]) -> bool:
    name_key = row.get("display_name", "").strip().lower()
    if name_key in LIKELY_USEFUL_NAMES:
        return True
    if row.get("target_bucket", "") in TARGET_BUCKET_VALUES:
        return True
    expected_value = row.get("expected_generator_value", "")
    return any(
        token in expected_value
        for token in [
            "waffle_alternative",
            "chicken_broccoli_pasta_alternative",
            "lunch_dinner_variety",
            "breakfast_variety",
            "cabbage_alternative",
        ]
    )


def choose_blocking_ingredient(rows: list[dict[str, str]]) -> dict[str, str]:
    if not rows:
        return {}

    def sort_key(row: dict[str, str]) -> tuple[int, float, int]:
        grams = to_float(row.get("quantity_grams_estimated", ""))
        status = row.get("mapping_status", "")
        grams_missing = 1 if grams <= 0 else 0
        status_weight = 1 if status == "unmapped" else 0
        return (status_weight, grams, grams_missing)

    useful_rows = [
        row for row in rows
        if row.get("mapping_status", "") in {"unmapped", "review_needed"}
    ]
    if not useful_rows:
        return rows[0]
    return sorted(useful_rows, key=sort_key, reverse=True)[0]


def infer_problem_type(row: dict[str, str], blocking_row: dict[str, str]) -> str:
    failure = row.get("generator_ready_failure_reason", "").lower()
    flags = row.get("quality_flags", "").lower()
    mapping_status = blocking_row.get("mapping_status", "")
    grams = blocking_row.get("quantity_grams_estimated", "")
    if "mapped_weight_ratio" in failure or "mapped_weight_ratio" in flags:
        return "mapping_gap"
    if mapping_status in {"unmapped", "review_needed"} and not grams:
        return "unit_or_mapping_gap"
    if any(token in failure for token in ["kcal", "protein", "carbs"]):
        return "nutrition_threshold_or_servings"
    if row.get("uses_pilot_servings_fallback", "").lower() == "true":
        return "servings_review"
    if mapping_status:
        return "mapping_gap"
    return "manual_review_needed"


def infer_proposed_fix_type(row: dict[str, str], blocking_row: dict[str, str]) -> str:
    name = normalized_ingredient_name(blocking_row)
    grams = blocking_row.get("quantity_grams_estimated", "")
    mapping_status = blocking_row.get("mapping_status", "")
    failure = row.get("generator_ready_failure_reason", "").lower()

    if name in LIKELY_FOODDB_ADDITIONS:
        return "fooddb_addition"
    if mapping_status in {"unmapped", "review_needed"} and not grams:
        return "unit_rule"
    if name in LIKELY_ALIAS_FIXES:
        return "alias_mapping"
    if any(token in failure for token in ["kcal", "protein", "carbs"]):
        return "servings_fix"
    if mapping_status in {"unmapped", "review_needed"}:
        return "alias_mapping"
    return "recipe_metadata_fix"


def build_queue_row(
    row: dict[str, str],
    blocking_row: dict[str, str],
    round_id: str,
    problem_type: str,
    proposed_fix_type: str,
) -> dict[str, str]:
    name = normalized_ingredient_name(blocking_row)
    needs_fooddb_addition = proposed_fix_type == "fooddb_addition"
    needs_alias = proposed_fix_type == "alias_mapping" or (
        blocking_row.get("mapping_status", "") in {"unmapped", "review_needed"}
        and proposed_fix_type != "fooddb_addition"
    )
    needs_unit_rule = proposed_fix_type == "unit_rule" or (
        bool(blocking_row) and not blocking_row.get("quantity_grams_estimated", "")
    )
    needs_servings_fix = proposed_fix_type == "servings_fix" or problem_type in {
        "nutrition_threshold_or_servings",
        "servings_review",
    }
    needs_web_source = needs_fooddb_addition

    return {
        "repair_id": "",
        "recipe_id_candidate": row.get("recipe_id_candidate", ""),
        "source_index": row.get("source_index", ""),
        "display_name": row.get("display_name", ""),
        "recipe_source": "recipes_v1_2_targeted_expansion",
        "recipe_kind_guess": row.get("recipe_kind_guess", ""),
        "target_slot": infer_target_slot(row),
        "target_bucket": row.get("target_bucket", ""),
        "priority": infer_priority(row),
        "problem_type": problem_type,
        "blocking_ingredient": name,
        "ingredient_raw_text": blocking_row.get("ingredient_raw_text", ""),
        "ingredient_name_normalized": name,
        "current_mapping_status": blocking_row.get("mapping_status", ""),
        "current_quantity_grams_estimated": blocking_row.get("quantity_grams_estimated", ""),
        "proposed_fix_type": proposed_fix_type,
        "proposed_fix_detail": proposed_fix_detail(proposed_fix_type, name, row),
        "needs_fooddb_addition": bool_text(needs_fooddb_addition),
        "needs_alias": bool_text(needs_alias),
        "needs_unit_rule": bool_text(needs_unit_rule),
        "needs_servings_fix": bool_text(needs_servings_fix),
        "needs_web_source": bool_text(needs_web_source),
        "source_needed": "verified_food_nutrition_source" if needs_web_source else "",
        "source_name": "",
        "source_url": "",
        "decision_status": "pending",
        "decision_notes": "",
        "expected_generator_value": infer_expected_generator_value(row),
        "created_from_round": round_id,
        "qc_notes": qc_notes(row),
    }


def normalized_ingredient_name(row: dict[str, str]) -> str:
    return (
        row.get("ingredient_name_normalized", "")
        or row.get("ingredient_name_parsed", "")
        or row.get("ingredient_raw_text", "")
    ).strip().lower()


def infer_target_slot(row: dict[str, str]) -> str:
    bucket = row.get("target_bucket", "")
    if bucket == "breakfast_competitor":
        return "breakfast"
    if bucket in {"carb_protein_main", "fish_turkey_pork_legume_main"}:
        return "lunch,dinner"
    return "review"


def infer_priority(row: dict[str, str]) -> str:
    name_key = row.get("display_name", "").strip().lower()
    if name_key in LIKELY_USEFUL_NAMES:
        return "high"
    if row.get("target_bucket", "") in {
        "breakfast_competitor",
        "carb_protein_main",
        "fish_turkey_pork_legume_main",
    }:
        return "high"
    return "medium"


def infer_expected_generator_value(row: dict[str, str]) -> str:
    expected = row.get("expected_generator_value", "")
    bucket = row.get("target_bucket", "")
    protein = row.get("primary_protein", "").lower()
    if "waffle" in expected or bucket == "breakfast_competitor":
        return "breakfast_variety"
    if bucket == "carb_protein_main":
        return "carb_protein_main"
    if any(token in protein for token in ["fish", "turkey", "pork", "salmon", "tuna"]):
        return "fish_turkey_pork_main"
    if any(token in protein for token in ["bean", "lentil", "vegetarian"]):
        return "vegetarian_balanced"
    if "lunch" in expected or "dinner" in expected:
        return "lunch_dinner_variety"
    return "other"


def proposed_fix_detail(proposed_fix_type: str, ingredient_name: str, row: dict[str, str]) -> str:
    display_name = row.get("display_name", "")
    if proposed_fix_type == "fooddb_addition":
        return f"Review canonical Food_DB ingredient for '{ingredient_name}' used by {display_name}."
    if proposed_fix_type == "alias_mapping":
        return f"Review safe alias mapping for '{ingredient_name}' used by {display_name}."
    if proposed_fix_type == "unit_rule":
        return f"Review unit-to-grams rule for '{ingredient_name}' used by {display_name}."
    if proposed_fix_type == "servings_fix":
        return f"Review servings or portion assumptions for {display_name}."
    return f"Manual metadata review for {display_name}."


def qc_notes(row: dict[str, str]) -> str:
    parts = []
    if row.get("generator_ready_failure_reason", ""):
        parts.append(f"failure={row.get('generator_ready_failure_reason', '')}")
    if row.get("quality_flags", ""):
        parts.append(f"quality_flags={row.get('quality_flags', '')}")
    if row.get("manual_review_recommendation", ""):
        parts.append(f"manual_review={row.get('manual_review_recommendation', '')}")
    return "; ".join(parts)


def bool_text(value: bool) -> str:
    return "true" if value else "false"


def to_bool(value: str) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def to_float(value: str) -> float:
    try:
        return float(str(value).strip())
    except ValueError:
        return 0.0


if __name__ == "__main__":
    main()
