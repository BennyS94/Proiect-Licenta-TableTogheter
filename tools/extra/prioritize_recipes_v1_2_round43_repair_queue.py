from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
RECIPES_AUDIT_DIR = ROOT / "data" / "recipesdb" / "audit"
RECIPES_DRAFT_DIR = ROOT / "data" / "recipesdb" / "draft"
FOODDB_AUDIT_DIR = ROOT / "data" / "fooddb" / "audit"
FOODDB_DRAFT_DIR = ROOT / "data" / "fooddb" / "draft"

REPAIR_QUEUE_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_repair_queue.csv"
ROUND42_ADDITIONS_PATH = RECIPES_AUDIT_DIR / "recipes_v1_2_round42_manual_repair_queue_additions.csv"
GEN_READY_AUDIT_PATH = RECIPES_AUDIT_DIR / "recipes_v1_2_round42_dataset_generator_ready_audit.csv"
SELECTED_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_round42_dataset_curated_selected.csv"
VERIFIED_BATCH1_PATH = FOODDB_DRAFT_DIR / "fooddb_v1_2_manual_additions_verified_batch1.csv"
SOURCE_QUEUE_PATH = FOODDB_AUDIT_DIR / "fooddb_v1_2_manual_source_verification_queue.csv"

PRIORITY_OUT = RECIPES_AUDIT_DIR / "recipes_v1_2_round43_repair_queue_prioritized.csv"
SUMMARY_OUT = RECIPES_AUDIT_DIR / "recipes_v1_2_round43_repair_queue_priority_summary.txt"
RECIPE_SOURCE_BATCH2_OUT = RECIPES_AUDIT_DIR / "recipes_v1_2_round43_source_verification_batch2_candidates.csv"
FOOD_SOURCE_PLAN_OUT = FOODDB_AUDIT_DIR / "fooddb_v1_2_source_verification_batch2_plan.txt"
FOOD_SOURCE_CANDIDATES_OUT = FOODDB_AUDIT_DIR / "fooddb_v1_2_source_verification_batch2_candidates.csv"


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def write_text(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def truthy(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def normalize(text: str) -> str:
    return " ".join((text or "").lower().replace("_", " ").replace("-", " ").split())


def split_items(text: str) -> list[str]:
    raw = (text or "").strip()
    if not raw:
        return []
    parts = re.split(r"[;|,]", raw)
    return [normalize(part) for part in parts if normalize(part)]


def safe_float(value: object) -> float:
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def semantically_weak(row: dict[str, str]) -> bool:
    text = normalize(" ".join([
        row.get("display_name", ""),
        row.get("target_bucket", ""),
        row.get("risk_notes", ""),
        row.get("why_failed", ""),
    ]))
    weak_terms = [
        "cocktail", "drink", "dessert", "cake", "cookie", "candy", "ice cream",
        "dog food", "cat food", "pet", "sauce only", "component", "brand heavy",
    ]
    return any(term in text for term in weak_terms)


def source_sensitive_ingredient(ingredient: str) -> bool:
    text = normalize(ingredient)
    sensitive_terms = [
        "beef", "steak", "pork", "lamb", "chicken thigh", "salmon", "cod", "fish",
        "tuna", "cheese", "sour cream", "cream", "milk", "beans", "bean", "lentil",
        "guanciale", "pie crust", "pastry", "biscuit", "baked beans", "refried beans",
        "coconut milk", "evaporated milk", "plant milk", "oat milk", "soy milk",
    ]
    return any(term in text for term in sensitive_terms)


def generic_ambiguity(ingredient: str) -> bool:
    text = normalize(ingredient)
    return text in {"beef", "pork", "turkey", "chicken", "fish", "meat", "steak"} or text.startswith("generic ")


def source_type_for_ingredient(ingredient: str) -> str:
    text = normalize(ingredient)
    if any(term in text for term in ["cheese", "cream", "milk", "beans", "pastry", "biscuit", "guanciale"]):
        return "official_nutrition_database_or_manufacturer"
    if any(term in text for term in ["beef", "pork", "lamb", "chicken", "fish", "salmon", "cod", "tuna"]):
        return "CIQUAL_or_USDA"
    return "official_nutrition_database"


def already_safe_added_names(rows: list[dict[str, str]]) -> set[str]:
    safe: set[str] = set()
    for row in rows:
        if normalize(row.get("decision", "")) == "safe to add" or normalize(row.get("decision", "")) == "safe_to_add":
            for value in [row.get("canonical_name", ""), row.get("display_name", ""), row.get("candidate_food_id", "")]:
                if value:
                    safe.add(normalize(value))
    return safe


def score_row(row: dict[str, str], blocker_frequency: Counter[str], selected_by_id: dict[str, dict[str, str]], gen_audit_by_id: dict[str, dict[str, str]]) -> tuple[int, str, str]:
    score = 0
    reasons: list[str] = []
    recipe_id = row.get("recipe_id_candidate", "")
    selected = selected_by_id.get(recipe_id, {})
    audit = gen_audit_by_id.get(recipe_id, {})
    target_bucket = row.get("target_bucket") or selected.get("target_bucket", "")
    expected_value = row.get("expected_generator_value") or selected.get("expected_generator_value", "")
    failure_reason = row.get("why_failed") or audit.get("generator_ready_failure_reason") or row.get("proposed_fix_detail", "")
    blocker_items = split_items(row.get("blocking_ingredients") or row.get("blocking_ingredient") or row.get("needed_fooddb_items", ""))
    blocker = blocker_items[0] if blocker_items else normalize(row.get("blocking_ingredient", ""))

    if target_bucket in {"fish_turkey_pork_main", "vegetarian_legume_balanced"}:
        score += 30
        reasons.append("fills_high_value_gap_bucket")
    elif target_bucket in {"lunch_dinner_carb_protein", "carb_protein_main"}:
        score += 26
        reasons.append("fills_lunch_dinner_carb_protein_gap")
    elif target_bucket == "breakfast_competitor":
        score += 20
        reasons.append("fills_breakfast_variety_gap")
    elif target_bucket == "flexible_high_value":
        score += 16
        reasons.append("flexible_high_value")

    value_text = normalize(expected_value)
    for label, points in [
        ("fish", 12),
        ("turkey", 12),
        ("pork", 10),
        ("vegetarian", 12),
        ("legume", 12),
        ("carb protein", 10),
        ("lunch dinner", 8),
        ("breakfast", 7),
        ("no repeat", 6),
    ]:
        if label in value_text:
            score += points
            reasons.append(f"value_{label.replace(' ', '_')}")

    unique_blockers = set(blocker_items)
    if len(unique_blockers) <= 1:
        score += 20
        reasons.append("one_blocker")
    elif len(unique_blockers) <= 2:
        score += 12
        reasons.append("two_blockers")
    else:
        score -= 8
        reasons.append("many_blockers")

    fix_type = normalize(row.get("proposed_fix_type", ""))
    if "alias" in fix_type:
        score += 13
        reasons.append("likely_alias_repair")
    if "unit" in fix_type:
        score += 9
        reasons.append("unit_rule_repair")
    if "servings" in fix_type:
        score += 4
        reasons.append("servings_review_needed")
    if "fooddb" in fix_type:
        score += 7
        reasons.append("fooddb_candidate_repair")

    if blocker:
        freq = blocker_frequency[blocker]
        score += min(25, freq * 4)
        reasons.append(f"blocker_frequency_{freq}")

    if source_sensitive_ingredient(blocker):
        score += 7
        reasons.append("common_source_sensitive_ingredient")
    if truthy(row.get("needs_web_source")) or truthy(row.get("source_needed")) or truthy(row.get("needs_fooddb_addition")):
        score += 5
        reasons.append("source_verification_candidate")
    if generic_ambiguity(blocker):
        score -= 18
        reasons.append("generic_mapping_ambiguity")
    if semantically_weak({**selected, **row}):
        score -= 30
        reasons.append("semantic_quality_risk")

    failure_count = len(split_items(failure_reason.replace("_lt_", " lt ").replace("_gt_", " gt ")))
    if failure_reason.count(";") >= 3:
        score -= 6
        reasons.append("many_failure_flags")
    if "breakfast_kcal_per_serving_gt_800" in failure_reason:
        score -= 8
        reasons.append("possibly_too_heavy_breakfast")
    if "mapped_weight_ratio_lt_0.45" in failure_reason and "macro_relevant_mapped_weight_ratio_lt_0.50" in failure_reason:
        score -= 5
        reasons.append("coverage_gap_large")
    if failure_count > 6:
        score -= 4
        reasons.append("complex_failure_pattern")

    if score >= 72:
        tier = "batch2_high"
    elif score >= 52:
        tier = "batch2_medium"
    elif score >= 28:
        tier = "backlog"
    else:
        tier = "reject"
    if generic_ambiguity(blocker) and tier in {"batch2_high", "batch2_medium"}:
        tier = "backlog"
        reasons.append("tier_capped_for_generic_ambiguity")
    return score, tier, ";".join(reasons)


def main() -> None:
    repair_rows = read_csv(REPAIR_QUEUE_PATH)
    round42_rows = read_csv(ROUND42_ADDITIONS_PATH)
    gen_audit_rows = read_csv(GEN_READY_AUDIT_PATH)
    selected_rows = read_csv(SELECTED_PATH)
    verified_rows = read_csv(VERIFIED_BATCH1_PATH)
    source_queue_rows = read_csv(SOURCE_QUEUE_PATH)

    if not round42_rows:
        round42_rows = [row for row in repair_rows if normalize(row.get("created_from_round", "")) == "round42"]

    selected_by_id = {row.get("recipe_id_candidate", ""): row for row in selected_rows}
    gen_audit_by_id = {row.get("recipe_id_candidate", ""): row for row in gen_audit_rows}
    safe_added = already_safe_added_names(verified_rows)
    source_queue_by_ingredient = {normalize(row.get("ingredient_name_normalized", "")): row for row in source_queue_rows}

    blocker_frequency: Counter[str] = Counter()
    blocker_to_recipes: dict[str, set[str]] = defaultdict(set)
    blocker_to_names: dict[str, set[str]] = defaultdict(set)
    for row in round42_rows:
        recipe_id = row.get("recipe_id_candidate", "")
        recipe_name = row.get("display_name", "")
        blockers = split_items(row.get("blocking_ingredients") or row.get("blocking_ingredient") or row.get("needed_fooddb_items", ""))
        if not blockers:
            blockers = [normalize(row.get("blocking_ingredient", ""))]
        for blocker in [item for item in blockers if item]:
            blocker_frequency[blocker] += 1
            blocker_to_recipes[blocker].add(recipe_id)
            blocker_to_names[blocker].add(recipe_name)

    prioritized_rows: list[dict[str, object]] = []
    for row in round42_rows:
        recipe_id = row.get("recipe_id_candidate", "")
        selected = selected_by_id.get(recipe_id, {})
        audit = gen_audit_by_id.get(recipe_id, {})
        score, tier, reason = score_row(row, blocker_frequency, selected_by_id, gen_audit_by_id)
        blocking_ingredients = row.get("blocking_ingredients") or row.get("blocking_ingredient") or row.get("needed_fooddb_items", "")
        source_needed = truthy(row.get("needs_web_source")) or truthy(row.get("source_needed")) or truthy(row.get("needs_fooddb_addition")) or any(source_sensitive_ingredient(item) for item in split_items(blocking_ingredients))
        prioritized_rows.append({
            "recipe_id_candidate": recipe_id,
            "display_name": row.get("display_name", ""),
            "target_bucket": row.get("target_bucket") or selected.get("target_bucket", ""),
            "expected_generator_value": row.get("expected_generator_value") or selected.get("expected_generator_value", ""),
            "failure_reason": row.get("why_failed") or audit.get("generator_ready_failure_reason") or row.get("proposed_fix_detail", ""),
            "blocking_ingredients": blocking_ingredients,
            "proposed_fix_type": row.get("proposed_fix_type", ""),
            "needs_fooddb_addition": row.get("needs_fooddb_addition", ""),
            "needs_alias": row.get("needs_alias", ""),
            "needs_unit_rule": row.get("needs_unit_rule", ""),
            "needs_servings_fix": row.get("needs_servings_fix", ""),
            "needs_web_source": str(source_needed).lower(),
            "priority_score": score,
            "priority_tier": tier,
            "reason_for_priority": reason,
        })
    prioritized_rows.sort(key=lambda row: (-int(row["priority_score"]), str(row["display_name"])))

    candidate_groups: list[dict[str, object]] = []
    for blocker, count in blocker_frequency.most_common():
        if not blocker:
            continue
        affected_recipes = sorted(blocker_to_recipes[blocker])
        affected_names = sorted(name for name in blocker_to_names[blocker] if name)
        rows_for_blocker = [row for row in prioritized_rows if blocker in split_items(str(row["blocking_ingredients"]))]
        high_rows = [row for row in rows_for_blocker if row["priority_tier"] in {"batch2_high", "batch2_medium"}]
        needs_source = source_sensitive_ingredient(blocker) or any(str(row["needs_web_source"]).lower() == "true" for row in rows_for_blocker)
        action = "add_fooddb_item_or_verify_alias" if needs_source else "alias_or_unit_review"
        source_queue_hit = source_queue_by_ingredient.get(blocker, {})
        if blocker in safe_added:
            action = "already_safe_added_review_alias_usage"
        if generic_ambiguity(blocker):
            action = "keep_deferred_generic_ambiguity"
            needs_source = False
        group_score = count * 10 + len(high_rows) * 8
        if needs_source:
            group_score += 15
        if generic_ambiguity(blocker):
            group_score -= 80
        candidate_groups.append({
            "ingredient_or_blocker": blocker,
            "affected_recipes_count": len(affected_recipes),
            "affected_recipe_ids": ";".join(affected_recipes[:20]),
            "affected_recipe_names": ";".join(affected_names[:12]),
            "expected_recovered_recipes": len(set(row["recipe_id_candidate"] for row in high_rows)),
            "source_needed": str(needs_source).lower(),
            "recommended_source_type": source_queue_hit.get("suggested_source_type") or source_type_for_ingredient(blocker),
            "recommended_action": action,
            "priority_score": group_score,
            "risk_notes": "generic_ambiguity" if generic_ambiguity(blocker) else ("source_sensitive" if needs_source else "low_source_risk"),
            "why_it_matters": "affects_multiple_repair_candidates" if count > 1 else "single_recipe_but_may_fill_gap",
        })

    candidate_groups.sort(key=lambda row: (-int(row["priority_score"]), str(row["ingredient_or_blocker"])))
    source_candidates = [
        row for row in candidate_groups
        if row["source_needed"] == "true"
        and row["recommended_action"] != "already_safe_added_review_alias_usage"
        and row["priority_score"] >= 18
        and int(row["expected_recovered_recipes"]) > 0
    ][:20]

    write_csv(PRIORITY_OUT, prioritized_rows, [
        "recipe_id_candidate", "display_name", "target_bucket", "expected_generator_value",
        "failure_reason", "blocking_ingredients", "proposed_fix_type", "needs_fooddb_addition",
        "needs_alias", "needs_unit_rule", "needs_servings_fix", "needs_web_source",
        "priority_score", "priority_tier", "reason_for_priority",
    ])
    write_csv(RECIPE_SOURCE_BATCH2_OUT, source_candidates, [
        "ingredient_or_blocker", "affected_recipes_count", "affected_recipe_ids",
        "affected_recipe_names", "expected_recovered_recipes", "source_needed",
        "recommended_source_type", "recommended_action", "priority_score",
        "risk_notes", "why_it_matters",
    ])
    write_csv(FOOD_SOURCE_CANDIDATES_OUT, source_candidates, [
        "ingredient_or_blocker", "affected_recipes_count", "affected_recipe_ids",
        "affected_recipe_names", "expected_recovered_recipes", "source_needed",
        "recommended_source_type", "recommended_action", "priority_score",
        "risk_notes", "why_it_matters",
    ])

    tier_counts = Counter(row["priority_tier"] for row in prioritized_rows)
    fix_counts = Counter(row["proposed_fix_type"] for row in prioritized_rows)
    bucket_counts = Counter(row["target_bucket"] for row in prioritized_rows)
    top_blockers = candidate_groups[:15]

    summary_lines = [
        "Recipes_DB v1.2 Round43 repair queue priority summary",
        "",
        f"round42_repair_rows_reviewed={len(round42_rows)}",
        *[f"{tier}={tier_counts.get(tier, 0)}" for tier in ["batch2_high", "batch2_medium", "backlog", "reject"]],
        "",
        "Proposed fix type counts:",
        *[f"- {key or 'unknown'}: {value}" for key, value in fix_counts.most_common()],
        "",
        "Target bucket counts:",
        *[f"- {key or 'unknown'}: {value}" for key, value in bucket_counts.most_common()],
        "",
        "Top blockers:",
        *[f"- {row['ingredient_or_blocker']}: affected={row['affected_recipes_count']}, expected_recovered={row['expected_recovered_recipes']}, action={row['recommended_action']}, source_needed={row['source_needed']}, score={row['priority_score']}" for row in top_blockers],
        "",
        "Strict recommendation:",
        "- Do not repair all 113 rows at once.",
        "- First handle batch2_high recipes that need only alias/unit decisions.",
        "- For Food_DB/source work, verify only the top 10-20 blocker ingredients that affect multiple useful recipes.",
        "- Keep generic meat ambiguities and processed/obscure items out of automatic repair.",
    ]
    write_text(SUMMARY_OUT, summary_lines)

    plan_lines = [
        "Food_DB v1.2 source verification batch2 plan",
        "",
        "Scope:",
        "- 10-20 Food_DB/source items max.",
        "- No web fetching in this task.",
        "- No values should be invented; user must provide source-backed macros.",
        "",
        f"candidate_count={len(source_candidates)}",
        "",
        "Recommended candidates:",
    ]
    for row in source_candidates:
        plan_lines.append(
            f"- {row['ingredient_or_blocker']}: affected_recipes={row['affected_recipes_count']}, "
            f"expected_recovered={row['expected_recovered_recipes']}, source={row['recommended_source_type']}, "
            f"risk={row['risk_notes']}"
        )
    plan_lines.extend([
        "",
        "Recommended workflow:",
        "- Fill source_name, source_url, raw/cooked state, kcal/protein/carbs/fat per 100g.",
        "- Prefer CIQUAL/USDA/official nutrition databases; use manufacturer sources only for processed items.",
        "- Apply only rows that pass sanity checks in a later controlled batch.",
    ])
    write_text(FOOD_SOURCE_PLAN_OUT, plan_lines)
    print(f"Round43 repair prioritization complete: rows={len(prioritized_rows)} source_candidates={len(source_candidates)}")


if __name__ == "__main__":
    main()
