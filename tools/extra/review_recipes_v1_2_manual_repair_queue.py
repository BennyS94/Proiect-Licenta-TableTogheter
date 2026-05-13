from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RECIPES_DRAFT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft"
FOODDB_DRAFT_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round9.csv"
AUDIT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"

QUEUE_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_repair_queue.csv"
REVIEW_PATH = AUDIT_DIR / "recipes_v1_2_manual_repair_review.csv"
SUMMARY_PATH = AUDIT_DIR / "recipes_v1_2_manual_repair_review_summary.txt"


REVIEW_EXTRA_COLUMNS = [
    "repair_decision",
    "repair_safety",
    "repair_expected_impact",
    "repair_reason",
    "approved_fix_type",
    "approved_food_id",
    "approved_food_canonical_name",
    "approved_quantity_grams_estimated",
    "apply_repair",
]

LOCAL_SAFE_FOOD_FIXES = {
    "ham": ("food_cooked_ham_choice", "alias_mapping", "local cooked ham item exists"),
    "hard eggs": ("food_egg_hard_boiled", "alias_mapping", "hard-boiled egg item exists"),
    "eggland s best eggs": ("food_egg_raw", "alias_mapping", "brand egg normalized to egg raw"),
    "skim milk": ("food_milk_skimmed_pasteurised", "alias_mapping", "skimmed milk item exists"),
    "smoked salmon": ("food_salmon_smoked", "alias_mapping", "smoked salmon item exists"),
    "soy sauce": ("food_soy_sauce_prepacked", "alias_mapping", "soy sauce item exists"),
    "reduced sodium soy sauce": ("food_soy_sauce_prepacked", "alias_mapping", "macro-safe soy sauce fallback; sodium is out of generator scope"),
    "coconut oil": ("food_coconut_fat_or_oil", "alias_mapping", "coconut oil item exists"),
    "white pepper": ("food_white_pepper_powder", "alias_mapping", "white pepper item exists"),
    "cubed butternut squash": ("food_squash_butternut_pulp_raw", "alias_mapping", "butternut squash item exists"),
    "spicy brown mustard": ("food_mustard_with_grains", "alias_mapping", "grain mustard is closest local match"),
}

SAFE_UNIT_GRAMS = {
    "hard eggs": "600",
    "eggland s best eggs": "200",
    "heavy cream": "320",
    "coconut oil": "6.75",
    "white pepper": "0.29",
    "cubed butternut squash": "280",
    "spicy brown mustard": "15",
}

REVIEW_ONLY_LOCAL_FIXES = {
    "heavy cream": ("food_thick_cream_30_fat_refrigerated", "unit_rule_alias_mapping", "heavy cream mapped to local 30% thick cream; safe enough for draft but still a review approximation"),
}

DEFER_NEEDS_SOURCE = {
    "jalapeno pepper jack cheese",
    "swiss cheese",
    "cheddar cheese",
    "low fat sour cream",
    "pizza sauce",
    "canned refried beans",
    "refried beans",
    "romano cheese",
}

DEFER_TOO_RISKY = {
    "chicken thighs",
    "baby bok choy",
    "hot water",
    "green onions",
    "green onion",
    "seasoning salt",
    "red pepper",
}


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    queue_rows = read_csv(QUEUE_PATH)
    food_lookup = build_food_lookup(read_csv(FOODDB_DRAFT_PATH))

    review_rows = [review_row(row, food_lookup) for row in queue_rows]
    columns = list(queue_rows[0].keys()) + REVIEW_EXTRA_COLUMNS if queue_rows else REVIEW_EXTRA_COLUMNS
    write_csv(REVIEW_PATH, review_rows, columns)
    SUMMARY_PATH.write_text(build_summary(review_rows), encoding="utf-8")

    decision_counts = Counter(row["repair_decision"] for row in review_rows)
    print("Recipes_DB v1.2 manual repair review written")
    print(f"- reviewed rows: {len(review_rows)}")
    for decision, count in decision_counts.most_common():
        print(f"- {decision}: {count}")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_food_lookup(food_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {
        row.get("food_id", ""): row
        for row in food_rows
        if row.get("food_id", "")
    }


def review_row(row: dict[str, str], food_lookup: dict[str, dict[str, str]]) -> dict[str, str]:
    ingredient = normalized_ingredient(row)
    base = dict(row)

    if ingredient in LOCAL_SAFE_FOOD_FIXES:
        food_id, fix_type, reason = LOCAL_SAFE_FOOD_FIXES[ingredient]
        if food_id in food_lookup:
            grams = row.get("current_quantity_grams_estimated", "") or SAFE_UNIT_GRAMS.get(ingredient, "")
            return with_review(
                base,
                decision="approve_safe",
                safety="safe_auto",
                impact=expected_impact(row, ingredient),
                reason=f"{reason}; no web source needed because Food_DB draft already has {food_id}.",
                fix_type=fix_type,
                food_id=food_id,
                food_name=food_lookup[food_id].get("canonical_name", ""),
                grams=grams,
                apply_repair=True,
            )

    if ingredient in REVIEW_ONLY_LOCAL_FIXES:
        food_id, fix_type, reason = REVIEW_ONLY_LOCAL_FIXES[ingredient]
        if food_id in food_lookup:
            return with_review(
                base,
                decision="approve_with_review",
                safety="needs_review",
                impact="medium",
                reason=f"{reason}; unit grams are common-cup estimate, not source-specific.",
                fix_type=fix_type,
                food_id=food_id,
                food_name=food_lookup[food_id].get("canonical_name", ""),
                grams=SAFE_UNIT_GRAMS.get(ingredient, row.get("current_quantity_grams_estimated", "")),
                apply_repair=True,
            )

    if ingredient in DEFER_NEEDS_SOURCE or row.get("needs_fooddb_addition", "").lower() == "true":
        return with_review(
            base,
            decision="defer_needs_source",
            safety="needs_review",
            impact="medium",
            reason="Food_DB exact local item is missing or too approximate; needs verified source before application.",
            fix_type=row.get("proposed_fix_type", ""),
            food_id="",
            food_name="",
            grams=row.get("current_quantity_grams_estimated", ""),
            apply_repair=False,
        )

    if ingredient in DEFER_TOO_RISKY:
        return with_review(
            base,
            decision="defer_too_risky",
            safety="unsafe",
            impact="low",
            reason="Ingredient or unit decision is ambiguous; applying it would hide a modeling problem.",
            fix_type=row.get("proposed_fix_type", ""),
            food_id="",
            food_name="",
            grams=row.get("current_quantity_grams_estimated", ""),
            apply_repair=False,
        )

    if row.get("proposed_fix_type", "") == "servings_fix":
        return with_review(
            base,
            decision="reject",
            safety="unsafe",
            impact="low",
            reason="Servings cannot be changed safely from available local data; this would be macro-fitting, not repair.",
            fix_type="servings_fix",
            food_id="",
            food_name="",
            grams=row.get("current_quantity_grams_estimated", ""),
            apply_repair=False,
        )

    if row.get("proposed_fix_type", "") == "unit_rule":
        return with_review(
            base,
            decision="defer_too_risky",
            safety="needs_review",
            impact="low",
            reason="Unit rule is not in the safe Round36 allowlist.",
            fix_type="unit_rule",
            food_id="",
            food_name="",
            grams=row.get("current_quantity_grams_estimated", ""),
            apply_repair=False,
        )

    return with_review(
        base,
        decision="defer_too_risky",
        safety="needs_review",
        impact="low",
        reason="No safe local repair rule matched this row.",
        fix_type=row.get("proposed_fix_type", ""),
        food_id="",
        food_name="",
        grams=row.get("current_quantity_grams_estimated", ""),
        apply_repair=False,
    )


def with_review(
    row: dict[str, str],
    *,
    decision: str,
    safety: str,
    impact: str,
    reason: str,
    fix_type: str,
    food_id: str,
    food_name: str,
    grams: str,
    apply_repair: bool,
) -> dict[str, str]:
    updated = dict(row)
    updated.update(
        {
            "repair_decision": decision,
            "repair_safety": safety,
            "repair_expected_impact": impact,
            "repair_reason": reason,
            "approved_fix_type": fix_type,
            "approved_food_id": food_id,
            "approved_food_canonical_name": food_name,
            "approved_quantity_grams_estimated": grams,
            "apply_repair": "true" if apply_repair else "false",
        }
    )
    return updated


def expected_impact(row: dict[str, str], ingredient: str) -> str:
    if ingredient in {"ham", "hard eggs", "smoked salmon", "skim milk", "eggland s best eggs"}:
        return "high"
    if row.get("problem_type", "") in {"mapping_gap", "unit_or_mapping_gap"}:
        return "medium"
    return "low"


def normalized_ingredient(row: dict[str, str]) -> str:
    return (
        row.get("ingredient_name_normalized", "")
        or row.get("blocking_ingredient", "")
        or row.get("ingredient_raw_text", "")
    ).strip().lower()


def build_summary(rows: list[dict[str, str]]) -> str:
    decision_counts = Counter(row["repair_decision"] for row in rows)
    safety_counts = Counter(row["repair_safety"] for row in rows)
    impact_counts = Counter(row["repair_expected_impact"] for row in rows)
    fix_counts = Counter(row["approved_fix_type"] or "none" for row in rows if row["apply_repair"] == "true")
    applied = [row for row in rows if row["apply_repair"] == "true"]

    lines = [
        "Recipes_DB v1.2 manual repair review summary",
        "",
        f"- reviewed rows: {len(rows)}",
        f"- rows approved for draft application: {len(applied)}",
        "",
        "Repair decisions",
        *format_counter(decision_counts),
        "",
        "Repair safety",
        *format_counter(safety_counts),
        "",
        "Expected impact",
        *format_counter(impact_counts),
        "",
        "Applied fix types",
        *format_counter(fix_counts),
        "",
        "Applied repairs",
    ]
    for row in applied:
        lines.append(
            "- "
            f"{row['repair_id']} | {row['display_name']} | "
            f"{row['blocking_ingredient']} -> {row['approved_food_id'] or row['approved_fix_type']}"
        )
    lines.extend(
        [
            "",
            "Strict notes",
            "- Servings fixes were rejected unless a safe local ingredient/unit fix existed.",
            "- Food_DB additions were not created; only existing local Food_DB draft items were used.",
            "- Deferred rows need manual source review or a separate modeling decision.",
        ]
    )
    return "\n".join(lines) + "\n"


def format_counter(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key or 'blank'}: {value}" for key, value in counter.most_common()]


if __name__ == "__main__":
    main()
