from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
RECIPES_DRAFT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft"
AUDIT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
FOODDB_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round9.csv"

QUEUE_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_repair_queue.csv"
ROUND37_ADDITIONS_PATH = AUDIT_DIR / "recipes_v1_2_round37_manual_repair_queue_additions.csv"
ROUND37_UNIT_ROWS_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_round37_plus100_ingredients_unit_rules.csv"
ROUND37_MATCHES_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_round37_plus100_food_matches.csv"

OUT_REVIEW = AUDIT_DIR / "recipes_v1_2_round38_repair_review.csv"
OUT_SUMMARY = AUDIT_DIR / "recipes_v1_2_round38_repair_review_summary.txt"
OUT_BLOCKERS = AUDIT_DIR / "recipes_v1_2_round38_blocking_ingredients_summary.csv"
OUT_SOURCE_NEEDED = AUDIT_DIR / "recipes_v1_2_round38_manual_web_source_needed.csv"

REVIEW_COLUMNS = [
    "repair_decision",
    "repair_type",
    "repair_safety",
    "expected_impact",
    "blocking_reason",
    "approved_food_id",
    "approved_food_canonical_name",
    "approved_quantity_grams_estimated",
    "proposed_fix_detail_round38",
    "source_needed",
    "source_notes",
    "apply_repair",
]

SAFE_ALIAS_FIXES: dict[str, dict[str, str]] = {
    "green onion": {"food_id": "food_chive_or_spring_onion_fresh", "impact": "low"},
    "green onions": {"food_id": "food_chive_or_spring_onion_fresh", "impact": "low"},
    "quick cooking oats": {"food_id": "food_oat_raw", "impact": "high"},
    "steel cut oats": {"food_id": "food_oat_raw", "impact": "high"},
    "virgin olive oil": {"food_id": "food_olive_oil_extra_virgin", "impact": "medium"},
    "white sugar": {"food_id": "food_sugar_white", "impact": "medium"},
    "head cabbage": {"food_id": "food_white_cabbage_raw", "impact": "medium"},
    "sesame oil": {"food_id": "food_sesame_oil", "impact": "low"},
    "granny smith apple": {"food_id": "food_apple_var_granny_smith_pulp_and_skin_raw", "impact": "low"},
    "salted cod fish": {"food_id": "food_cod_atlantic_dried_and_salted", "impact": "high"},
    "white onions": {"food_id": "food_onion_raw", "impact": "medium"},
    "balsamic vinegar": {"food_id": "food_vinegar_balsamic", "impact": "low"},
    "garbanzo beans": {"food_id": "food_chick_pea_canned_drained", "impact": "high"},
    "firm tofu": {"food_id": "food_tofu_plain", "impact": "high"},
    "turmeric": {"food_id": "food_turmeric_powder", "impact": "low"},
    "freshly parmesan cheese": {"food_id": "food_parmesan_cheese_hard", "impact": "medium"},
    "whole milk ricotta": {"food_id": "food_ricotta_cheese", "impact": "high"},
    "beef chuck": {"food_id": "food_beef_chuck_raw", "impact": "high"},
    "sirloin tips": {"food_id": "food_beef_sirloin_steak_raw", "impact": "high"},
    "beef round steak": {"food_id": "food_beef_round_steak_raw", "impact": "high"},
    "chuck roast": {"food_id": "food_beef_chuck_raw", "impact": "high"},
}

REVIEW_ALIAS_FIXES: dict[str, dict[str, str]] = {
    "low fat milk": {"food_id": "food_milk_semi_skimmed_pasteurised", "impact": "medium"},
    "soft goat cheese": {"food_id": "food_cheese_from_goat_s_milk_fresh", "impact": "medium"},
    "marinara sauce": {"food_id": "food_tomato_sauce_with_onions_prepacked", "impact": "medium"},
    "plain low fat yogurt": {"food_id": "food_yogurt_fermented_milk_or_dairy_specialty_plain_0_fat_with_sugar_fortified_with_vitamin_d", "impact": "medium"},
    "porcini mushrooms": {"food_id": "food_cep_or_boletus_mushroom_raw", "impact": "medium"},
    "tender beef steak": {"food_id": "food_beef_steak_or_beef_steak_raw", "impact": "high"},
    "whole kernel corn": {"food_id": "food_sweet_corn_canned_drained", "impact": "medium"},
    "red wine vinegar": {"food_id": "food_vinegar", "impact": "low"},
    "julienne sun tomatoes": {"food_id": "food_tomato_dried", "impact": "medium"},
}

SAFE_UNIT_FIXES: dict[str, dict[str, str]] = {
    "cinnamon": {"food_id": "food_cinnamon_powder", "grams_per_teaspoon": "2.6", "impact": "low"},
}

REJECT_INGREDIENTS = {
    "and cubed",
    "carrots and",
}

DEFER_TOO_RISKY = {
    "avocado",
    "vegetable oil for frying",
    "beef",
    "chicken thighs with skin",
    "piece ginger",
    "ginger root",
    "olive oil cooking spray",
    "cooking spray",
    "cubed lamb meat",
    "meaty ham bone",
    "onion salt",
}

DEFER_NEEDS_SOURCE = {
    "oat milk",
    "plain or vanilla soy milk",
    "sour cream",
    "refrigerated pie pastry",
    "milk or unsweetened plant milk of your choice",
    "baked beans",
    "sottocenere",
    "rosemary",
    "pinto beans",
    "great northern beans",
    "guanciale",
    "dry pinto beans",
    "low fat vegetable broth",
    "biscuit baking mix",
}


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    repair_rows = round37_repair_rows()
    food_lookup = build_food_lookup(read_csv(FOODDB_PATH))
    unit_by_key = rows_by_key(read_csv(ROUND37_UNIT_ROWS_PATH))
    match_by_key = rows_by_key(read_csv(ROUND37_MATCHES_PATH))

    reviewed = [
        review_row(row, food_lookup, unit_by_key, match_by_key)
        for row in repair_rows
    ]
    columns = list(repair_rows[0].keys()) + REVIEW_COLUMNS if repair_rows else REVIEW_COLUMNS
    write_csv(OUT_REVIEW, reviewed, columns)
    write_csv(OUT_BLOCKERS, blocking_summary_rows(reviewed), blocking_summary_columns())
    write_csv(OUT_SOURCE_NEEDED, source_needed_rows(reviewed), source_needed_columns())
    OUT_SUMMARY.write_text(build_summary(reviewed), encoding="utf-8")

    decisions = Counter(row["repair_decision"] for row in reviewed)
    print("Round38 repair review written")
    print(f"- reviewed rows: {len(reviewed)}")
    for decision, count in decisions.most_common():
        print(f"- {decision}: {count}")


def round37_repair_rows() -> list[dict[str, str]]:
    rows = read_csv(ROUND37_ADDITIONS_PATH)
    if rows:
        return rows
    queue_rows = read_csv(QUEUE_PATH)
    return [
        row for row in queue_rows
        if row.get("created_from_round") == "round37"
        or row.get("recipe_id_candidate", "").startswith("recipes_v1_2_round37_plus100_")
    ]


def review_row(
    row: dict[str, str],
    food_lookup: dict[str, dict[str, str]],
    unit_by_key: dict[tuple[str, str], dict[str, str]],
    match_by_key: dict[tuple[str, str], dict[str, str]],
) -> dict[str, str]:
    ingredient = normalized_ingredient(row)
    key = (row.get("recipe_id_candidate", ""), ingredient)
    unit_row = unit_by_key.get(key, {})
    match_row = match_by_key.get(key, {})

    if ingredient in REJECT_INGREDIENTS:
        return with_decision(
            row,
            decision="reject",
            repair_type="recipe_reject",
            safety="unsafe",
            impact="low",
            reason="Ingredientul blocant este un artefact de parsare, nu un aliment reparabil sigur.",
            food_id="",
            food_name="",
            grams=row.get("current_quantity_grams_estimated", ""),
            source_needed="false",
            source_notes="",
            apply_repair=False,
        )

    if ingredient in SAFE_ALIAS_FIXES:
        return alias_decision(
            row=row,
            food_lookup=food_lookup,
            fix=SAFE_ALIAS_FIXES[ingredient],
            decision="approve_safe",
            safety="safe_auto",
            repair_type="alias_mapping",
            reason="Alias exact sau foarte apropiat catre un rand Food_DB local existent.",
            apply_repair=True,
        )

    if ingredient in REVIEW_ALIAS_FIXES:
        return alias_decision(
            row=row,
            food_lookup=food_lookup,
            fix=REVIEW_ALIAS_FIXES[ingredient],
            decision="approve_with_review",
            safety="needs_review",
            repair_type="alias_mapping",
            reason="Alias local plauzibil, dar nu perfect exact; aplicat doar ca draft/audit.",
            apply_repair=True,
        )

    if ingredient in SAFE_UNIT_FIXES:
        fix = SAFE_UNIT_FIXES[ingredient]
        grams = grams_for_unit_fix(unit_row, fix)
        food_id = fix["food_id"]
        if grams and food_id in food_lookup:
            return with_decision(
                row,
                decision="approve_safe",
                repair_type="unit_rule",
                safety="safe_auto",
                impact=fix["impact"],
                reason="Regula de unitate comuna si aliment local exact.",
                food_id=food_id,
                food_name=food_lookup[food_id].get("canonical_name", ""),
                grams=grams,
                source_needed="false",
                source_notes="",
                apply_repair=True,
            )

    if ingredient in DEFER_TOO_RISKY or row.get("proposed_fix_type") == "servings_fix":
        return with_decision(
            row,
            decision="defer_too_risky",
            repair_type=row.get("proposed_fix_type", "") or "keep_for_manual_web_source",
            safety="unsafe",
            impact="low",
            reason="Reparatia ar presupune unitate, yield sau aliment generic prea ambiguu pentru aplicare automata.",
            food_id=match_row.get("mapped_food_id", ""),
            food_name=match_row.get("mapped_food_canonical_name", ""),
            grams=row.get("current_quantity_grams_estimated", ""),
            source_needed="true",
            source_notes="Necesita decizie manuala de modelare sau sursa verificata.",
            apply_repair=False,
        )

    if ingredient in DEFER_NEEDS_SOURCE or row.get("needs_fooddb_addition", "").lower() == "true":
        return with_decision(
            row,
            decision="defer_needs_source",
            repair_type=row.get("proposed_fix_type", "") or "keep_for_manual_web_source",
            safety="needs_review",
            impact="medium",
            reason="Nu exista o potrivire locala suficient de exacta in Food_DB draft.",
            food_id="",
            food_name="",
            grams=row.get("current_quantity_grams_estimated", ""),
            source_needed="true",
            source_notes="Necesita sursa nutritionala locala/verificata sau regula manuala explicita.",
            apply_repair=False,
        )

    return with_decision(
        row,
        decision="defer_needs_source",
        repair_type=row.get("proposed_fix_type", "") or "keep_for_manual_web_source",
        safety="needs_review",
        impact="low",
        reason="Nu exista regula Round38 sigura pentru acest blocaj.",
        food_id="",
        food_name="",
        grams=row.get("current_quantity_grams_estimated", ""),
        source_needed="true",
        source_notes="Necesita review manual separat.",
        apply_repair=False,
    )


def alias_decision(
    *,
    row: dict[str, str],
    food_lookup: dict[str, dict[str, str]],
    fix: dict[str, str],
    decision: str,
    safety: str,
    repair_type: str,
    reason: str,
    apply_repair: bool,
) -> dict[str, str]:
    food_id = fix["food_id"]
    if food_id not in food_lookup:
        return with_decision(
            row,
            decision="defer_needs_source",
            repair_type="fooddb_addition",
            safety="needs_review",
            impact=fix.get("impact", "medium"),
            reason=f"Food_DB local nu contine {food_id}; nu se adauga valori nutritionale in Round38.",
            food_id="",
            food_name="",
            grams=row.get("current_quantity_grams_estimated", ""),
            source_needed="true",
            source_notes="Necesita Food_DB addition cu sursa verificata.",
            apply_repair=False,
        )
    return with_decision(
        row,
        decision=decision,
        repair_type=repair_type,
        safety=safety,
        impact=fix.get("impact", "medium"),
        reason=reason,
        food_id=food_id,
        food_name=food_lookup[food_id].get("canonical_name", ""),
        grams=row.get("current_quantity_grams_estimated", ""),
        source_needed="false",
        source_notes="Sursa nutritionala este rand Food_DB local existent.",
        apply_repair=apply_repair,
    )


def with_decision(
    row: dict[str, str],
    *,
    decision: str,
    repair_type: str,
    safety: str,
    impact: str,
    reason: str,
    food_id: str,
    food_name: str,
    grams: str,
    source_needed: str,
    source_notes: str,
    apply_repair: bool,
) -> dict[str, str]:
    updated = dict(row)
    updated.update(
        {
            "repair_decision": decision,
            "repair_type": repair_type,
            "repair_safety": safety,
            "expected_impact": impact,
            "blocking_reason": reason,
            "approved_food_id": food_id,
            "approved_food_canonical_name": food_name,
            "approved_quantity_grams_estimated": grams,
            "proposed_fix_detail_round38": build_fix_detail(repair_type, food_id, grams),
            "source_needed": source_needed,
            "source_notes": source_notes,
            "apply_repair": "true" if apply_repair else "false",
        }
    )
    return updated


def build_fix_detail(repair_type: str, food_id: str, grams: str) -> str:
    parts = [repair_type]
    if food_id:
        parts.append(f"food_id={food_id}")
    if grams:
        parts.append(f"grams={grams}")
    return "; ".join(parts)


def grams_for_unit_fix(unit_row: dict[str, str], fix: dict[str, str]) -> str:
    quantity = to_float(unit_row.get("quantity_value"))
    unit = (unit_row.get("quantity_unit") or "").strip().lower()
    if quantity is None:
        return ""
    if unit in {"teaspoon", "teaspoons", "tsp"} and fix.get("grams_per_teaspoon"):
        return format_number(quantity * float(fix["grams_per_teaspoon"]))
    if unit in {"tablespoon", "tablespoons", "tbsp"} and fix.get("grams_per_teaspoon"):
        return format_number(quantity * float(fix["grams_per_teaspoon"]) * 3)
    return ""


def blocking_summary_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    grouped: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        grouped[normalized_ingredient(row)].append(row)
    output = []
    for ingredient, group in sorted(grouped.items()):
        grams_total = sum(to_float(row.get("current_quantity_grams_estimated")) or 0.0 for row in group)
        safe_now = all(row.get("apply_repair") == "true" for row in group)
        needs_source = any(row.get("source_needed") == "true" for row in group)
        output.append(
            {
                "ingredient_name_normalized": ingredient,
                "affected_recipe_count": str(len({row.get("recipe_id_candidate", "") for row in group})),
                "affected_rows": str(len(group)),
                "total_grams_affected": format_number(grams_total),
                "proposed_action": dominant_action(group),
                "safe_to_apply_now": str(safe_now).lower(),
                "needs_web_source": str(needs_source).lower(),
            }
        )
    return output


def source_needed_rows(rows: list[dict[str, str]]) -> list[dict[str, str]]:
    output = []
    for row in rows:
        if row.get("source_needed") != "true":
            continue
        ingredient = normalized_ingredient(row)
        output.append(
            {
                "recipe_id_candidate": row.get("recipe_id_candidate", ""),
                "display_name": row.get("display_name", ""),
                "blocking_ingredient": row.get("blocking_ingredient", ""),
                "needed_fooddb_item": ingredient if row.get("repair_type") in {"alias_mapping", "fooddb_addition"} else "",
                "needed_unit_rule": ingredient if row.get("repair_type") == "unit_rule" else "",
                "needed_servings_decision": "true" if row.get("repair_type") == "servings_fix" else "false",
                "suggested_source_type": suggested_source_type(row),
                "reason": row.get("blocking_reason", ""),
            }
        )
    return output


def suggested_source_type(row: dict[str, str]) -> str:
    ingredient = normalized_ingredient(row)
    if ingredient in {"beef", "cubed lamb meat", "chicken thighs with skin", "meaty ham bone"}:
        return "manual_curated"
    if row.get("repair_type") == "fooddb_addition":
        return "ciqual"
    if row.get("source_needed") == "true":
        return "ciqual"
    return "manual_curated"


def dominant_action(group: list[dict[str, str]]) -> str:
    decisions = Counter(row.get("repair_decision", "") for row in group)
    decision = decisions.most_common(1)[0][0] if decisions else ""
    if decision in {"approve_safe", "approve_with_review"}:
        return "apply_safe_repair"
    if decision == "defer_needs_source":
        return "manual_source_review"
    if decision == "reject":
        return "reject_or_reparse"
    return "manual_modeling_review"


def blocking_summary_columns() -> list[str]:
    return [
        "ingredient_name_normalized",
        "affected_recipe_count",
        "affected_rows",
        "total_grams_affected",
        "proposed_action",
        "safe_to_apply_now",
        "needs_web_source",
    ]


def source_needed_columns() -> list[str]:
    return [
        "recipe_id_candidate",
        "display_name",
        "blocking_ingredient",
        "needed_fooddb_item",
        "needed_unit_rule",
        "needed_servings_decision",
        "suggested_source_type",
        "reason",
    ]


def build_summary(rows: list[dict[str, str]]) -> str:
    decision_counts = Counter(row["repair_decision"] for row in rows)
    safety_counts = Counter(row["repair_safety"] for row in rows)
    repair_counts = Counter(row["repair_type"] for row in rows)
    impact_counts = Counter(row["expected_impact"] for row in rows)
    applied = [row for row in rows if row["apply_repair"] == "true"]
    deferred = [row for row in rows if row["apply_repair"] != "true"]
    lines = [
        "Recipes_DB v1.2 Round38 repair review summary",
        "",
        f"- reviewed rows: {len(rows)}",
        f"- approved/applicable rows: {len(applied)}",
        f"- deferred/rejected rows: {len(deferred)}",
        "",
        "Repair decisions",
        *format_counter(decision_counts),
        "",
        "Repair safety",
        *format_counter(safety_counts),
        "",
        "Repair types",
        *format_counter(repair_counts),
        "",
        "Expected impact",
        *format_counter(impact_counts),
        "",
        "Applied repairs",
    ]
    for row in applied:
        lines.append(
            "- "
            f"{row['repair_id']} | {row['display_name']} | "
            f"{row['blocking_ingredient']} -> {row['approved_food_id'] or row['repair_type']}"
        )
    lines.extend(["", "Deferred/rejected top rows"])
    for row in deferred[:30]:
        lines.append(
            "- "
            f"{row['repair_id']} | {row['display_name']} | "
            f"{row['blocking_ingredient']} | {row['repair_decision']} | {row['blocking_reason']}"
        )
    lines.extend(
        [
            "",
            "Strict notes",
            "- Generic beef/pork/turkey/lamb repairs were not applied automatically.",
            "- No Food_DB rows were created and no web data was fetched.",
            "- Approved repairs map only to existing local Food_DB draft items or narrow unit rules.",
        ]
    )
    return "\n".join(lines) + "\n"


def rows_by_key(rows: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    output = {}
    for row in rows:
        key = (row.get("recipe_id_candidate", ""), normalized_ingredient(row))
        output.setdefault(key, row)
    return output


def build_food_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row.get("food_id", ""): row for row in rows if row.get("food_id")}


def normalized_ingredient(row: dict[str, Any]) -> str:
    return (
        row.get("ingredient_name_normalized")
        or row.get("blocking_ingredient")
        or row.get("ingredient_raw_text")
        or ""
    ).strip().lower()


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def to_float(value: object) -> float | None:
    try:
        text = str(value or "").strip()
        if not text:
            return None
        return float(text)
    except ValueError:
        return None


def format_number(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".")


def format_counter(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key or 'blank'}: {value}" for key, value in counter.most_common()]


if __name__ == "__main__":
    main()
