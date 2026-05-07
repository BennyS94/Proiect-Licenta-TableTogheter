from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_PARSED_INGREDIENTS = (
    REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_ingredients_parsed_unit_rules.csv"
)
DEFAULT_MAPPING = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "draft"
    / "recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_round3_targeted_blockers.csv"
)
DEFAULT_CONTRIBUTIONS = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "audit"
    / "recipes_v1_1_ingredient_nutrition_contributions_round3.csv"
)
DEFAULT_FOODDB = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round3.csv"

OUT_AUDIT = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round4_decision_blockers.csv"
OUT_SUMMARY = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round4_decision_blockers_summary.txt"
)

AUDIT_COLUMNS = [
    "ingredient_name_normalized",
    "total_grams_affected",
    "affected_recipe_count",
    "example_raw_texts",
    "example_recipes",
    "current_mapping_status",
    "possible_fooddb_match",
    "possible_fooddb_id",
    "source_macro_available",
    "needs_edible_yield_rule",
    "needs_cooked_raw_state_decision",
    "suggested_action",
    "safety",
    "recommended_decision",
    "risk_notes",
]

MACRO_COLUMNS = [
    "energy_kcal_100g",
    "protein_g_100g",
    "carbs_g_100g",
    "fat_g_100g",
]

PREFERRED_FOOD_IDS = {
    "potato_raw": "food_potato_peeled_raw",
    "potato_cooked": "food_potato_boiled_cooked_in_water",
    "red_potato_possible": "food_new_potato_raw",
    "rice_raw": "food_rice_raw",
    "rice_cooked": "food_rice_cooked_unsalted",
    "jasmine_rice_cooked": "food_rice_thai_cooked",
    "basmati_rice_raw": "food_basmati_rice_raw",
    "basmati_rice_cooked": "food_rice_basmati_cooked",
    "brown_rice_raw": "food_rice_brown_raw",
    "brown_rice_cooked": "food_rice_brown_cooked_unsalted",
    "wild_rice_raw": "food_wild_rice_raw",
    "wild_rice_cooked": "food_wild_rice_cooked_unsalted",
    "pasta_raw": "food_dried_pasta_raw",
    "pasta_cooked": "food_dried_pasta_cooked_unsalted",
    "beef_ground": "food_beef_minced_steak_15_fat_raw",
    "beef_ground_lean": "food_beef_minced_steak_10_fat_raw",
    "beef_stew": "food_beef_stewing_meat_raw",
    "beef_chuck": "food_beef_chuck_raw",
    "beef_flank": "food_beef_flank_steak_raw",
    "beef_short_ribs": "food_beef_short_ribs_raw",
    "pork_shoulder": "food_pork_shoulder_raw",
    "pork_loin": "food_pork_loin_raw",
    "pork_chop": "food_pork_chop_raw",
    "turkey_generic": "food_turkey_meat_raw",
    "turkey_breast_possible": "food_turkey_escalope_raw",
    "chicken_leg_meat": "food_chicken_leg_meat_raw",
    "chicken_leg_meat_skin": "food_chicken_leg_meat_and_skin_raw",
    "chicken_meat": "food_chicken_meat_raw",
    "chicken_broth": "food_chicken_broth_ready_to_serve",
    "mozzarella": "food_mozzarella_cheese_from_cow_s_milk",
    "shrimp_raw": "food_shrimp_or_prawn_raw",
    "water": "food_water_municipal",
    "bread": "food_bread_french_bread_baguette",
}

CARB_KEYWORDS = {
    "rice",
    "potato",
    "potatoes",
    "pasta",
    "bread",
    "flour",
    "noodle",
    "noodles",
    "couscous",
    "quinoa",
    "barley",
    "oats",
    "tortilla",
    "bean",
    "beans",
    "lentil",
    "lentils",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Lipseste fisierul asteptat: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def clean_text(value: object) -> str:
    return str(value or "").strip()


def normalize_text(value: object) -> str:
    text = clean_text(value).casefold()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_float(value: object) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def format_number(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}".rstrip("0").rstrip(".")


def clipped(values: list[str], limit: int = 5) -> str:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        text = clean_text(value)
        if not text or text in seen:
            continue
        output.append(text)
        seen.add(text)
        if len(output) >= limit:
            break
    return " | ".join(output)


def truth(value: bool) -> str:
    return "true" if value else "false"


def build_fooddb_lookup(food_rows: list[dict[str, str]]) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
    by_id: dict[str, dict[str, str]] = {}
    by_name: dict[str, dict[str, str]] = {}
    for row in food_rows:
        food_id = clean_text(row.get("food_id"))
        if food_id:
            by_id[food_id] = row
        for column in ("canonical_name", "display_name"):
            name = normalize_text(clean_text(row.get(column)).replace("_", " "))
            if name and name not in by_name:
                by_name[name] = row
    return by_id, by_name


def has_complete_macros(food_row: dict[str, str] | None) -> bool:
    if not food_row:
        return False
    return all(parse_float(food_row.get(column)) is not None for column in MACRO_COLUMNS)


def food_label(food_row: dict[str, str] | None) -> str:
    if not food_row:
        return ""
    canonical = clean_text(food_row.get("canonical_name"))
    return canonical or clean_text(food_row.get("display_name"))


def preferred_food(food_by_id: dict[str, dict[str, str]], key: str) -> tuple[str, str, bool]:
    food_id = PREFERRED_FOOD_IDS.get(key, "")
    food_row = food_by_id.get(food_id)
    return food_label(food_row), food_id if food_row else "", has_complete_macros(food_row)


def exact_food_match(
    ingredient_name: str,
    food_by_name: dict[str, dict[str, str]],
) -> tuple[str, str, bool]:
    food_row = food_by_name.get(normalize_text(ingredient_name))
    if not food_row:
        return "", "", False
    return food_label(food_row), clean_text(food_row.get("food_id")), has_complete_macros(food_row)


def has_any(value: str, words: set[str]) -> bool:
    tokens = set(value.split())
    return bool(tokens & words)


def classify_decision(
    ingredient_name: str,
    raw_texts: list[str],
    food_by_id: dict[str, dict[str, str]],
    food_by_name: dict[str, dict[str, str]],
) -> dict[str, object]:
    raw_blob = normalize_text(" ".join(raw_texts))
    name = normalize_text(ingredient_name)
    possible_match = ""
    possible_food_id = ""
    source_macro_available = False
    needs_edible_yield_rule = False
    needs_cooked_raw_state_decision = False
    suggested_action = "keep_review"
    safety = "needs_review"
    recommended_decision = "review manually before round4"
    risk_notes: list[str] = []

    exact_match, exact_food_id, exact_has_macros = exact_food_match(name, food_by_name)

    if name == "red potatoes":
        possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "red_potato_possible")
        if not possible_food_id:
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "potato_raw")
        needs_cooked_raw_state_decision = True
        suggested_action = "keep_review"
        recommended_decision = "manual approval needed: map red potatoes to generic/new potato only by raw/cooked context"
        risk_notes.append("exact red potato item is not canonicalized; raw/cooked state changes macros")
    elif name == "chicken thighs":
        possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "chicken_leg_meat_skin")
        if not possible_food_id:
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "chicken_leg_meat")
        needs_edible_yield_rule = True
        suggested_action = "add_edible_yield_rule"
        recommended_decision = "manual approval needed: choose thigh meat/skin basis and edible-yield rule"
        risk_notes.append("raw grams may include bone and skin; exact chicken thigh source was previously suspect")
    elif name in {"bone in chicken pieces", "cut up chicken parts"}:
        possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "chicken_meat")
        needs_edible_yield_rule = True
        suggested_action = "add_edible_yield_rule"
        safety = "needs_review"
        recommended_decision = "defer until edible-yield rule exists; do not map to chicken breast"
        risk_notes.append("mixed bone-in parts need edible yield and cut composition decision")
    elif name in {"ground turkey", "turkey"}:
        possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "turkey_generic")
        suggested_action = "keep_review"
        if name == "ground turkey":
            recommended_decision = "add exact ground turkey item if local source macro is approved"
            risk_notes.append("generic turkey meat is not exact for ground turkey fat level")
        else:
            recommended_decision = "keep generic turkey in review unless row text has exact turkey breast/leg state"
            risk_notes.append("generic turkey is too broad for global auto mapping")
    elif name == "generic beef" or name == "beef":
        if "ground beef" in raw_blob:
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "beef_ground")
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe row-level promotion only for raw text that explicitly says ground beef"
            risk_notes.append("do not map all generic beef rows globally")
        elif "stew" in raw_blob:
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "beef_stew")
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe row-level promotion only for explicit stew beef text"
            risk_notes.append("generic beef rows remain review")
        else:
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "beef_chuck")
            suggested_action = "keep_review"
            recommended_decision = "keep generic beef in review; promote only exact cut/context rows"
            risk_notes.append("global generic beef mapping would distort cut/fat assumptions")
    elif name in {"beef stew meat", "cubed beef stew meat"}:
        possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "beef_stew")
        suggested_action = "safe_promote"
        safety = "safe_auto"
        recommended_decision = "safe row-level promotion to beef stewing meat when raw text is exact"
    elif name in {"beef round steak", "beef top sirloin"}:
        possible_match = "specific beef cut exists nearby, but exact cut/fat may differ"
        suggested_action = "keep_review"
        recommended_decision = "manual cut decision before promotion"
        risk_notes.append("do not collapse specific beef cuts into generic beef automatically")
    elif name == "generic pork" or name == "pork":
        if "pork shoulder" in raw_blob:
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "pork_shoulder")
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe row-level promotion only for explicit pork shoulder text"
        elif "pork loin" in raw_blob:
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "pork_loin")
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe row-level promotion only for explicit pork loin text"
        else:
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "pork_chop")
            suggested_action = "keep_review"
            recommended_decision = "keep generic pork in review unless exact cut is present"
            risk_notes.append("generic pork is too broad for automatic fat/cut choice")
    elif name in {"potatoes", "potato"}:
        if any(word in raw_blob for word in ("boiled", "cooked", "roasted", "baked")):
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "potato_cooked")
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe row-level promotion when cooked state is explicit"
        elif any(word in raw_blob for word in ("raw", "peeled", "uncooked")):
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "potato_raw")
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe row-level promotion when raw state is explicit"
        else:
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "potato_raw")
            needs_cooked_raw_state_decision = True
            suggested_action = "keep_review"
            recommended_decision = "manual raw/cooked state decision before promotion"
    elif "rice" in name:
        if any(word in raw_blob for word in ("vinegar", "flour", "noodle", "noodles", "wine")):
            possible_match = "not a rice grain macro row"
            suggested_action = "keep_unmapped"
            safety = "unsafe"
            recommended_decision = "do not map rice-flour/noodle/vinegar/wine contexts to plain rice"
        elif any(word in raw_blob for word in ("uncooked", "raw", "dry")):
            if "brown" in name:
                key = "brown_rice_raw"
            elif "wild" in name:
                key = "wild_rice_raw"
            elif "jasmine" in name:
                key = "rice_raw"
            elif "basmati" in name:
                key = "basmati_rice_raw"
            else:
                key = "rice_raw"
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, key)
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe row-level promotion because raw/dry state is explicit"
        elif "cooked" in raw_blob:
            if "brown" in name:
                key = "brown_rice_cooked"
            elif "wild" in name:
                key = "wild_rice_cooked"
            elif "jasmine" in name:
                key = "jasmine_rice_cooked"
            elif "basmati" in name:
                key = "basmati_rice_cooked"
            else:
                key = "rice_cooked"
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, key)
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe row-level promotion because cooked state is explicit"
        else:
            if "brown" in name:
                key = "brown_rice_raw"
            elif "wild" in name:
                key = "wild_rice_raw"
            elif "basmati" in name:
                key = "basmati_rice_raw"
            else:
                key = "rice_raw"
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, key)
            needs_cooked_raw_state_decision = True
            suggested_action = "keep_review"
            recommended_decision = "keep generic rice in review until cooked/raw state is clear"
    elif "pasta" in name:
        if "cooked" in raw_blob:
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "pasta_cooked")
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe row-level promotion if plain cooked pasta is intended"
        elif any(word in raw_blob for word in ("uncooked", "raw", "dry", "dried")):
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "pasta_raw")
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe row-level promotion if dry pasta is explicit"
        else:
            possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "pasta_raw")
            needs_cooked_raw_state_decision = True
            suggested_action = "keep_review"
            recommended_decision = "keep pasta variants in review unless dry/cooked state and type are clear"
    elif name == "chicken broth":
        possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "chicken_broth")
        if possible_food_id:
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe low-macro promotion to chicken broth item"
        else:
            suggested_action = "add_fooddb_item"
            recommended_decision = "add chicken broth only from sane local macro source"
    elif name == "water":
        possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "water")
        if possible_food_id:
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "optional low-macro promotion; improves weight coverage but not calories/protein"
        else:
            suggested_action = "keep_review"
            recommended_decision = "ignore for macro recovery if no exact water item exists"
    elif name in {"uncooked shrimp", "shrimp"}:
        possible_match, possible_food_id, source_macro_available = preferred_food(food_by_id, "shrimp_raw")
        if possible_food_id:
            suggested_action = "safe_promote"
            safety = "safe_auto"
            recommended_decision = "safe row-level promotion to raw shrimp/prawn when text is exact"
        else:
            suggested_action = "keep_review"
            recommended_decision = "add/review shrimp item before promotion"
    elif exact_food_id and exact_has_macros and has_any(name, CARB_KEYWORDS):
        possible_match = exact_match
        possible_food_id = exact_food_id
        source_macro_available = True
        suggested_action = "safe_promote"
        safety = "safe_auto"
        recommended_decision = "safe exact Food_DB match, but keep row-level scope"
    elif exact_food_id and exact_has_macros:
        possible_match = exact_match
        possible_food_id = exact_food_id
        source_macro_available = True
        suggested_action = "keep_review"
        recommended_decision = "exact source exists, but not targeted as macro blocker in this pass"
    else:
        possible_match = "no exact safe Food_DB match found in round3 draft"
        suggested_action = "keep_review"
        recommended_decision = "defer or handle in separate targeted Food_DB/mapping decision"

    if not source_macro_available and possible_food_id:
        source_macro_available = has_complete_macros(food_by_id.get(possible_food_id))

    return {
        "possible_fooddb_match": possible_match,
        "possible_fooddb_id": possible_food_id,
        "source_macro_available": truth(source_macro_available),
        "needs_edible_yield_rule": truth(needs_edible_yield_rule),
        "needs_cooked_raw_state_decision": truth(needs_cooked_raw_state_decision),
        "suggested_action": suggested_action,
        "safety": safety,
        "recommended_decision": recommended_decision,
        "risk_notes": "; ".join(risk_notes),
    }


def build_audit(
    mapping_rows: list[dict[str, str]],
    contribution_rows: list[dict[str, str]],
    food_rows: list[dict[str, str]],
) -> list[dict[str, object]]:
    food_by_id, food_by_name = build_fooddb_lookup(food_rows)
    mapping_by_key = {
        (clean_text(row.get("recipe_id_candidate")), clean_text(row.get("ingredient_position"))): row
        for row in mapping_rows
    }
    grouped: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "total_grams": 0.0,
            "recipe_ids": set(),
            "raw_texts": [],
            "recipes": [],
            "statuses": Counter(),
        }
    )

    for contribution in contribution_rows:
        grams = parse_float(contribution.get("quantity_grams_estimated"))
        if grams is None or grams <= 0 or clean_text(contribution.get("contribution_status")) == "used":
            continue
        key = (
            clean_text(contribution.get("recipe_id_candidate")),
            clean_text(contribution.get("ingredient_position")),
        )
        mapping = mapping_by_key.get(key, {})
        mapping_status = clean_text(mapping.get("mapping_status")) or clean_text(
            contribution.get("contribution_status")
        )
        if mapping_status == "accepted_auto":
            continue

        ingredient_name = clean_text(contribution.get("ingredient_name_normalized"))
        if not ingredient_name:
            ingredient_name = clean_text(mapping.get("ingredient_name_normalized"))
        item = grouped[ingredient_name]
        item["total_grams"] = float(item["total_grams"]) + grams
        item["recipe_ids"].add(clean_text(contribution.get("recipe_id_candidate")))
        item["raw_texts"].append(clean_text(contribution.get("ingredient_raw_text")))
        item["recipes"].append(clean_text(contribution.get("display_name")))
        item["statuses"][mapping_status] += 1

    rows: list[dict[str, object]] = []
    for ingredient_name, item in grouped.items():
        decision = classify_decision(
            ingredient_name=ingredient_name,
            raw_texts=list(item["raw_texts"]),
            food_by_id=food_by_id,
            food_by_name=food_by_name,
        )
        status_text = "; ".join(f"{status}:{count}" for status, count in item["statuses"].most_common())
        rows.append(
            {
                "ingredient_name_normalized": ingredient_name,
                "total_grams_affected": format_number(float(item["total_grams"])),
                "affected_recipe_count": len(item["recipe_ids"]),
                "example_raw_texts": clipped(list(item["raw_texts"])),
                "example_recipes": clipped(list(item["recipes"])),
                "current_mapping_status": status_text,
                **decision,
            }
        )

    return sorted(
        rows,
        key=lambda row: (
            -(parse_float(row.get("total_grams_affected")) or 0.0),
            clean_text(row.get("ingredient_name_normalized")),
        ),
    )


def total_grams(rows: list[dict[str, object]], *, safety: str | None = None, action: str | None = None) -> float:
    total = 0.0
    for row in rows:
        if safety is not None and clean_text(row.get("safety")) != safety:
            continue
        if action is not None and clean_text(row.get("suggested_action")) != action:
            continue
        total += parse_float(row.get("total_grams_affected")) or 0.0
    return total


def row_line(row: dict[str, object]) -> str:
    return (
        f"- {row['ingredient_name_normalized']}: {row['total_grams_affected']}g | "
        f"recipes={row['affected_recipe_count']} | action={row['suggested_action']} | "
        f"safety={row['safety']} | food_id={row['possible_fooddb_id']}"
    )


def write_summary(path: Path, rows: list[dict[str, object]]) -> None:
    safety_counts = Counter(clean_text(row.get("safety")) for row in rows)
    action_counts = Counter(clean_text(row.get("suggested_action")) for row in rows)
    safe_rows = [row for row in rows if clean_text(row.get("safety")) == "safe_auto"]
    review_rows = [row for row in rows if clean_text(row.get("safety")) == "needs_review"]
    unsafe_rows = [row for row in rows if clean_text(row.get("safety")) == "unsafe"]
    edible_rows = [row for row in rows if clean_text(row.get("needs_edible_yield_rule")) == "true"]
    state_rows = [row for row in rows if clean_text(row.get("needs_cooked_raw_state_decision")) == "true"]
    carb_rows = [
        row
        for row in rows
        if has_any(normalize_text(row.get("ingredient_name_normalized")), CARB_KEYWORDS)
    ]

    lines: list[str] = []
    lines.append("Recipes_DB v1.1 round4 decision blockers audit")
    lines.append("=" * 56)
    lines.append("")
    lines.append(f"Total blockers with grams: {len(rows)}")
    lines.append(f"Expected grams recoverable by safe_auto rows: {format_number(total_grams(safe_rows))}g")
    lines.append(f"Grams needing manual review: {format_number(total_grams(review_rows))}g")
    lines.append(f"Grams marked unsafe/deferred: {format_number(total_grams(unsafe_rows))}g")
    lines.append("")
    lines.append("Safety counts:")
    for safety, count in safety_counts.most_common():
        lines.append(f"- {safety}: {count}")
    lines.append("")
    lines.append("Suggested action counts:")
    for action, count in action_counts.most_common():
        lines.append(f"- {action}: {count}")
    lines.append("")
    lines.append("Safe enough for round4, if row-level rules stay strict:")
    if safe_rows:
        for row in safe_rows[:30]:
            lines.append(row_line(row))
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Needs manual approval / decision:")
    for row in review_rows[:30]:
        lines.append(row_line(row))
    lines.append("")
    lines.append("Unsafe or should stay deferred:")
    if unsafe_rows:
        for row in unsafe_rows[:30]:
            lines.append(row_line(row))
    else:
        lines.append("- none explicitly unsafe; most unresolved rows are manual-review blockers")
    lines.append("")
    lines.append("Edible-yield blockers:")
    if edible_rows:
        for row in edible_rows:
            lines.append(row_line(row))
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Cooked/raw state blockers:")
    if state_rows:
        for row in state_rows[:30]:
            lines.append(row_line(row))
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Remaining starch/carb blockers by grams:")
    if carb_rows:
        for row in carb_rows[:30]:
            lines.append(row_line(row))
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Specific decisions")
    lines.append("------------------")
    for key in (
        "red potatoes",
        "chicken thighs",
        "bone in chicken pieces",
        "ground turkey",
        "turkey",
        "pork",
        "beef",
    ):
        match = next((row for row in rows if clean_text(row.get("ingredient_name_normalized")) == key), None)
        if match:
            lines.append(
                f"- {key}: {match['suggested_action']} / {match['safety']} | "
                f"{match['recommended_decision']}"
            )
        else:
            lines.append(f"- {key}: not present as remaining gram blocker in round3 contributions")
    lines.append("")
    lines.append("Recommendation")
    lines.append("--------------")
    lines.append(
        "- Round4 should not be a broad automatic mapping pass. Apply only strict safe_auto row-level fixes, "
        "then decide edible-yield and generic meat/starch rules manually."
    )
    lines.append(
        "- Round4 alone is unlikely to make v1.1 fully materialization-ready unless chicken thigh/bone-in chicken, "
        "turkey, red potato, and generic pork/beef decisions are resolved."
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a decision audit for remaining Recipes_DB v1.1 round4 macro blockers."
    )
    parser.add_argument("--parsed_ingredients", default=str(DEFAULT_PARSED_INGREDIENTS))
    parser.add_argument("--mapping", default=str(DEFAULT_MAPPING))
    parser.add_argument("--contributions", default=str(DEFAULT_CONTRIBUTIONS))
    parser.add_argument("--fooddb", default=str(DEFAULT_FOODDB))
    parser.add_argument("--out_audit", default=str(OUT_AUDIT))
    parser.add_argument("--out_summary", default=str(OUT_SUMMARY))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    read_csv(Path(args.parsed_ingredients))
    mapping_rows = read_csv(Path(args.mapping))
    contribution_rows = read_csv(Path(args.contributions))
    food_rows = read_csv(Path(args.fooddb))

    audit_rows = build_audit(mapping_rows, contribution_rows, food_rows)
    write_csv(Path(args.out_audit), audit_rows, AUDIT_COLUMNS)
    write_summary(Path(args.out_summary), audit_rows)

    print("Round4 decision blockers audit built")
    print(f"blocker_count={len(audit_rows)}")
    print(f"safe_auto_grams={format_number(total_grams(audit_rows, safety='safe_auto'))}")
    print(f"written_audit={args.out_audit}")
    print(f"written_summary={args.out_summary}")


if __name__ == "__main__":
    main()
