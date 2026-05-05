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
    / "recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_round2_unit_rules_review_promotions.csv"
)
DEFAULT_CONTRIBUTIONS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_ingredient_nutrition_contributions.csv"
)
DEFAULT_FOODDB = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round2.csv"

OUT_AUDIT = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_remaining_macro_blockers_audit.csv"
OUT_SUMMARY = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_remaining_macro_blockers_summary.txt"

AUDIT_COLUMNS = [
    "ingredient_name_normalized",
    "total_grams_affected",
    "affected_recipe_count",
    "row_count",
    "example_raw_texts",
    "example_recipes",
    "current_status",
    "possible_existing_fooddb_match",
    "possible_fooddb_id",
    "safety",
    "recommended_action",
    "audit_notes",
]

TARGET_FOOD_IDS = {
    "beef_ground_generic": "food_beef_minced_steak_15_fat_raw",
    "beef_ground_lean": "food_beef_minced_steak_10_fat_raw",
    "beef_stew": "food_beef_stewing_meat_raw",
    "potato_raw": "food_potato_peeled_raw",
    "potato_cooked": "food_potato_boiled_cooked_in_water",
    "rice_cooked": "food_rice_cooked_unsalted",
    "rice_raw": "food_rice_raw",
    "jasmine_rice_cooked": "food_rice_thai_cooked",
    "mozzarella": "food_mozzarella_cheese_from_cow_s_milk",
    "pork_shoulder": "food_pork_shoulder_raw",
    "pork_loin": "food_pork_loin_raw",
    "onions": "food_onion_raw",
    "garlic_cloves": "food_garlic_fresh",
    "kosher_salt": "food_salt_white_sea_igneous_or_rock_no_enrichment",
    "turkey_generic": "food_turkey_meat_raw",
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


def normalize_text(value: object) -> str:
    text = clean_text(value).casefold()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


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


def food_id_exists(food_ids: set[str], food_id: str) -> bool:
    return food_id in food_ids


def classify_blocker(
    ingredient_name: str,
    raw_texts: list[str],
    food_ids: set[str],
) -> tuple[str, str, str, str, str]:
    joined_raw = " ".join(normalize_text(raw) for raw in raw_texts)
    possible_match = ""
    possible_food_id = ""
    safety = "needs_review"
    recommended_action = "keep_review"
    notes: list[str] = []

    if ingredient_name == "beef":
        if "ground beef" in joined_raw:
            possible_food_id = TARGET_FOOD_IDS["beef_ground_generic"]
            possible_match = "ground beef rows can map to beef_minced_steak_15_fat_raw"
            safety = "safe_auto" if food_id_exists(food_ids, possible_food_id) else "needs_review"
            recommended_action = "promote_exact_mapping" if safety == "safe_auto" else "keep_review"
            notes.append("generic ingredient name stays row-level only; do not map all beef")
        else:
            possible_food_id = "food_beef_steak_or_beef_steak_raw"
            possible_match = "generic beef candidate is too broad"
            safety = "needs_review"
            recommended_action = "keep_review"
    elif ingredient_name == "red potatoes":
        possible_food_id = TARGET_FOOD_IDS["potato_raw"]
        possible_match = "generic potato exists, but exact red potato item is missing"
        safety = "needs_review"
        recommended_action = "keep_review"
    elif ingredient_name == "potatoes":
        possible_food_id = TARGET_FOOD_IDS["potato_raw"]
        possible_match = "potato raw/cooked exists; promote only rows with clear state"
        safety = "safe_auto" if food_id_exists(food_ids, possible_food_id) else "needs_review"
        recommended_action = "promote_exact_mapping" if safety == "safe_auto" else "keep_review"
    elif ingredient_name in {"white rice", "uncooked white rice", "jasmine rice", "rice"}:
        if "rice vinegar" in joined_raw or "rice wine" in joined_raw or "rice flour" in joined_raw:
            safety = "unsafe"
            recommended_action = "keep_unmapped"
            possible_match = "rice non-grain context"
        elif "cooked" in joined_raw:
            possible_food_id = TARGET_FOOD_IDS["jasmine_rice_cooked"] if ingredient_name == "jasmine rice" else TARGET_FOOD_IDS["rice_cooked"]
            possible_match = "cooked rice state is explicit"
            safety = "safe_auto" if food_id_exists(food_ids, possible_food_id) else "needs_review"
            recommended_action = "promote_exact_mapping" if safety == "safe_auto" else "keep_review"
        elif "uncooked" in joined_raw or "raw" in joined_raw or "dry" in joined_raw:
            possible_food_id = TARGET_FOOD_IDS["rice_raw"]
            possible_match = "raw/uncooked rice state is explicit"
            safety = "safe_auto" if food_id_exists(food_ids, possible_food_id) else "needs_review"
            recommended_action = "promote_exact_mapping" if safety == "safe_auto" else "keep_review"
        else:
            possible_food_id = TARGET_FOOD_IDS["rice_raw"]
            possible_match = "generic rice state unclear"
            safety = "needs_review"
            recommended_action = "keep_review"
    elif ingredient_name == "mozzarella cheese":
        possible_food_id = TARGET_FOOD_IDS["mozzarella"]
        possible_match = "mozzarella from cow milk exists"
        safety = "safe_auto" if food_id_exists(food_ids, possible_food_id) else "needs_review"
        recommended_action = "promote_exact_mapping" if safety == "safe_auto" else "keep_review"
    elif ingredient_name == "pork shoulder":
        possible_food_id = TARGET_FOOD_IDS["pork_shoulder"]
        possible_match = "pork shoulder raw exists"
        safety = "safe_auto" if food_id_exists(food_ids, possible_food_id) else "needs_review"
        recommended_action = "promote_exact_mapping" if safety == "safe_auto" else "keep_review"
    elif ingredient_name == "pork loin":
        possible_food_id = TARGET_FOOD_IDS["pork_loin"]
        possible_match = "pork loin raw exists"
        safety = "safe_auto" if food_id_exists(food_ids, possible_food_id) else "needs_review"
        recommended_action = "promote_exact_mapping" if safety == "safe_auto" else "keep_review"
    elif ingredient_name == "pork":
        possible_match = "generic pork remains broad"
        safety = "needs_review"
        recommended_action = "keep_review"
    elif ingredient_name == "turkey":
        possible_food_id = TARGET_FOOD_IDS["turkey_generic"]
        possible_match = "generic turkey meat exists but ground turkey is not exact"
        safety = "needs_review"
        recommended_action = "keep_review"
    elif ingredient_name in {"chicken thighs", "bone in chicken pieces"}:
        possible_match = "needs exact cut plus edible-yield handling"
        safety = "needs_review"
        recommended_action = "needs_edible_yield_rule"
    elif ingredient_name == "onions":
        possible_food_id = TARGET_FOOD_IDS["onions"]
        possible_match = "plural onion can map to onion_raw"
        safety = "safe_auto" if food_id_exists(food_ids, possible_food_id) else "needs_review"
        recommended_action = "promote_exact_mapping" if safety == "safe_auto" else "keep_review"
    elif ingredient_name == "garlic cloves":
        possible_food_id = TARGET_FOOD_IDS["garlic_cloves"]
        possible_match = "garlic cloves can map to garlic_fresh"
        safety = "safe_auto" if food_id_exists(food_ids, possible_food_id) else "needs_review"
        recommended_action = "promote_exact_mapping" if safety == "safe_auto" else "keep_review"
    elif ingredient_name == "kosher salt":
        possible_food_id = TARGET_FOOD_IDS["kosher_salt"]
        possible_match = "salt variant can map to salt for low macro impact"
        safety = "safe_auto" if food_id_exists(food_ids, possible_food_id) else "needs_review"
        recommended_action = "promote_exact_mapping" if safety == "safe_auto" else "keep_review"
    else:
        possible_match = "not targeted in this blocker pass"
        safety = "needs_review"
        recommended_action = "keep_review"

    return possible_match, possible_food_id, safety, recommended_action, "; ".join(notes)


def build_audit(
    mapping_rows: list[dict[str, str]],
    contribution_rows: list[dict[str, str]],
    food_rows: list[dict[str, str]],
) -> list[dict[str, object]]:
    food_ids = {clean_text(row.get("food_id")) for row in food_rows}
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
            "row_count": 0,
        }
    )

    for row in contribution_rows:
        grams = parse_float(row.get("quantity_grams_estimated"))
        if grams is None or grams <= 0 or clean_text(row.get("contribution_status")) == "used":
            continue
        key = (clean_text(row.get("recipe_id_candidate")), clean_text(row.get("ingredient_position")))
        mapping_row = mapping_by_key.get(key, {})
        mapping_status = clean_text(mapping_row.get("mapping_status"))
        if mapping_status == "accepted_auto":
            continue
        ingredient_name = clean_text(row.get("ingredient_name_normalized"))
        item = grouped[ingredient_name]
        item["total_grams"] = float(item["total_grams"]) + grams
        item["recipe_ids"].add(clean_text(row.get("recipe_id_candidate")))
        item["raw_texts"].append(clean_text(row.get("ingredient_raw_text")))
        item["recipes"].append(clean_text(row.get("display_name")))
        item["statuses"][mapping_status] += 1
        item["row_count"] = int(item["row_count"]) + 1

    output: list[dict[str, object]] = []
    for ingredient_name, item in grouped.items():
        match, food_id, safety, action, notes = classify_blocker(
            ingredient_name,
            list(item["raw_texts"]),
            food_ids,
        )
        status_counts = "; ".join(f"{status}:{count}" for status, count in item["statuses"].most_common())
        output.append(
            {
                "ingredient_name_normalized": ingredient_name,
                "total_grams_affected": format_number(float(item["total_grams"])),
                "affected_recipe_count": len(item["recipe_ids"]),
                "row_count": item["row_count"],
                "example_raw_texts": clipped(list(item["raw_texts"])),
                "example_recipes": clipped(list(item["recipes"])),
                "current_status": status_counts,
                "possible_existing_fooddb_match": match,
                "possible_fooddb_id": food_id,
                "safety": safety,
                "recommended_action": action,
                "audit_notes": notes,
            }
        )
    return sorted(output, key=lambda row: (-parse_float(row["total_grams_affected"]), clean_text(row["ingredient_name_normalized"])))


def write_summary(path: Path, audit_rows: list[dict[str, object]]) -> None:
    safety_counts = Counter(clean_text(row.get("safety")) for row in audit_rows)
    action_counts = Counter(clean_text(row.get("recommended_action")) for row in audit_rows)
    lines: list[str] = []
    lines.append("Recipes_DB v1.1 remaining macro blockers audit")
    lines.append("=" * 52)
    lines.append("")
    lines.append(f"Total blockers with grams: {len(audit_rows)}")
    lines.append("")
    lines.append("Safety counts:")
    for safety, count in safety_counts.most_common():
        lines.append(f"- {safety}: {count}")
    lines.append("")
    lines.append("Recommended action counts:")
    for action, count in action_counts.most_common():
        lines.append(f"- {action}: {count}")
    lines.append("")
    lines.append("Top blockers by grams:")
    for row in audit_rows[:30]:
        lines.append(
            f"- {row['ingredient_name_normalized']}: {row['total_grams_affected']}g | "
            f"action={row['recommended_action']} | safety={row['safety']} | "
            f"food_id={row['possible_fooddb_id']}"
        )
    lines.append("")
    lines.append("Recommended next step:")
    lines.append("- Apply only safe row-level promotions, then rerun nutrition cache as round3 draft.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit remaining macro blockers for Recipes_DB v1.1.")
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
    print("Remaining macro blockers audit built")
    print(f"blocker_count={len(audit_rows)}")
    print(f"written_audit={args.out_audit}")
    print(f"written_summary={args.out_summary}")


if __name__ == "__main__":
    main()
