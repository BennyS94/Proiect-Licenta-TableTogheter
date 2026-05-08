from __future__ import annotations

import csv
import math
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

INPUT_INGREDIENTS = (
    REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_ingredients_parsed_unit_rules_round5.csv"
)
INPUT_MAPPING = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "draft"
    / "recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_round7_macro_blockers.csv"
)
INPUT_CONTRIBUTIONS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_ingredient_nutrition_contributions_round7.csv"
)
INPUT_FOODDB = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round7.csv"

OUT_AUDIT = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round8_accepted_no_grams_audit.csv"
OUT_SUMMARY = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round8_accepted_no_grams_summary.txt"

AUDIT_COLUMNS = [
    "ingredient_name_normalized",
    "ingredient_raw_text",
    "quantity_value",
    "quantity_unit",
    "mapped_food_id",
    "mapped_food_canonical_name",
    "affected_recipe_id",
    "display_name",
    "macro_role",
    "estimated_priority",
    "possible_unit_rule",
    "safety",
    "recommended_action",
]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def clean_text(value: object) -> str:
    return str(value or "").strip()


def normalize_text(value: object) -> str:
    text = clean_text(value).casefold()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("&", " and ")
    text = text.replace("_", " ")
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_positive_float(value: object) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    if math.isnan(parsed) or math.isinf(parsed) or parsed <= 0:
        return None
    return parsed


def row_key(row: dict[str, str]) -> tuple[str, str]:
    return clean_text(row.get("recipe_id_candidate")), clean_text(row.get("ingredient_position"))


def macro_role(row: dict[str, str]) -> str:
    name = normalize_text(row.get("ingredient_name_normalized"))
    mapped_name = normalize_text(row.get("mapped_food_canonical_name"))
    text = f"{name} {mapped_name}"
    if name == "water" or "broth" in text or "stock" in text:
        return "water_low_impact"
    if any(token in text for token in ("salt", "pepper", "paprika", "cayenne", "thyme", "bay", "nutmeg", "curry", "coriander", "caraway")):
        return "seasoning"
    if any(token in text for token in ("oil", "butter", "mayonnaise")):
        return "fat"
    if any(token in text for token in ("cheese", "milk", "ricotta", "mozzarella", "parmesan")):
        return "dairy"
    if any(token in text for token in ("chicken", "beef", "pork", "turkey", "shrimp", "ham", "steak", "egg")):
        return "protein"
    if any(token in text for token in ("rice", "potato", "flour", "cornstarch", "honey", "sugar")):
        return "carb"
    if any(token in text for token in ("apple", "fruit", "berry", "lemon", "lime")):
        return "fruit"
    if any(token in text for token in ("celery", "onion", "garlic", "beans", "asparagus", "mushroom", "tomatillo")):
        return "veg"
    return "unknown"


def possible_unit_rule(row: dict[str, str]) -> str:
    name = normalize_text(row.get("ingredient_name_normalized"))
    raw = normalize_text(row.get("ingredient_raw_text"))
    unit = normalize_text(row.get("quantity_unit"))
    if name == "mayonnaise" and unit in {"teaspoon", "teaspoons", "tsp"}:
        return "mayonnaise_teaspoon_4_7g"
    if name == "mayonnaise" and unit in {"tablespoon", "tablespoons", "tbsp"}:
        return "mayonnaise_tablespoon_14g"
    if name == "mayonnaise" and unit in {"cup", "cups"}:
        return "mayonnaise_cup_230g"
    if name in {"flour", "all purpose flour", "whole wheat flour"} and unit in {"cup", "cups"}:
        return "flour_cup_120g"
    if name in {"flour", "all purpose flour", "whole wheat flour"} and unit in {"tablespoon", "tablespoons", "tbsp"}:
        return "flour_tablespoon_7_5g"
    if "rice" in name and "vinegar" not in raw and "noodle" not in raw and "flour" not in raw and unit in {"cup", "cups"}:
        if any(signal in raw for signal in ("cooked", "dry", "uncooked", "raw")):
            return "rice_cup_rule_requires_state"
    if name == "mushrooms" and unit in {"cup", "cups"} and ("sliced" in raw or "chopped" in raw):
        return "mushrooms_sliced_or_chopped_cup_70g"
    if name == "asparagus" and unit in {"cup", "cups"}:
        return "asparagus_cup_134g"
    if name == "asparagus" and unit in {"spear", "spears", "count", "piece", "pieces"}:
        return "asparagus_spear_16g"
    if name in {"tomatillo", "tomatillos"} and unit in {"count", "piece", "pieces", "whole"}:
        return "tomatillo_count_34g"
    if name in {"tomatillo", "tomatillos"} and unit in {"cup", "cups"} and "chopped" in raw:
        return "tomatillo_chopped_cup_132g"
    if name in {"mozzarella cheese", "shredded mozzarella cheese"} and unit in {"cup", "cups"}:
        return "mozzarella_cheese_cup_112g"
    return ""


def classify_row(row: dict[str, str]) -> tuple[str, str, str, str]:
    role = macro_role(row)
    rule = possible_unit_rule(row)
    if rule:
        return role, "high" if role in {"protein", "carb", "fat", "dairy"} else "medium", "safe_auto", "add_unit_rule"
    if role in {"seasoning", "water_low_impact"}:
        return role, "low", "needs_review", "keep_review"
    if role in {"protein", "carb", "fat", "dairy"}:
        return role, "high", "needs_review", "mapping_fix_needed"
    if role in {"veg", "fruit"}:
        return role, "medium", "needs_review", "keep_review"
    return role, "medium", "needs_review", "keep_review"


def clipped(values: list[str], limit: int = 8) -> str:
    output: list[str] = []
    seen: set[str] = set()
    for value in values:
        cleaned = clean_text(value)
        if not cleaned or cleaned in seen:
            continue
        output.append(cleaned)
        seen.add(cleaned)
        if len(output) >= limit:
            break
    return " | ".join(output)


def build_summary(rows: list[dict[str, object]]) -> str:
    ingredient_counts = Counter(clean_text(row["ingredient_name_normalized"]) for row in rows)
    priority_counts = Counter(clean_text(row["estimated_priority"]) for row in rows)
    role_counts = Counter(clean_text(row["macro_role"]) for row in rows)
    action_counts = Counter(clean_text(row["recommended_action"]) for row in rows)
    rule_counts = Counter(clean_text(row["possible_unit_rule"]) for row in rows if clean_text(row["possible_unit_rule"]))
    high_rows = [row for row in rows if row["estimated_priority"] == "high"]

    lines = [
        "Recipes_DB v1.1 round8 accepted_auto fara grame audit",
        "=" * 58,
        "",
        f"accepted_auto rows without grams: {len(rows)}",
        f"high-priority accepted_auto rows without grams: {len(high_rows)}",
        "",
        "Macro role counts:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in role_counts.most_common())
    lines.extend(["", "Priority counts:"])
    lines.extend(f"- {name}: {count}" for name, count in priority_counts.most_common())
    lines.extend(["", "Recommended actions:"])
    lines.extend(f"- {name}: {count}" for name, count in action_counts.most_common())
    lines.extend(["", "Top ingredients by frequency:"])
    lines.extend(f"- {name}: {count}" for name, count in ingredient_counts.most_common(30))
    lines.extend(["", "Top ingredients by likely macro impact:"])
    macro_rows = [row for row in rows if row["estimated_priority"] in {"high", "medium"}]
    macro_counts = Counter(clean_text(row["ingredient_name_normalized"]) for row in macro_rows)
    lines.extend(f"- {name}: {count}" for name, count in macro_counts.most_common(30))
    lines.extend(["", "Top unit rules that would recover useful coverage:"])
    if rule_counts:
        lines.extend(f"- {name}: {count}" for name, count in rule_counts.most_common(30))
    else:
        lines.append("- none")
    return "\n".join(lines) + "\n"


def main() -> None:
    ingredient_rows = read_csv(INPUT_INGREDIENTS)
    mapping_rows = read_csv(INPUT_MAPPING)
    read_csv(INPUT_CONTRIBUTIONS)
    read_csv(INPUT_FOODDB)
    ingredient_index = {row_key(row): row for row in ingredient_rows}

    output_rows: list[dict[str, object]] = []
    for row in mapping_rows:
        if clean_text(row.get("mapping_status")) != "accepted_auto":
            continue
        if parse_positive_float(row.get("quantity_grams_estimated")) is not None:
            continue
        source_row = ingredient_index.get(row_key(row), row)
        combined = dict(source_row)
        combined.update(row)
        role, priority, safety, action = classify_row(combined)
        output_rows.append(
            {
                "ingredient_name_normalized": clean_text(combined.get("ingredient_name_normalized")),
                "ingredient_raw_text": clean_text(combined.get("ingredient_raw_text")),
                "quantity_value": clean_text(combined.get("quantity_value")),
                "quantity_unit": clean_text(combined.get("quantity_unit")),
                "mapped_food_id": clean_text(combined.get("mapped_food_id")),
                "mapped_food_canonical_name": clean_text(combined.get("mapped_food_canonical_name")),
                "affected_recipe_id": clean_text(combined.get("recipe_id_candidate")),
                "display_name": clean_text(combined.get("display_name")),
                "macro_role": role,
                "estimated_priority": priority,
                "possible_unit_rule": possible_unit_rule(combined),
                "safety": safety,
                "recommended_action": action,
            }
        )

    output_rows.sort(
        key=lambda item: (
            {"high": 0, "medium": 1, "low": 2}.get(clean_text(item["estimated_priority"]), 3),
            clean_text(item["ingredient_name_normalized"]),
            clean_text(item["display_name"]),
        )
    )
    write_csv(OUT_AUDIT, output_rows, AUDIT_COLUMNS)
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(build_summary(output_rows), encoding="utf-8")

    print(f"accepted_auto_without_grams={len(output_rows)}")
    print(f"written_audit={OUT_AUDIT}")
    print(f"written_summary={OUT_SUMMARY}")


if __name__ == "__main__":
    main()
