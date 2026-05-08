from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_ROUND6_BLOCKERS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round6_remaining_macro_blockers.csv"
)
DEFAULT_PARSED = (
    REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_ingredients_parsed_unit_rules_round5.csv"
)
DEFAULT_MAPPING = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "draft"
    / "recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_round5_manual_decisions.csv"
)
DEFAULT_FOODDB = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round5.csv"

OUT_AUDIT = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round7_macro_blockers_decision_audit.csv"
)
OUT_SUMMARY = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round7_macro_blockers_summary.txt"
)

AUDIT_COLUMNS = [
    "ingredient_name_normalized",
    "total_grams_affected",
    "affected_recipe_count",
    "example_raw_texts",
    "example_recipes",
    "possible_fooddb_match",
    "possible_fooddb_id",
    "source_macro_available",
    "macro_impact_role",
    "safety",
    "recommended_action",
    "risk_notes",
]

TARGET_FOOD_IDS = {
    "mayonnaise": "food_mayonnaise_70_fat_and_more_prepacked",
    "ham": "food_cooked_ham_choice",
    "bay_scallops": "food_scallop_without_coral_raw",
    "pork_sausage": "food_sausage_meat_pure_pork_raw",
}

ROLE_BY_NAME = {
    "mayonnaise": "fat",
    "ham": "protein",
    "bay scallops": "protein",
    "salted cod fish": "protein",
    "pork sausage": "protein",
    "mild italian sausage links": "protein",
    "spicy pork sausage": "protein",
    "spinach pasta dough": "carb",
    "pork neck bones": "protein",
    "pork": "protein",
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


def food_label(food_row: dict[str, str] | None) -> str:
    if not food_row:
        return ""
    return clean_text(food_row.get("canonical_name")) or clean_text(food_row.get("display_name"))


def has_macros(food_row: dict[str, str] | None) -> bool:
    if not food_row:
        return False
    for column in ("energy_kcal_100g", "protein_g_100g", "carbs_g_100g", "fat_g_100g"):
        if parse_float(food_row.get(column)) is None:
            return False
    return True


def macro_energy_plausible(food_row: dict[str, str] | None) -> bool:
    if not food_row or not has_macros(food_row):
        return False
    energy = parse_float(food_row.get("energy_kcal_100g")) or 0.0
    protein = parse_float(food_row.get("protein_g_100g")) or 0.0
    carbs = parse_float(food_row.get("carbs_g_100g")) or 0.0
    fat = parse_float(food_row.get("fat_g_100g")) or 0.0
    if energy < 0 or energy > 900 or protein < 0 or carbs < 0 or fat < 0:
        return False
    estimated = protein * 4 + carbs * 4 + fat * 9
    if energy == 0:
        return estimated == 0
    ratio = estimated / energy
    return 0.55 <= ratio <= 1.55


def build_food_lookup(food_rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {clean_text(row.get("food_id")): row for row in food_rows if clean_text(row.get("food_id"))}


def role_for_name(name: str) -> str:
    if name in ROLE_BY_NAME:
        return ROLE_BY_NAME[name]
    tokens = set(normalize_text(name).split())
    if tokens & {"oil", "butter", "mayonnaise", "mayo", "cream"}:
        return "fat"
    if tokens & {"rice", "pasta", "flour", "potato", "potatoes", "dough", "beans", "lentils"}:
        return "carb"
    if tokens & {"pork", "beef", "chicken", "turkey", "fish", "cod", "scallops", "ham", "sausage"}:
        return "protein"
    if tokens & {"salt", "pepper", "vinegar", "water", "broth"}:
        return "low_macro"
    return "unknown"


def decision_for_blocker(name: str, food_by_id: dict[str, dict[str, str]]) -> dict[str, str]:
    possible_food_id = ""
    safety = "needs_review"
    recommended_action = "keep_review"
    risk_notes: list[str] = []

    if name == "mayonnaise":
        possible_food_id = TARGET_FOOD_IDS["mayonnaise"]
        safety = "safe_auto"
        recommended_action = "promote_exact_mapping"
        risk_notes.append("exact mayonnaise item; do not map aioli or salad dressing")
    elif name == "ham":
        possible_food_id = TARGET_FOOD_IDS["ham"]
        safety = "safe_auto"
        recommended_action = "promote_exact_mapping"
        risk_notes.append("generic cooked ham item; do not map bacon prosciutto sausage")
    elif name == "bay scallops":
        possible_food_id = TARGET_FOOD_IDS["bay_scallops"]
        safety = "safe_auto"
        recommended_action = "promote_exact_mapping"
        risk_notes.append("bay scallops collapsed to raw scallop without coral for macro draft")
    elif name in {"pork sausage", "mild italian sausage links", "spicy pork sausage"}:
        possible_food_id = TARGET_FOOD_IDS["pork_sausage"]
        safety = "safe_auto" if name == "pork sausage" else "needs_review"
        recommended_action = "promote_exact_mapping"
        risk_notes.append("sausage_variant_collapsed_to_generic_pork_sausage_v1_1")
    elif name == "salted cod fish":
        safety = "needs_review"
        recommended_action = "keep_review"
        risk_notes.append("exact salted cod item not present; do not force to generic cod")
    elif name == "spinach pasta dough":
        safety = "needs_review"
        recommended_action = "keep_review"
        risk_notes.append("exact spinach pasta dough item not present")
    elif name == "pork neck bones":
        safety = "needs_review"
        recommended_action = "add_edible_yield_rule"
        risk_notes.append("bone-specific pork yield/source decision still needed")
    elif name == "pork":
        safety = "needs_review"
        recommended_action = "keep_review"
        risk_notes.append("generic pork remains too broad for global mapping")
    elif "vinegar" in name:
        safety = "unsafe"
        recommended_action = "keep_unmapped"
        risk_notes.append("vinegar variants are not macro blockers and remain unsafe for rice mapping")
    else:
        safety = "needs_review"
        recommended_action = "keep_review"
        risk_notes.append("not targeted in round7 macro-blocker pass")

    food_row = food_by_id.get(possible_food_id)
    if possible_food_id and not macro_energy_plausible(food_row):
        safety = "needs_review"
        recommended_action = "keep_review"
        risk_notes.append("candidate macros missing or sanity check failed")

    return {
        "possible_fooddb_match": food_label(food_row),
        "possible_fooddb_id": possible_food_id if food_row else "",
        "source_macro_available": "true" if macro_energy_plausible(food_row) else "false",
        "macro_impact_role": role_for_name(name),
        "safety": safety,
        "recommended_action": recommended_action,
        "risk_notes": "; ".join(risk_notes),
    }


def build_audit(blocker_rows: list[dict[str, str]], food_rows: list[dict[str, str]]) -> list[dict[str, object]]:
    food_by_id = build_food_lookup(food_rows)
    audit_rows: list[dict[str, object]] = []
    for blocker in blocker_rows:
        name = normalize_text(blocker.get("ingredient_name_normalized"))
        decision = decision_for_blocker(name, food_by_id)
        audit_rows.append(
            {
                "ingredient_name_normalized": clean_text(blocker.get("ingredient_name_normalized")),
                "total_grams_affected": clean_text(blocker.get("total_grams")),
                "affected_recipe_count": clean_text(blocker.get("affected_recipe_count")),
                "example_raw_texts": clean_text(blocker.get("example_raw_texts")),
                "example_recipes": clean_text(blocker.get("affected_recipes")),
                **decision,
            }
        )
    return audit_rows


def write_summary(path: Path, audit_rows: list[dict[str, object]]) -> None:
    action_counts = Counter(clean_text(row.get("recommended_action")) for row in audit_rows)
    safety_counts = Counter(clean_text(row.get("safety")) for row in audit_rows)
    promotable = [row for row in audit_rows if clean_text(row.get("recommended_action")) == "promote_exact_mapping"]
    deferred = [row for row in audit_rows if clean_text(row.get("recommended_action")) != "promote_exact_mapping"]

    lines = [
        "Recipes_DB v1.1 round7 macro-blocker decision summary",
        "========================================================",
        "",
        f"Blockers reviewed: {len(audit_rows)}",
        f"Promote exact/approved mappings: {len(promotable)}",
        f"Deferred or review decisions: {len(deferred)}",
        "",
        "Safety counts:",
    ]
    lines.extend(f"- {name}: {count}" for name, count in safety_counts.most_common())
    lines.append("")
    lines.append("Recommended action counts:")
    lines.extend(f"- {name}: {count}" for name, count in action_counts.most_common())
    lines.append("")
    lines.append("Round7 approved mapping candidates:")
    for row in promotable:
        lines.append(
            f"- {row['ingredient_name_normalized']} -> {row['possible_fooddb_id']} | "
            f"{row['possible_fooddb_match']} | safety={row['safety']} | notes={row['risk_notes']}"
        )
    lines.append("")
    lines.append("Still deferred high-impact examples:")
    for row in deferred[:20]:
        lines.append(
            f"- {row['ingredient_name_normalized']} | action={row['recommended_action']} | "
            f"safety={row['safety']} | notes={row['risk_notes']}"
        )
    lines.append("")
    lines.append("Recommended next patch:")
    lines.append("- Build Food_DB round7 draft as alias-to-existing decisions, rerun mapping, then rebuild nutrition cache.")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(args: argparse.Namespace) -> None:
    blocker_rows = read_csv(args.round6_blockers_path)
    read_csv(args.parsed_path)
    read_csv(args.mapping_path)
    food_rows = read_csv(args.fooddb_path)
    audit_rows = build_audit(blocker_rows, food_rows)
    write_csv(OUT_AUDIT, audit_rows, AUDIT_COLUMNS)
    write_summary(OUT_SUMMARY, audit_rows)
    action_counts = Counter(clean_text(row.get("recommended_action")) for row in audit_rows)
    print("Recipes_DB v1.1 round7 macro-blocker decision audit built")
    print(f"blockers_reviewed={len(audit_rows)}")
    print(f"promote_exact_mapping={action_counts['promote_exact_mapping']}")
    print(f"keep_review={action_counts['keep_review']}")
    print(f"keep_unmapped={action_counts['keep_unmapped']}")
    print(f"written={OUT_AUDIT}")
    print(f"written={OUT_SUMMARY}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit round7 macro-blocker decisions for Recipes_DB v1.1.")
    parser.add_argument("--round6_blockers_path", type=Path, default=DEFAULT_ROUND6_BLOCKERS)
    parser.add_argument("--parsed_path", type=Path, default=DEFAULT_PARSED)
    parser.add_argument("--mapping_path", type=Path, default=DEFAULT_MAPPING)
    parser.add_argument("--fooddb_path", type=Path, default=DEFAULT_FOODDB)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())

