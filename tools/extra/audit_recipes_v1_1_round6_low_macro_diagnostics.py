from __future__ import annotations

import argparse
import csv
import re
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_CURATED = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_curated_200.csv"
DEFAULT_CACHE = (
    REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_nutrition_cache_draft_round5.csv"
)
DEFAULT_CONTRIBUTIONS = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "audit"
    / "recipes_v1_1_ingredient_nutrition_contributions_round5.csv"
)
DEFAULT_MAPPING = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "draft"
    / "recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_round5_manual_decisions.csv"
)
DEFAULT_PARSED = (
    REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_ingredients_parsed_unit_rules_round5.csv"
)
DEFAULT_FOODDB = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round5.csv"

OUT_DIAGNOSTICS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round6_low_macro_diagnostics.csv"
)
OUT_SUMMARY = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round6_low_macro_summary.txt"
OUT_SERVINGS_REVIEW = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round6_servings_review.csv"
)
OUT_REPLACEMENT_CANDIDATES = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "audit"
    / "recipes_v1_1_round6_recipe_replacement_candidates.csv"
)
OUT_REMAINING_BLOCKERS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round6_remaining_macro_blockers.csv"
)

MAIN_LIKE_KINDS = {"complete_main", "near_complete_main", "soup", "salad"}
COMPONENT_KINDS = {"protein_component", "carb_side", "veg_side"}

DIAGNOSTIC_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "recipe_kind_guess",
    "primary_protein",
    "cache_status",
    "servings_basis",
    "servings_estimation_method",
    "mapped_weight_ratio",
    "macro_relevant_mapped_weight_ratio",
    "energy_kcal_total",
    "protein_g_total",
    "carbs_g_total",
    "fat_g_total",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "carbs_g_per_serving",
    "fat_g_per_serving",
    "accepted_auto_with_grams_count",
    "review_needed_with_grams_count",
    "unmapped_with_grams_count",
    "known_weight_grams_sum",
    "mapped_weight_grams",
    "macro_relevant_mapped_weight_grams",
    "low_or_no_macro_mapped_weight_grams",
    "known_weight_per_serving_grams",
    "low_or_no_macro_mapped_weight_share",
    "estimated_unresolved_kcal",
    "estimated_unresolved_protein_g",
    "estimated_unresolved_carbs_g",
    "estimated_unresolved_fat_g",
    "top_unresolved_macro_blockers",
    "main_diagnosis",
    "diagnosis_reasons",
    "suggested_next_action",
]

SERVINGS_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "recipe_kind_guess",
    "primary_protein",
    "servings_basis",
    "servings_estimation_method",
    "known_weight_grams_sum",
    "known_weight_per_serving_grams",
    "energy_kcal_total",
    "protein_g_total",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "mapped_weight_ratio",
    "macro_relevant_mapped_weight_ratio",
    "servings_issue_type",
    "servings_review_score",
    "rough_servings_by_weight",
    "review_notes",
]

REPLACEMENT_COLUMNS = [
    "recipe_id_candidate",
    "display_name",
    "recipe_kind_guess",
    "primary_protein",
    "cache_status",
    "main_diagnosis",
    "replacement_score",
    "mapped_weight_ratio",
    "macro_relevant_mapped_weight_ratio",
    "energy_kcal_per_serving",
    "protein_g_per_serving",
    "known_weight_grams_sum",
    "top_unresolved_macro_blockers",
    "candidate_reason",
    "suggested_next_action",
]

BLOCKER_COLUMNS = [
    "ingredient_name_normalized",
    "likely_role",
    "total_grams",
    "row_count",
    "affected_recipe_count",
    "affected_recipes",
    "example_raw_texts",
    "mapping_statuses",
    "mapped_food_ids_seen",
    "estimate_source",
    "estimated_lost_kcal",
    "estimated_lost_protein_g",
    "estimated_lost_carbs_g",
    "estimated_lost_fat_g",
    "estimated_macro_impact_score",
    "likely_lost_kcal_class",
    "likely_lost_protein_class",
    "likely_lost_carbs_class",
    "likely_lost_fat_class",
    "suggested_next_action",
    "notes",
]

FOOD_MACRO_COLUMNS = {
    "energy_kcal_100g": "energy_kcal",
    "protein_g_100g": "protein_g",
    "carbs_g_100g": "carbs_g",
    "fat_g_100g": "fat_g",
}

ROLE_ESTIMATES_100G = {
    "protein": (180.0, 20.0, 0.0, 9.0),
    "carb": (220.0, 5.0, 45.0, 1.0),
    "fat": (750.0, 0.5, 1.0, 82.0),
    "dairy": (250.0, 15.0, 5.0, 17.0),
    "veg": (35.0, 1.5, 7.0, 0.3),
    "fruit": (60.0, 0.8, 14.0, 0.2),
    "sauce": (120.0, 2.0, 14.0, 5.0),
    "seasoning": (20.0, 0.5, 4.0, 0.2),
    "acid": (20.0, 0.0, 3.0, 0.0),
    "water": (0.0, 0.0, 0.0, 0.0),
    "unknown": (100.0, 3.0, 12.0, 3.0),
}

PROTEIN_WORDS = {
    "beef",
    "pork",
    "turkey",
    "chicken",
    "sausage",
    "ham",
    "fish",
    "cod",
    "salmon",
    "shrimp",
    "scallop",
    "meat",
    "tofu",
    "egg",
    "eggs",
    "pancetta",
    "bacon",
    "lobster",
    "crab",
}
CARB_WORDS = {
    "rice",
    "pasta",
    "flour",
    "potato",
    "potatoes",
    "bread",
    "noodle",
    "noodles",
    "dough",
    "tortilla",
    "quinoa",
    "oats",
    "bean",
    "beans",
    "lentil",
    "lentils",
}
FAT_WORDS = {"oil", "butter", "mayonnaise", "mayo", "shortening", "margarine", "cream"}
VEG_WORDS = {
    "mushroom",
    "mushrooms",
    "asparagus",
    "tomatillo",
    "tomatillos",
    "spinach",
    "broccoli",
    "tomato",
    "tomatoes",
    "zucchini",
    "onion",
    "pepper",
    "celery",
}
SEASONING_WORDS = {
    "salt",
    "pepper",
    "oregano",
    "basil",
    "thyme",
    "parsley",
    "rosemary",
    "sage",
    "paprika",
    "cumin",
    "cayenne",
    "seasoning",
}
ACID_WORDS = {"vinegar", "lemon", "lime"}
WATER_WORDS = {"water", "broth", "stock"}

DEFERRED_KEY_MACRO_WORDS = {
    "beef",
    "pork",
    "turkey",
    "chicken",
    "thigh",
    "potato",
    "potatoes",
    "rice",
    "pasta",
    "flour",
    "sausage",
    "fish",
    "cod",
    "ham",
    "mushroom",
    "mushrooms",
    "mayonnaise",
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


def num(value: object) -> float:
    parsed = parse_float(value)
    return 0.0 if parsed is None else parsed


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


def tokens(text: str) -> set[str]:
    return set(normalize_text(text).split())


def ratio(numerator: float, denominator: float) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def format_counter(counter: Counter[str], limit: int = 8) -> str:
    parts = []
    for key, count in counter.most_common(limit):
        label = key or "<blank>"
        parts.append(f"{label}:{count}")
    return " | ".join(parts)


def build_food_lookup(food_rows: list[dict[str, str]]) -> tuple[dict[str, dict[str, str]], dict[str, dict[str, str]]]:
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


def food_macros(food_row: dict[str, str] | None) -> tuple[float, float, float, float] | None:
    if not food_row:
        return None
    values = []
    for column in FOOD_MACRO_COLUMNS:
        parsed = parse_float(food_row.get(column))
        if parsed is None:
            return None
        values.append(parsed)
    return values[0], values[1], values[2], values[3]


def infer_role(name: str, raw_text: str = "", parsed_role: str = "") -> str:
    role = normalize_text(parsed_role)
    if role == "fat_source":
        return "fat"
    if role in {"protein", "carb", "veg", "dairy", "seasoning", "sauce"}:
        return role

    joined = f"{normalize_text(name)} {normalize_text(raw_text)}"
    token_set = tokens(joined)
    if token_set & WATER_WORDS:
        return "water"
    if token_set & PROTEIN_WORDS:
        return "protein"
    if token_set & FAT_WORDS:
        return "fat"
    if token_set & CARB_WORDS:
        return "carb"
    if token_set & VEG_WORDS:
        return "veg"
    if token_set & SEASONING_WORDS:
        return "seasoning"
    if token_set & ACID_WORDS:
        return "acid"
    if "fruit" in token_set or "pineapple" in token_set or "apple" in token_set:
        return "fruit"
    return "unknown"


def estimate_macros_for_unresolved(
    row: dict[str, str],
    parsed_row: dict[str, str] | None,
    food_by_id: dict[str, dict[str, str]],
    food_by_name: dict[str, dict[str, str]],
) -> tuple[str, str, tuple[float, float, float, float]]:
    mapped_food_id = clean_text(row.get("mapped_food_id"))
    if mapped_food_id:
        macros = food_macros(food_by_id.get(mapped_food_id))
        if macros:
            return infer_role(row.get("ingredient_name_normalized", ""), row.get("ingredient_raw_text", ""), parsed_row.get("ingredient_role", "") if parsed_row else ""), "mapped_food_id", macros

    name = clean_text(row.get("ingredient_name_normalized"))
    possible_food = food_by_name.get(normalize_text(name))
    macros = food_macros(possible_food)
    if macros:
        return infer_role(name, row.get("ingredient_raw_text", ""), parsed_row.get("ingredient_role", "") if parsed_row else ""), "exact_fooddb_name", macros

    parsed_role = parsed_row.get("ingredient_role", "") if parsed_row else ""
    role = infer_role(name, row.get("ingredient_raw_text", ""), parsed_role)
    return role, "role_heuristic", ROLE_ESTIMATES_100G.get(role, ROLE_ESTIMATES_100G["unknown"])


def macro_class(value: float, macro_name: str) -> str:
    if macro_name == "kcal":
        if value >= 1000:
            return "high"
        if value >= 300:
            return "medium"
        if value > 50:
            return "low"
        return "minimal"
    if macro_name == "protein":
        if value >= 80:
            return "high"
        if value >= 25:
            return "medium"
        if value > 5:
            return "low"
        return "minimal"
    if macro_name == "carbs":
        if value >= 120:
            return "high"
        if value >= 40:
            return "medium"
        if value > 10:
            return "low"
        return "minimal"
    if value >= 60:
        return "high"
    if value >= 20:
        return "medium"
    if value > 5:
        return "low"
    return "minimal"


def suggested_blocker_action(name: str, role: str, statuses: Counter[str], grams: float) -> str:
    normalized = normalize_text(name)
    token_set = tokens(normalized)
    if "vinegar" in token_set:
        return "keep_deferred"
    if normalized in {"salt", "black pepper"} or role in {"seasoning", "water", "acid"} and grams < 300:
        return "keep_deferred"
    if "unmapped" in statuses and role in {"protein", "carb", "fat", "dairy", "veg"}:
        if token_set & {"pork", "beef", "turkey", "chicken"}:
            return "manual_mapping_or_fooddb_item"
        return "mapping_fix_or_fooddb_item"
    if "review_needed" in statuses:
        return "manual_review_then_mapping_fix"
    return "mapping_fix"


def build_remaining_blockers(
    mapping_rows: list[dict[str, str]],
    parsed_by_key: dict[tuple[str, str], dict[str, str]],
    food_by_id: dict[str, dict[str, str]],
    food_by_name: dict[str, dict[str, str]],
) -> tuple[list[dict[str, object]], dict[str, dict[str, float]], dict[str, list[str]]]:
    grouped: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "grams": 0.0,
            "rows": 0,
            "recipes": set(),
            "recipe_names": [],
            "raw_texts": [],
            "statuses": Counter(),
            "food_ids": Counter(),
            "estimate_sources": Counter(),
            "roles": Counter(),
            "kcal": 0.0,
            "protein": 0.0,
            "carbs": 0.0,
            "fat": 0.0,
        }
    )
    recipe_impact: dict[str, dict[str, float]] = defaultdict(lambda: defaultdict(float))
    recipe_blockers: dict[str, list[str]] = defaultdict(list)

    for row in mapping_rows:
        status = clean_text(row.get("mapping_status"))
        if status == "accepted_auto":
            continue
        grams = num(row.get("quantity_grams_estimated"))
        if grams <= 0:
            continue
        key = (clean_text(row.get("recipe_id_candidate")), clean_text(row.get("ingredient_position")))
        parsed_row = parsed_by_key.get(key)
        role, estimate_source, macros_100g = estimate_macros_for_unresolved(row, parsed_row, food_by_id, food_by_name)
        kcal, protein, carbs, fat = (grams * value / 100.0 for value in macros_100g)
        name = clean_text(row.get("ingredient_name_normalized")) or clean_text(row.get("ingredient_name_parsed"))
        bucket = grouped[name]
        bucket["grams"] = float(bucket["grams"]) + grams
        bucket["rows"] = int(bucket["rows"]) + 1
        bucket["recipes"].add(clean_text(row.get("recipe_id_candidate")))
        bucket["recipe_names"].append(clean_text(row.get("display_name")))
        bucket["raw_texts"].append(clean_text(row.get("ingredient_raw_text")))
        bucket["statuses"][status] += 1
        if clean_text(row.get("mapped_food_id")):
            bucket["food_ids"][clean_text(row.get("mapped_food_id"))] += 1
        bucket["estimate_sources"][estimate_source] += 1
        bucket["roles"][role] += 1
        bucket["kcal"] = float(bucket["kcal"]) + kcal
        bucket["protein"] = float(bucket["protein"]) + protein
        bucket["carbs"] = float(bucket["carbs"]) + carbs
        bucket["fat"] = float(bucket["fat"]) + fat

        recipe_id = clean_text(row.get("recipe_id_candidate"))
        recipe_impact[recipe_id]["kcal"] += kcal
        recipe_impact[recipe_id]["protein"] += protein
        recipe_impact[recipe_id]["carbs"] += carbs
        recipe_impact[recipe_id]["fat"] += fat
        recipe_impact[recipe_id]["grams"] += grams
        recipe_blockers[recipe_id].append(f"{name}:{format_number(grams)}g:{role}")

    rows: list[dict[str, object]] = []
    for name, bucket in grouped.items():
        grams = float(bucket["grams"])
        kcal = float(bucket["kcal"])
        protein = float(bucket["protein"])
        carbs = float(bucket["carbs"])
        fat = float(bucket["fat"])
        role = bucket["roles"].most_common(1)[0][0] if bucket["roles"] else "unknown"
        score = kcal + protein * 25.0 + carbs * 2.0 + fat * 5.0
        statuses = bucket["statuses"]
        rows.append(
            {
                "ingredient_name_normalized": name,
                "likely_role": role,
                "total_grams": format_number(grams),
                "row_count": bucket["rows"],
                "affected_recipe_count": len(bucket["recipes"]),
                "affected_recipes": clipped(bucket["recipe_names"], 6),
                "example_raw_texts": clipped(bucket["raw_texts"], 6),
                "mapping_statuses": format_counter(statuses),
                "mapped_food_ids_seen": format_counter(bucket["food_ids"]),
                "estimate_source": format_counter(bucket["estimate_sources"], 4),
                "estimated_lost_kcal": format_number(kcal),
                "estimated_lost_protein_g": format_number(protein),
                "estimated_lost_carbs_g": format_number(carbs),
                "estimated_lost_fat_g": format_number(fat),
                "estimated_macro_impact_score": format_number(score),
                "likely_lost_kcal_class": macro_class(kcal, "kcal"),
                "likely_lost_protein_class": macro_class(protein, "protein"),
                "likely_lost_carbs_class": macro_class(carbs, "carbs"),
                "likely_lost_fat_class": macro_class(fat, "fat"),
                "suggested_next_action": suggested_blocker_action(name, role, statuses, grams),
                "notes": "estimated_from_review_match_or_role_heuristic",
            }
        )

    rows.sort(
        key=lambda row: (
            num(row.get("estimated_macro_impact_score")),
            num(row.get("estimated_lost_kcal")),
            num(row.get("total_grams")),
        ),
        reverse=True,
    )
    return rows, recipe_impact, recipe_blockers


def is_main_like(kind: str) -> bool:
    return kind in MAIN_LIKE_KINDS


def good_enough_thresholds(kind: str) -> tuple[float, float, float]:
    if kind in MAIN_LIKE_KINDS:
        return 0.6, 300.0, 20.0
    if kind == "breakfast":
        return 0.55, 250.0, 8.0
    if kind == "snack":
        return 0.45, 100.0, 0.0
    if kind in COMPONENT_KINDS:
        return 0.5, 30.0, 0.0
    return 0.55, 200.0, 5.0


def low_macro_thresholds(kind: str) -> tuple[float, float]:
    if kind in MAIN_LIKE_KINDS:
        return 250.0, 10.0
    if kind == "breakfast":
        return 180.0, 5.0
    if kind == "snack":
        return 80.0, 0.0
    if kind in COMPONENT_KINDS:
        return 30.0, 0.0
    return 150.0, 5.0


def target_serving_weight(kind: str) -> float:
    if kind == "soup":
        return 350.0
    if kind == "salad":
        return 280.0
    if kind in {"complete_main", "near_complete_main"}:
        return 350.0
    if kind == "breakfast":
        return 250.0
    if kind == "snack":
        return 100.0
    if kind in {"carb_side", "veg_side"}:
        return 150.0
    if kind == "protein_component":
        return 120.0
    return 250.0


def classify_recipe(
    cache_row: dict[str, str],
    recipe_impact: dict[str, float],
    recipe_blockers: list[str],
) -> tuple[str, list[str], str]:
    kind = clean_text(cache_row.get("recipe_kind_guess"))
    cache_status = clean_text(cache_row.get("cache_status"))
    mapped_ratio = num(cache_row.get("mapped_weight_ratio"))
    macro_ratio = num(cache_row.get("macro_relevant_mapped_weight_ratio"))
    kcal_per = num(cache_row.get("energy_kcal_per_serving"))
    protein_per = num(cache_row.get("protein_g_per_serving"))
    kcal_total = num(cache_row.get("energy_kcal_total"))
    protein_total = num(cache_row.get("protein_g_total"))
    known_weight = num(cache_row.get("known_weight_grams_sum"))
    mapped_weight = num(cache_row.get("mapped_weight_grams"))
    low_or_no_weight = num(cache_row.get("low_or_no_macro_mapped_weight_grams"))
    servings = num(cache_row.get("servings_basis"))
    review_grams = num(cache_row.get("review_needed_with_grams_count"))
    unmapped_grams = num(cache_row.get("unmapped_with_grams_count"))
    unresolved_impact = recipe_impact.get("kcal", 0.0)
    unresolved_protein = recipe_impact.get("protein", 0.0)
    serving_weight = known_weight / servings if servings > 0 else 0.0
    low_no_share = low_or_no_weight / mapped_weight if mapped_weight > 0 else 0.0

    good_ratio, good_kcal, good_protein = good_enough_thresholds(kind)
    low_kcal, low_protein = low_macro_thresholds(kind)
    low_macro = kcal_per < low_kcal and (protein_per < low_protein if low_protein > 0 else True)
    strong_main_low_macro = is_main_like(kind) and kcal_per < 250 and protein_per < 10
    high_coverage = mapped_ratio >= 0.8 and macro_ratio >= 0.65
    low_coverage = mapped_ratio < 0.65 or macro_ratio < 0.5
    has_unresolved_grams = review_grams > 0 or unmapped_grams > 0
    likely_servings_issue = (
        servings >= 4
        and macro_ratio >= 0.5
        and serving_weight > 0
        and serving_weight < target_serving_weight(kind) * 0.65
        and (kcal_total >= 700 or protein_total >= 35)
        and (kcal_per < good_kcal or protein_per < max(good_protein, 8.0))
    )
    missing_key_macro = unresolved_impact >= 300 or unresolved_protein >= 20 or bool(
        recipe_blockers and any(tokens(item) & DEFERRED_KEY_MACRO_WORDS for item in recipe_blockers)
    )

    reasons: list[str] = []
    if cache_status == "usable_from_mapped_ingredients" and macro_ratio >= good_ratio and kcal_per >= good_kcal and protein_per >= good_protein:
        return "good_enough_for_generator", ["meets_kind_specific_macro_and_coverage_thresholds"], "candidate_for_subset_materialization"

    if likely_servings_issue:
        reasons.extend(
            [
                f"known_weight_per_serving_low:{format_number(serving_weight)}g",
                f"servings_basis:{format_number(servings)}",
                "total_macros_plausible_but_per_serving_low",
            ]
        )
        return "likely_servings_overestimated", reasons, "servings_adjustment"

    if low_coverage and has_unresolved_grams:
        reasons.extend(
            [
                f"mapped_weight_ratio:{format_number(mapped_ratio)}",
                f"macro_relevant_ratio:{format_number(macro_ratio)}",
                "unresolved_weight_with_grams_present",
            ]
        )
        if strong_main_low_macro:
            reasons.append("complete_or_near_complete_main_kcal_lt_250_and_protein_lt_10")
        return "low_macro_due_to_low_mapping_coverage", reasons, "targeted_mapping_pass"

    if high_coverage and low_macro:
        reasons.extend(
            [
                f"mapped_weight_ratio:{format_number(mapped_ratio)}",
                f"macro_relevant_ratio:{format_number(macro_ratio)}",
                "coverage_good_but_per_serving_macros_low",
            ]
        )
        if missing_key_macro:
            reasons.append("unresolved_key_macro_ingredient_likely")
            return "likely_missing_key_macro_ingredient", reasons, "targeted_mapping_pass"
        return "low_macro_despite_good_weight_coverage", reasons, "servings_or_recipe_review"

    if missing_key_macro and low_macro:
        reasons.extend(["low_macro_with_estimated_unresolved_macro_impact", f"estimated_unresolved_kcal:{format_number(unresolved_impact)}"])
        return "likely_missing_key_macro_ingredient", reasons, "targeted_mapping_pass"

    if low_no_share >= 0.5 and low_macro:
        reasons.extend(["mapped_weight_inflated_by_low_or_no_macro_items", f"low_no_macro_share:{format_number(low_no_share)}"])
        return "likely_weak_recipe_for_generator", reasons, "recipe_review_or_replacement"

    if strong_main_low_macro:
        reasons.append("complete_or_near_complete_main_kcal_lt_250_and_protein_lt_10")
        return "likely_weak_recipe_for_generator", reasons, "recipe_review_or_replacement"

    reasons.append("mixed_signals_require_manual_review")
    return "needs_manual_review", reasons, "manual_review"


def rough_servings_by_weight(kind: str, known_weight: float) -> float:
    target = target_serving_weight(kind)
    if known_weight <= 0 or target <= 0:
        return 0.0
    return max(1.0, round(known_weight / target, 1))


def build_diagnostics(
    curated_rows: list[dict[str, str]],
    cache_rows: list[dict[str, str]],
    recipe_impact: dict[str, dict[str, float]],
    recipe_blockers: dict[str, list[str]],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    curated_by_id = {clean_text(row.get("recipe_id_candidate")): row for row in curated_rows}
    diagnostics: list[dict[str, object]] = []
    servings_review: list[dict[str, object]] = []
    replacement_candidates: list[dict[str, object]] = []

    for cache_row in cache_rows:
        recipe_id = clean_text(cache_row.get("recipe_id_candidate"))
        curated = curated_by_id.get(recipe_id, {})
        impact = recipe_impact.get(recipe_id, {})
        blockers = recipe_blockers.get(recipe_id, [])
        diagnosis, reasons, suggested_action = classify_recipe(cache_row, impact, blockers)
        kind = clean_text(cache_row.get("recipe_kind_guess")) or clean_text(curated.get("recipe_kind_guess"))
        servings = num(cache_row.get("servings_basis"))
        known_weight = num(cache_row.get("known_weight_grams_sum"))
        mapped_weight = num(cache_row.get("mapped_weight_grams"))
        low_or_no_weight = num(cache_row.get("low_or_no_macro_mapped_weight_grams"))
        serving_weight = known_weight / servings if servings > 0 else 0.0
        low_no_share = low_or_no_weight / mapped_weight if mapped_weight > 0 else 0.0
        top_blockers = clipped(blockers, 5)
        row = {
            "recipe_id_candidate": recipe_id,
            "display_name": clean_text(cache_row.get("display_name")) or clean_text(curated.get("display_name")),
            "recipe_kind_guess": kind,
            "primary_protein": clean_text(cache_row.get("primary_protein")) or clean_text(curated.get("primary_protein")),
            "cache_status": clean_text(cache_row.get("cache_status")),
            "servings_basis": clean_text(cache_row.get("servings_basis")),
            "servings_estimation_method": clean_text(cache_row.get("servings_estimation_method")),
            "mapped_weight_ratio": clean_text(cache_row.get("mapped_weight_ratio")),
            "macro_relevant_mapped_weight_ratio": clean_text(cache_row.get("macro_relevant_mapped_weight_ratio")),
            "energy_kcal_total": clean_text(cache_row.get("energy_kcal_total")),
            "protein_g_total": clean_text(cache_row.get("protein_g_total")),
            "carbs_g_total": clean_text(cache_row.get("carbs_g_total")),
            "fat_g_total": clean_text(cache_row.get("fat_g_total")),
            "energy_kcal_per_serving": clean_text(cache_row.get("energy_kcal_per_serving")),
            "protein_g_per_serving": clean_text(cache_row.get("protein_g_per_serving")),
            "carbs_g_per_serving": clean_text(cache_row.get("carbs_g_per_serving")),
            "fat_g_per_serving": clean_text(cache_row.get("fat_g_per_serving")),
            "accepted_auto_with_grams_count": clean_text(cache_row.get("accepted_auto_with_grams_count")),
            "review_needed_with_grams_count": clean_text(cache_row.get("review_needed_with_grams_count")),
            "unmapped_with_grams_count": clean_text(cache_row.get("unmapped_with_grams_count")),
            "known_weight_grams_sum": clean_text(cache_row.get("known_weight_grams_sum")),
            "mapped_weight_grams": clean_text(cache_row.get("mapped_weight_grams")),
            "macro_relevant_mapped_weight_grams": clean_text(cache_row.get("macro_relevant_mapped_weight_grams")),
            "low_or_no_macro_mapped_weight_grams": clean_text(cache_row.get("low_or_no_macro_mapped_weight_grams")),
            "known_weight_per_serving_grams": format_number(serving_weight),
            "low_or_no_macro_mapped_weight_share": format_number(low_no_share),
            "estimated_unresolved_kcal": format_number(impact.get("kcal", 0.0)),
            "estimated_unresolved_protein_g": format_number(impact.get("protein", 0.0)),
            "estimated_unresolved_carbs_g": format_number(impact.get("carbs", 0.0)),
            "estimated_unresolved_fat_g": format_number(impact.get("fat", 0.0)),
            "top_unresolved_macro_blockers": top_blockers,
            "main_diagnosis": diagnosis,
            "diagnosis_reasons": "; ".join(reasons),
            "suggested_next_action": suggested_action,
        }
        diagnostics.append(row)

        if should_review_servings(row):
            servings_review.append(build_servings_row(row))

        replacement_score, replacement_reason = replacement_candidate_score(row)
        if replacement_score > 0:
            replacement = {column: row.get(column, "") for column in REPLACEMENT_COLUMNS if column in row}
            replacement["replacement_score"] = format_number(replacement_score)
            replacement["candidate_reason"] = replacement_reason
            replacement["suggested_next_action"] = replacement_suggestion(row)
            replacement_candidates.append(replacement)

    diagnostics.sort(key=lambda row: clean_text(row.get("recipe_id_candidate")))
    servings_review.sort(key=lambda row: num(row.get("servings_review_score")), reverse=True)
    replacement_candidates.sort(key=lambda row: num(row.get("replacement_score")), reverse=True)
    return diagnostics, servings_review, replacement_candidates


def should_review_servings(row: dict[str, object]) -> bool:
    kind = clean_text(row.get("recipe_kind_guess"))
    servings = num(row.get("servings_basis"))
    known_weight = num(row.get("known_weight_grams_sum"))
    serving_weight = num(row.get("known_weight_per_serving_grams"))
    kcal_total = num(row.get("energy_kcal_total"))
    protein_total = num(row.get("protein_g_total"))
    kcal_per = num(row.get("energy_kcal_per_serving"))
    protein_per = num(row.get("protein_g_per_serving"))
    macro_ratio = num(row.get("macro_relevant_mapped_weight_ratio"))
    if servings <= 1 or known_weight <= 0:
        return False
    target = target_serving_weight(kind)
    total_plausible = kcal_total >= 600 or protein_total >= 35
    per_serving_low = kcal_per < good_enough_thresholds(kind)[1] or protein_per < max(good_enough_thresholds(kind)[2], 8.0)
    return macro_ratio >= 0.45 and total_plausible and per_serving_low and serving_weight < target * 0.8


def build_servings_row(row: dict[str, object]) -> dict[str, object]:
    kind = clean_text(row.get("recipe_kind_guess"))
    serving_weight = num(row.get("known_weight_per_serving_grams"))
    target = target_serving_weight(kind)
    gap = max(0.0, target - serving_weight)
    kcal_gap = max(0.0, good_enough_thresholds(kind)[1] - num(row.get("energy_kcal_per_serving")))
    protein_gap = max(0.0, good_enough_thresholds(kind)[2] - num(row.get("protein_g_per_serving")))
    score = gap + kcal_gap + protein_gap * 20.0
    issue_type = "low_known_weight_per_serving"
    if num(row.get("energy_kcal_total")) >= 700 and num(row.get("energy_kcal_per_serving")) < good_enough_thresholds(kind)[1]:
        issue_type = "total_kcal_plausible_but_per_serving_low"
    if num(row.get("protein_g_total")) >= 35 and num(row.get("protein_g_per_serving")) < max(good_enough_thresholds(kind)[2], 8.0):
        issue_type = "total_protein_plausible_but_per_serving_low"
    return {
        "recipe_id_candidate": row.get("recipe_id_candidate", ""),
        "display_name": row.get("display_name", ""),
        "recipe_kind_guess": kind,
        "primary_protein": row.get("primary_protein", ""),
        "servings_basis": row.get("servings_basis", ""),
        "servings_estimation_method": row.get("servings_estimation_method", ""),
        "known_weight_grams_sum": row.get("known_weight_grams_sum", ""),
        "known_weight_per_serving_grams": row.get("known_weight_per_serving_grams", ""),
        "energy_kcal_total": row.get("energy_kcal_total", ""),
        "protein_g_total": row.get("protein_g_total", ""),
        "energy_kcal_per_serving": row.get("energy_kcal_per_serving", ""),
        "protein_g_per_serving": row.get("protein_g_per_serving", ""),
        "mapped_weight_ratio": row.get("mapped_weight_ratio", ""),
        "macro_relevant_mapped_weight_ratio": row.get("macro_relevant_mapped_weight_ratio", ""),
        "servings_issue_type": issue_type,
        "servings_review_score": format_number(score),
        "rough_servings_by_weight": format_number(rough_servings_by_weight(kind, num(row.get("known_weight_grams_sum")))),
        "review_notes": "pilot_fallback_servings_needs_manual_check",
    }


def replacement_candidate_score(row: dict[str, object]) -> tuple[float, str]:
    diagnosis = clean_text(row.get("main_diagnosis"))
    kind = clean_text(row.get("recipe_kind_guess"))
    mapped_ratio = num(row.get("mapped_weight_ratio"))
    macro_ratio = num(row.get("macro_relevant_mapped_weight_ratio"))
    kcal_per = num(row.get("energy_kcal_per_serving"))
    protein_per = num(row.get("protein_g_per_serving"))
    unresolved_kcal = num(row.get("estimated_unresolved_kcal"))
    cache_status = clean_text(row.get("cache_status"))
    score = 0.0
    reasons: list[str] = []
    if cache_status in {"no_accepted_mapped_ingredients", "mapped_without_weight_estimates"}:
        score += 40
        reasons.append(cache_status)
    if diagnosis in {"likely_weak_recipe_for_generator", "low_macro_despite_good_weight_coverage"}:
        score += 35
        reasons.append(diagnosis)
    if is_main_like(kind) and kcal_per < 250 and protein_per < 10:
        score += 25
        reasons.append("main_like_low_kcal_low_protein")
    if mapped_ratio < 0.3 or macro_ratio < 0.2:
        score += 20
        reasons.append("very_low_coverage")
    if unresolved_kcal >= 800:
        score -= 15
        reasons.append("has_fixable_macro_blockers")
    if diagnosis == "likely_servings_overestimated":
        score -= 25
        reasons.append("prefer_servings_review_before_replacement")
    if score < 30:
        return 0.0, ""
    return score, "; ".join(reasons)


def replacement_suggestion(row: dict[str, object]) -> str:
    diagnosis = clean_text(row.get("main_diagnosis"))
    if diagnosis == "low_macro_due_to_low_mapping_coverage":
        return "try_targeted_mapping_before_replacement"
    if diagnosis == "likely_servings_overestimated":
        return "servings_adjustment_before_replacement"
    return "consider_recipe_review_or_replacement"


def median(values: list[float]) -> float | None:
    clean = sorted(value for value in values if value is not None)
    if not clean:
        return None
    mid = len(clean) // 2
    if len(clean) % 2:
        return clean[mid]
    return (clean[mid - 1] + clean[mid]) / 2


def choose_recommendation(
    diagnosis_counts: Counter[str],
    servings_count: int,
    replacement_count: int,
    blocker_rows: list[dict[str, object]],
) -> str:
    mapping_failures = diagnosis_counts["low_macro_due_to_low_mapping_coverage"] + diagnosis_counts["likely_missing_key_macro_ingredient"]
    weak_or_replace = diagnosis_counts["likely_weak_recipe_for_generator"] + diagnosis_counts["low_macro_despite_good_weight_coverage"]
    high_impact_blockers = sum(1 for row in blocker_rows[:20] if clean_text(row.get("suggested_next_action")) in {"mapping_fix_or_fooddb_item", "manual_mapping_or_fooddb_item", "manual_review_then_mapping_fix"})
    if mapping_failures >= servings_count and high_impact_blockers >= 8:
        return "A. targeted mapping pass"
    if servings_count > mapping_failures and servings_count >= 20:
        return "B. servings adjustment"
    if replacement_count >= 25 or weak_or_replace >= 25:
        return "C. recipe replacement"
    if diagnosis_counts["good_enough_for_generator"] >= 120:
        return "D. materialize a subset"
    if diagnosis_counts["good_enough_for_generator"] >= 180:
        return "E. materialize full v1.1"
    return "A. targeted mapping pass"


def write_summary(
    path: Path,
    diagnostics: list[dict[str, object]],
    servings_review: list[dict[str, object]],
    replacement_candidates: list[dict[str, object]],
    blockers: list[dict[str, object]],
) -> str:
    diagnosis_counts = Counter(clean_text(row.get("main_diagnosis")) for row in diagnostics)
    action_counts = Counter(clean_text(row.get("suggested_next_action")) for row in diagnostics)
    recommendation = choose_recommendation(diagnosis_counts, len(servings_review), len(replacement_candidates), blockers)
    good_count = diagnosis_counts["good_enough_for_generator"]
    coverage_fail_count = diagnosis_counts["low_macro_due_to_low_mapping_coverage"]
    good_coverage_fail_count = (
        diagnosis_counts["low_macro_despite_good_weight_coverage"]
        + diagnosis_counts["likely_missing_key_macro_ingredient"]
    )
    servings_count = diagnosis_counts["likely_servings_overestimated"]
    replacement_count = len(replacement_candidates)
    macro_ratios = [num(row.get("macro_relevant_mapped_weight_ratio")) for row in diagnostics]
    kcal_values = [num(row.get("energy_kcal_per_serving")) for row in diagnostics]
    protein_values = [num(row.get("protein_g_per_serving")) for row in diagnostics]

    lines = [
        "Recipes_DB v1.1 round6 low-macro diagnostics summary",
        "======================================================",
        "",
        f"Total recipes audited: {len(diagnostics)}",
        "",
        "Diagnosis counts:",
    ]
    for name, count in diagnosis_counts.most_common():
        lines.append(f"- {name}: {count}")
    lines.extend(
        [
            "",
            "Suggested recipe-level next actions:",
        ]
    )
    for name, count in action_counts.most_common():
        lines.append(f"- {name}: {count}")
    lines.extend(
        [
            "",
            f"Good enough for generator: {good_count}",
            f"Fail mostly due to mapping coverage: {coverage_fail_count}",
            f"Fail despite good/usable coverage or missing key macro ingredient: {good_coverage_fail_count}",
            f"Likely servings overestimated: {servings_count}",
            f"Replacement candidates: {replacement_count}",
            f"Servings review rows: {len(servings_review)}",
            f"Median macro_relevant_mapped_weight_ratio: {format_number(median(macro_ratios))}",
            f"Median kcal/serving: {format_number(median(kcal_values))}",
            f"Median protein/serving: {format_number(median(protein_values))}",
            "",
            "Top servings issues:",
        ]
    )
    for row in servings_review[:10]:
        lines.append(
            "- "
            f"{row['recipe_id_candidate']} | {row['display_name']} | "
            f"servings={row['servings_basis']} | "
            f"known_weight_per_serving={row['known_weight_per_serving_grams']}g | "
            f"kcal={row['energy_kcal_per_serving']} | protein={row['protein_g_per_serving']} | "
            f"{row['servings_issue_type']}"
        )
    lines.append("")
    lines.append("Top replacement candidates:")
    for row in replacement_candidates[:10]:
        lines.append(
            "- "
            f"{row['recipe_id_candidate']} | {row['display_name']} | "
            f"score={row['replacement_score']} | {row['main_diagnosis']} | "
            f"kcal={row['energy_kcal_per_serving']} | protein={row['protein_g_per_serving']}"
        )
    lines.append("")
    lines.append("Top 20 remaining blockers by likely nutrition impact:")
    for row in blockers[:20]:
        lines.append(
            "- "
            f"{row['ingredient_name_normalized']} | role={row['likely_role']} | "
            f"grams={row['total_grams']} | est_kcal={row['estimated_lost_kcal']} | "
            f"est_protein={row['estimated_lost_protein_g']} | action={row['suggested_next_action']}"
        )
    lines.extend(
        [
            "",
            f"Recommendation: {recommendation}",
        ]
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return recommendation


def run(args: argparse.Namespace) -> None:
    curated_rows = read_csv(args.curated_path)
    cache_rows = read_csv(args.cache_path)
    mapping_rows = read_csv(args.mapping_path)
    parsed_rows = read_csv(args.parsed_path)
    food_rows = read_csv(args.fooddb_path)
    read_csv(args.contributions_path)

    parsed_by_key = {
        (clean_text(row.get("recipe_id_candidate")), clean_text(row.get("ingredient_position"))): row
        for row in parsed_rows
    }
    food_by_id, food_by_name = build_food_lookup(food_rows)
    blockers, recipe_impact, recipe_blockers = build_remaining_blockers(
        mapping_rows,
        parsed_by_key,
        food_by_id,
        food_by_name,
    )
    diagnostics, servings_review, replacement_candidates = build_diagnostics(
        curated_rows,
        cache_rows,
        recipe_impact,
        recipe_blockers,
    )

    write_csv(OUT_DIAGNOSTICS, diagnostics, DIAGNOSTIC_COLUMNS)
    write_csv(OUT_SERVINGS_REVIEW, servings_review, SERVINGS_COLUMNS)
    write_csv(OUT_REPLACEMENT_CANDIDATES, replacement_candidates, REPLACEMENT_COLUMNS)
    write_csv(OUT_REMAINING_BLOCKERS, blockers, BLOCKER_COLUMNS)
    recommendation = write_summary(OUT_SUMMARY, diagnostics, servings_review, replacement_candidates, blockers)

    diagnosis_counts = Counter(clean_text(row.get("main_diagnosis")) for row in diagnostics)
    print("Recipes_DB v1.1 round6 low-macro diagnostics built")
    print(f"total_recipes={len(diagnostics)}")
    print(f"good_enough_for_generator={diagnosis_counts['good_enough_for_generator']}")
    print(f"low_macro_due_to_low_mapping_coverage={diagnosis_counts['low_macro_due_to_low_mapping_coverage']}")
    print(f"likely_servings_overestimated={diagnosis_counts['likely_servings_overestimated']}")
    print(f"replacement_candidates={len(replacement_candidates)}")
    print(f"top_blockers={len(blockers)}")
    print(f"recommendation={recommendation}")
    print(f"written={OUT_DIAGNOSTICS}")
    print(f"written={OUT_SUMMARY}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit Recipes_DB v1.1 round6 low macro diagnostics after round5."
    )
    parser.add_argument("--curated_path", type=Path, default=DEFAULT_CURATED)
    parser.add_argument("--cache_path", type=Path, default=DEFAULT_CACHE)
    parser.add_argument("--contributions_path", type=Path, default=DEFAULT_CONTRIBUTIONS)
    parser.add_argument("--mapping_path", type=Path, default=DEFAULT_MAPPING)
    parser.add_argument("--parsed_path", type=Path, default=DEFAULT_PARSED)
    parser.add_argument("--fooddb_path", type=Path, default=DEFAULT_FOODDB)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())

