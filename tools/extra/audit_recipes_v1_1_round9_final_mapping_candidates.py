from __future__ import annotations

import csv
import math
import re
from collections import Counter, defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

ROUND8_CACHE = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_nutrition_cache_draft_round8.csv"
ROUND8_CONTRIBUTIONS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_ingredient_nutrition_contributions_round8.csv"
)
ROUND8_MAPPING = (
    REPO_ROOT
    / "data"
    / "recipesdb"
    / "draft"
    / "recipes_v1_1_ingredient_food_matches_draft_fooddb_v1_1_round8_unit_rules_punctual_mapping.csv"
)
ROUND8_INGREDIENTS = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_ingredients_parsed_unit_rules_round8.csv"
ROUND8_FOODDB = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round8.csv"

OUT_CANDIDATES = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round9_final_mapping_candidates.csv"
OUT_SUMMARY = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round9_final_mapping_summary.txt"


OUTPUT_COLUMNS = [
    "classification",
    "ingredient_name_normalized",
    "total_grams_affected",
    "affected_recipe_count",
    "row_count",
    "example_raw_texts",
    "example_recipes",
    "macro_role",
    "possible_fooddb_match",
    "possible_fooddb_id",
    "source_macro_available",
    "safety",
    "recommended_action",
    "risk_notes",
]


ROUND9_SAFE_FIXES = {
    "whole wheat flour": {
        "possible_fooddb_match": "Wheat flour, whole-grain, soft wheat",
        "possible_fooddb_id": "food_wheat_flour_whole_grain_soft_wheat",
        "source_macro_available": "yes_usda_168944",
        "safety": "safe_exact_source_macro_sane",
        "recommended_action": "add_fooddb_source_row_and_map_exact_whole_wheat_flour",
        "risk_notes": "Exact flour source with plausible kcal/macros; carb blocker.",
    },
    "salted cod fish": {
        "possible_fooddb_match": "Fish, cod, Atlantic, dried and salted",
        "possible_fooddb_id": "food_cod_atlantic_dried_and_salted",
        "source_macro_available": "yes_usda_174190",
        "safety": "safe_exact_source_macro_sane",
        "recommended_action": "add_fooddb_source_row_and_map_only_salted_cod_fish",
        "risk_notes": "Do not map generic cod here; only salted cod raw text is accepted.",
    },
    "morel mushrooms": {
        "possible_fooddb_match": "Morel, raw",
        "possible_fooddb_id": "food_morel_raw",
        "source_macro_available": "existing_fooddb_round8",
        "safety": "safe_exact_existing_fooddb",
        "recommended_action": "map_exact_fresh_morel_mushrooms_to_morel_raw",
        "risk_notes": "Low macro priority but exact mushroom state is clear.",
    },
    "shiitake mushrooms": {
        "possible_fooddb_match": "Shiitake mushroom, dried",
        "possible_fooddb_id": "food_shiitake_mushroom_dried",
        "source_macro_available": "existing_fooddb_round8",
        "safety": "safe_exact_existing_fooddb",
        "recommended_action": "map_only_dried_shiitake_raw_text_to_dried_shiitake",
        "risk_notes": "Exact only because raw text says dried shiitake mushrooms.",
    },
    "sirloin steak": {
        "possible_fooddb_match": "Beef, sirloin steak, raw",
        "possible_fooddb_id": "food_beef_sirloin_steak_raw",
        "source_macro_available": "existing_fooddb_round8",
        "safety": "safe_exact_existing_fooddb",
        "recommended_action": "map_exact_sirloin_steak_to_raw_sirloin_steak",
        "risk_notes": "Specific beef cut, not generic beef.",
    },
    "chicken meat": {
        "possible_fooddb_match": "Chicken, meat, raw",
        "possible_fooddb_id": "food_chicken_meat_raw",
        "source_macro_available": "existing_fooddb_round8",
        "safety": "safe_exact_existing_fooddb",
        "recommended_action": "map_exact_chicken_meat_to_raw_chicken_meat",
        "risk_notes": "Specific chicken meat row with grams; no count/yield rule is introduced.",
    },
    "ricotta cheese": {
        "possible_fooddb_match": "Ricotta cheese",
        "possible_fooddb_id": "food_ricotta_cheese",
        "source_macro_available": "existing_fooddb_round8",
        "safety": "safe_exact_existing_fooddb",
        "recommended_action": "map_exact_ricotta_cheese_when_grams_exist",
        "risk_notes": "No new ricotta cup rule in this pass.",
    },
    "crabmeat": {
        "possible_fooddb_match": "Crab, raw",
        "possible_fooddb_id": "food_crab_raw",
        "source_macro_available": "existing_fooddb_round8",
        "safety": "safe_exact_existing_fooddb",
        "recommended_action": "map_fresh_crabmeat_to_raw_crab",
        "risk_notes": "Specific seafood ingredient; imitation crabmeat remains composed/deferred.",
    },
}

ALL_PURPOSE_FIX = {
    "possible_fooddb_match": "Wheat flour, white, all-purpose, enriched, unbleached",
    "possible_fooddb_id": "food_wheat_flour_white_all_purpose_enriched_unbleached",
    "source_macro_available": "yes_usda_168936",
    "safety": "safe_exact_source_macro_sane",
    "recommended_action": "add_fooddb_source_row_and_map_exact_all_purpose_flour_variants",
    "risk_notes": "Only raw text/name variants that explicitly say all-purpose flour are accepted.",
}

EXPLICIT_DEFERRED = {
    "pork neck bones": "needs bone-specific edible yield and source decision",
    "pork": "generic pork remains too broad for global mapping",
    "beef": "generic beef remains too broad for global mapping",
    "turkey": "generic turkey remains too broad for global mapping",
    "spinach pasta dough": "prepared dough/composed ingredient, not Food_DB atomic item",
    "tomato sauce": "source energy remains suspect from previous rounds",
    "rice vinegar": "vinegar variant, not rice; low macro priority",
    "rice wine vinegar": "vinegar variant, not rice; low macro priority",
    "chinese rice vinegar": "vinegar variant, not rice; low macro priority",
    "rice wine": "wine/vinegar-style variant, not rice grain",
}

COMPOSED_TOKENS = {
    "adobo",
    "dough",
    "ravioli",
    "cracker",
    "crackers",
    "tortilla",
    "tortillas",
    "wrappers",
    "wrapper",
    "vermicelli",
    "noodles",
    "bouillon",
    "base",
    "stock",
    "broth",
    "guacamole",
}


def read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"Lipseste fisierul asteptat: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return list(reader), list(reader.fieldnames or [])


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def clean_text(value: object) -> str:
    return str(value or "").strip()


def parse_float(value: object) -> float | None:
    text = clean_text(value)
    if not text:
        return None
    try:
        parsed = float(text)
    except ValueError:
        return None
    if math.isnan(parsed) or math.isinf(parsed):
        return None
    return parsed


def format_number(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.4f}".rstrip("0").rstrip(".")


def normalize_name(value: object) -> str:
    text = clean_text(value).casefold()
    text = text.replace("-", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def has_grams(row: dict[str, str]) -> bool:
    grams = parse_float(row.get("quantity_grams_estimated"))
    return grams is not None and grams > 0


def macro_role(name: str) -> str:
    text = normalize_name(name)
    if any(token in text for token in ("cod", "fish", "beef", "chicken", "pork", "turkey", "crab", "scallop")):
        return "protein"
    if any(token in text for token in ("flour", "rice", "lentil", "beans", "pasta", "oats")):
        return "carb"
    if any(token in text for token in ("cheese", "mayonnaise", "oil", "butter")):
        return "fat"
    if any(token in text for token in ("tomatillo", "mushroom", "asparagus", "broccoli", "spinach")):
        return "veg"
    if any(token in text for token in ("vinegar", "sauce", "broth", "stock", "salt", "pepper")):
        return "low_macro"
    return "unknown"


def collect_groups(mapping_rows: list[dict[str, str]]) -> dict[str, dict[str, object]]:
    groups: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "ingredient_name_normalized": "",
            "total_grams_affected": 0.0,
            "row_count": 0,
            "recipe_ids": set(),
            "raw_texts": [],
            "recipes": [],
            "statuses": Counter(),
            "rows": [],
        }
    )
    for row in mapping_rows:
        status = clean_text(row.get("mapping_status"))
        name = clean_text(row.get("ingredient_name_normalized"))
        if not name or status == "accepted_auto":
            continue
        grams = parse_float(row.get("quantity_grams_estimated")) or 0.0
        include_without_grams = name in EXPLICIT_DEFERRED or any(token in normalize_name(name) for token in COMPOSED_TOKENS)
        if grams <= 0 and not include_without_grams:
            continue

        item = groups[name]
        item["ingredient_name_normalized"] = name
        item["total_grams_affected"] = float(item["total_grams_affected"]) + grams
        item["row_count"] = int(item["row_count"]) + 1
        item["recipe_ids"].add(clean_text(row.get("recipe_id_candidate")))
        item["statuses"][status] += 1
        item["rows"].append(row)

        raw_text = clean_text(row.get("ingredient_raw_text"))
        if raw_text and raw_text not in item["raw_texts"]:
            item["raw_texts"].append(raw_text)
        recipe = clean_text(row.get("display_name"))
        if recipe and recipe not in item["recipes"]:
            item["recipes"].append(recipe)
    return groups


def is_all_purpose_variant(name: str) -> bool:
    return normalize_name(name).startswith("all purpose flour")


def classify_group(name: str, item: dict[str, object]) -> dict[str, str]:
    normalized = normalize_name(name)
    raw_blob = " | ".join(clean_text(row.get("ingredient_raw_text")) for row in item["rows"]).casefold()

    if is_all_purpose_variant(name):
        return {
            "classification": "safe_exact_fix",
            **ALL_PURPOSE_FIX,
        }

    if name in ROUND9_SAFE_FIXES:
        if name == "shiitake mushrooms" and "dried" not in raw_blob:
            return {
                "classification": "possible_but_needs_review",
                "possible_fooddb_match": "Shiitake mushroom item exists",
                "possible_fooddb_id": "food_shiitake_mushroom_dried",
                "source_macro_available": "existing_fooddb_round8",
                "safety": "state_not_clear",
                "recommended_action": "defer_until_raw_text_confirms_dried_or_raw_state",
                "risk_notes": "Round9 only maps the dried shiitake row.",
            }
        return {
            "classification": "safe_exact_fix",
            **ROUND9_SAFE_FIXES[name],
        }

    if name in EXPLICIT_DEFERRED:
        return {
            "classification": "keep_deferred",
            "possible_fooddb_match": "",
            "possible_fooddb_id": "",
            "source_macro_available": "not_used",
            "safety": "explicitly_deferred",
            "recommended_action": "keep_deferred_for_future_manual_or_recipe_review",
            "risk_notes": EXPLICIT_DEFERRED[name],
        }

    if name in {"tomatillo", "tomatillos"}:
        return {
            "classification": "possible_but_needs_review",
            "possible_fooddb_match": "Tomatillos, raw",
            "possible_fooddb_id": "",
            "source_macro_available": "source_exists_but_energy_or_macros_incomplete_or_suspect",
            "safety": "source_macro_not_safe",
            "recommended_action": "defer_low_macro_priority_until_source_energy_is_resolved",
            "risk_notes": "Foundation row lacks complete macros; SR row energy appears inconsistent with macro energy.",
        }

    if any(token in normalized for token in COMPOSED_TOKENS):
        return {
            "classification": "recipe_replacement_candidate",
            "possible_fooddb_match": "",
            "possible_fooddb_id": "",
            "source_macro_available": "not_used",
            "safety": "prepared_or_composed",
            "recommended_action": "treat_as_manual_recipe_review_or_replacement_not_mapping_loop",
            "risk_notes": "Prepared/composed item should not become a Food_DB atomic ingredient.",
        }

    if "rice" in normalized:
        return {
            "classification": "possible_but_needs_review",
            "possible_fooddb_match": "Rice item may exist only when cooked/dry state is explicit",
            "possible_fooddb_id": "",
            "source_macro_available": "existing_fooddb_round8_for_some_states",
            "safety": "state_or_variant_not_clear",
            "recommended_action": "do_not_promote_in_round9_unless_cooked_or_dry_state_is_explicit",
            "risk_notes": "Avoid rice vinegar/wine/wrapper/noodle confusion and state-unclear dry-vs-cooked rows.",
        }

    if "mushroom" in normalized:
        return {
            "classification": "possible_but_needs_review",
            "possible_fooddb_match": "Mushroom source/item may exist but exact species/state must be checked",
            "possible_fooddb_id": "",
            "source_macro_available": "varies_by_species",
            "safety": "species_or_package_state_not_safe",
            "recommended_action": "keep_deferred_or_manual_review_after_round9",
            "risk_notes": "Round9 maps only exact morel raw and dried shiitake rows.",
        }

    if "cheese" in normalized:
        return {
            "classification": "possible_but_needs_review",
            "possible_fooddb_match": "Specific cheese item may exist",
            "possible_fooddb_id": "",
            "source_macro_available": "varies_by_cheese",
            "safety": "unit_or_source_not_safe",
            "recommended_action": "do_not_add_cup_rules_or broad cheese collapses in round9",
            "risk_notes": "Round9 maps only ricotta with existing grams; other cheese cup rows need separate density review.",
        }

    if any(token in normalized for token in ("beef", "round steak", "top sirloin", "chicken", "pork", "turkey", "fish")):
        return {
            "classification": "possible_but_needs_review",
            "possible_fooddb_match": "Specific meat/fish item may exist",
            "possible_fooddb_id": "",
            "source_macro_available": "varies_by_cut_and_state",
            "safety": "not_exact_enough_for_round9",
            "recommended_action": "defer_unless_exact_cut_state_and_edible_yield_are_clear",
            "risk_notes": "No generic meat promotion and no count/piece rules in this pass.",
        }

    if name == "20 fat or higher preferred":
        return {
            "classification": "recipe_replacement_candidate",
            "possible_fooddb_match": "",
            "possible_fooddb_id": "",
            "source_macro_available": "not_used",
            "safety": "parser_fragment",
            "recommended_action": "manual_recipe_or_parser_review",
            "risk_notes": "Ingredient name is a preference fragment, not a food item.",
        }

    return {
        "classification": "keep_deferred",
        "possible_fooddb_match": "",
        "possible_fooddb_id": "",
        "source_macro_available": "not_used",
        "safety": "outside_round9_scope",
        "recommended_action": "stop_mapping_loop_and_review_later_if_recipe_remains_important",
        "risk_notes": "Not part of the final exact/safe round9 pass.",
    }


def build_candidate_rows(groups: dict[str, dict[str, object]]) -> list[dict[str, object]]:
    output: list[dict[str, object]] = []
    for name, item in groups.items():
        decision = classify_group(name, item)
        recipe_ids = item["recipe_ids"]
        output.append(
            {
                "classification": decision["classification"],
                "ingredient_name_normalized": name,
                "total_grams_affected": format_number(float(item["total_grams_affected"])),
                "affected_recipe_count": len(recipe_ids),
                "row_count": int(item["row_count"]),
                "example_raw_texts": " | ".join(item["raw_texts"][:5]),
                "example_recipes": " | ".join(item["recipes"][:5]),
                "macro_role": macro_role(name),
                "possible_fooddb_match": decision["possible_fooddb_match"],
                "possible_fooddb_id": decision["possible_fooddb_id"],
                "source_macro_available": decision["source_macro_available"],
                "safety": decision["safety"],
                "recommended_action": decision["recommended_action"],
                "risk_notes": decision["risk_notes"],
            }
        )
    return sorted(
        output,
        key=lambda row: (
            row["classification"] != "safe_exact_fix",
            -(parse_float(row["total_grams_affected"]) or 0.0),
            clean_text(row["ingredient_name_normalized"]),
        ),
    )


def cache_status_counts(cache_rows: list[dict[str, str]]) -> Counter[str]:
    return Counter(clean_text(row.get("cache_status")) for row in cache_rows)


def write_summary(path: Path, rows: list[dict[str, object]], cache_rows: list[dict[str, str]]) -> None:
    class_counts = Counter(clean_text(row["classification"]) for row in rows)
    safe_rows = [row for row in rows if row["classification"] == "safe_exact_fix"]
    review_rows = [row for row in rows if row["classification"] == "possible_but_needs_review"]
    deferred_rows = [row for row in rows if row["classification"] in {"keep_deferred", "recipe_replacement_candidate"}]
    status_counts = cache_status_counts(cache_rows)

    lines = [
        "Recipes_DB v1.1 round9 final mapping candidate audit",
        "=" * 58,
        "",
        f"Round8 recipes usable_from_mapped_ingredients: {status_counts['usable_from_mapped_ingredients']}",
        f"Candidate/blocker groups audited: {len(rows)}",
        "",
        "Classification counts:",
    ]
    lines.extend(f"- {classification}: {count}" for classification, count in class_counts.most_common())
    lines.extend(["", "Safe exact fixes selected for round9:"])
    if safe_rows:
        for row in safe_rows:
            lines.append(
                f"- {row['ingredient_name_normalized']}: {row['total_grams_affected']}g | "
                f"{row['possible_fooddb_id']} | {row['recommended_action']}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "Possible but kept for review:"])
    if review_rows:
        for row in sorted(review_rows, key=lambda item: -(parse_float(item["total_grams_affected"]) or 0.0))[:20]:
            lines.append(
                f"- {row['ingredient_name_normalized']}: {row['total_grams_affected']}g | "
                f"safety={row['safety']} | {row['risk_notes']}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "Deferred/replacement candidates kept out of mapping:"])
    if deferred_rows:
        for row in sorted(deferred_rows, key=lambda item: -(parse_float(item["total_grams_affected"]) or 0.0))[:20]:
            lines.append(
                f"- {row['ingredient_name_normalized']}: {row['total_grams_affected']}g | "
                f"class={row['classification']} | {row['risk_notes']}"
            )
    else:
        lines.append("- none")

    lines.extend(
        [
            "",
            "Round9 decision:",
            "- Apply only the safe_exact_fix rows above.",
            "- Keep tomatillos, state-unclear rice, generic meats, pork neck bones, tomato sauce, rice vinegar variants, prepared/composed rows, and meat count/piece rows deferred.",
            "- If nutrition impact is small, stop mapping passes and move to servings adjustment / recipe review.",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    cache_rows, _ = read_csv(ROUND8_CACHE)
    read_csv(ROUND8_CONTRIBUTIONS)
    mapping_rows, _ = read_csv(ROUND8_MAPPING)
    read_csv(ROUND8_INGREDIENTS)
    read_csv(ROUND8_FOODDB)

    groups = collect_groups(mapping_rows)
    candidate_rows = build_candidate_rows(groups)

    write_csv(OUT_CANDIDATES, candidate_rows, OUTPUT_COLUMNS)
    write_summary(OUT_SUMMARY, candidate_rows, cache_rows)

    class_counts = Counter(clean_text(row["classification"]) for row in candidate_rows)
    print("Round9 final mapping candidate audit written")
    print(f"candidate_groups={len(candidate_rows)}")
    for classification, count in class_counts.most_common():
        print(f"{classification}={count}")
    print(f"written_candidates={OUT_CANDIDATES}")
    print(f"written_summary={OUT_SUMMARY}")


if __name__ == "__main__":
    main()
