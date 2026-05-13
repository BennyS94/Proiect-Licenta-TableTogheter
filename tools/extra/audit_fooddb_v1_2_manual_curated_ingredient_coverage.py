from __future__ import annotations

from collections import Counter
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra.round41_manual_curated_common import (
    FOODDB_AUDIT_DIR,
    fooddb_path,
    read_csv,
    to_float,
    write_csv,
    write_text,
)


OUT_CSV = FOODDB_AUDIT_DIR / "fooddb_v1_2_manual_curated_ingredient_coverage.csv"
OUT_SUMMARY = FOODDB_AUDIT_DIR / "fooddb_v1_2_manual_curated_ingredient_coverage_summary.txt"


ROLE_KEYWORDS = {
    "protein": [
        "chicken",
        "turkey",
        "beef",
        "pork",
        "salmon",
        "tuna",
        "cod",
        "egg",
        "ham",
        "lentil",
        "bean",
    ],
    "carb": [
        "rice",
        "pasta",
        "potato",
        "oat",
        "bread",
        "couscous",
        "bulgur",
        "corn",
        "wrap",
        "lentil",
        "bean",
    ],
    "veg": [
        "broccoli",
        "carrot",
        "pepper",
        "onion",
        "tomato",
        "spinach",
        "cabbage",
        "mushroom",
        "zucchini",
        "courgette",
        "green_beans",
        "lettuce",
        "peas",
    ],
    "dairy_fat": [
        "milk",
        "yogurt",
        "cheese",
        "butter",
        "olive_oil",
        "vegetable_oil",
        "sour_cream",
        "fresh_cheese",
    ],
    "fruit": ["banana", "apple", "berries", "berry", "pineapple"],
    "seasoning_sauce": [
        "salt",
        "pepper",
        "soy_sauce",
        "tomato_puree",
        "tomato_sauce",
        "herb",
        "cayenne",
    ],
}

PREFERRED_CANONICALS = {
    "chicken_breast_without_skin_raw",
    "chicken_thigh_meat_and_skin_raw",
    "turkey_cooked_ham_in_slices",
    "beef_round_steak_raw",
    "pork_tenderloin_lean_raw",
    "pork_chop_raw",
    "salmon_canned_drained",
    "salmon_smoked",
    "tuna_plain_canned_drained",
    "egg_raw",
    "egg_hard_boiled",
    "rice_cooked_unsalted",
    "dried_pasta_cooked_unsalted",
    "potato_cooked",
    "oat_raw",
    "bread_wholemeal_or_integral_bread_made_with_flour_type_150",
    "couscous_precooked_durum_wheat_semolina_cooked_unsalted",
    "wheat_bulgur_cooked_unsalted",
    "lentil_boiled_cooked_in_water",
    "red_kidney_bean_boiled_cooked_in_water",
    "broccoli_boiled_cooked_in_water_tender",
    "carrot_raw",
    "sweet_pepper_green_yellow_or_red_raw",
    "onion_raw",
    "tomato_raw",
    "tomato_puree_canned",
    "spinach_raw",
    "button_mushroom_or_cultivated_mushroom_raw",
    "courgette_or_zucchini_pulp_and_peel_raw",
    "green_beans_cooked_unsalted",
    "lettuce_raw",
    "garden_peas_frozen_cooked",
    "sweet_corn_canned_drained",
    "milk_semi_skimmed_pasteurised",
    "yogurt_greek_style_plain",
    "drained_soft_fresh_cheese_around_6_fat",
    "cheddar_cheese",
    "sour_cream_light",
    "olive_oil_extra_virgin",
    "banana_pulp_raw",
    "apple_pulp_and_peel_raw",
}


def main() -> None:
    fooddb = read_csv(fooddb_path())
    rows = []
    for food in fooddb:
        role = infer_role(food)
        if role == "other":
            continue
        suitability, reason = classify_suitability(food, role)
        rows.append(
            {
                "food_id": food.get("food_id", ""),
                "canonical_name": food.get("canonical_name", ""),
                "display_name": food.get("display_name", ""),
                "role": role,
                "energy_kcal_100": food.get("energy_kcal_100g", ""),
                "protein_g_100": food.get("protein_g_100g", ""),
                "carbs_g_100": food.get("carbs_g_100g", ""),
                "fat_g_100": food.get("fat_g_100g", ""),
                "suitability": suitability,
                "reason": reason,
            }
        )
    rows.sort(key=lambda row: (row["suitability"], row["role"], row["canonical_name"]))
    write_csv(
        OUT_CSV,
        rows,
        [
            "food_id",
            "canonical_name",
            "display_name",
            "role",
            "energy_kcal_100",
            "protein_g_100",
            "carbs_g_100",
            "fat_g_100",
            "suitability",
            "reason",
        ],
    )
    counts = Counter(row["suitability"] for row in rows)
    role_counts = Counter(row["role"] for row in rows)
    summary = [
        "Food_DB v1.2 manual-curated ingredient coverage",
        "",
        f"fooddb_path={fooddb_path()}",
        f"total_fooddb_rows={len(fooddb)}",
        f"coverage_rows={len(rows)}",
        f"good_for_manual_curated={counts.get('good_for_manual_curated', 0)}",
        f"usable={counts.get('usable', 0)}",
        f"avoid_for_now={counts.get('avoid_for_now', 0)}",
        f"role_counts={dict(role_counts)}",
        "",
        "Conclusion:",
        "- There is enough coverage for a controlled manual-curated batch using explicit grams.",
        "- Turkey and legume variety remain weaker than chicken/beef/egg unless source verification continues.",
    ]
    write_text(OUT_SUMMARY, "\n".join(summary))
    print("\n".join(summary))


def infer_role(food: dict[str, str]) -> str:
    text = f"{food.get('canonical_name', '')} {food.get('display_name', '')}".lower()
    for role, keywords in ROLE_KEYWORDS.items():
        if any(keyword in text for keyword in keywords):
            return role
    if str(food.get("helper_use_as_protein", "")).lower() == "true":
        return "protein"
    if str(food.get("helper_use_as_carb_side", "")).lower() == "true":
        return "carb"
    if str(food.get("helper_use_as_veg_side", "")).lower() == "true":
        return "veg"
    return "other"


def classify_suitability(food: dict[str, str], role: str) -> tuple[str, str]:
    canonical = food.get("canonical_name", "")
    kcal = to_float(food.get("energy_kcal_100g"))
    protein = to_float(food.get("protein_g_100g"))
    carbs = to_float(food.get("carbs_g_100g"))
    fat = to_float(food.get("fat_g_100g"))
    text = f"{canonical} {food.get('display_name', '')}".lower()
    if canonical in PREFERRED_CANONICALS:
        return "good_for_manual_curated", "preferred_round41_clean_mapping"
    if "alcohol" in text or "dessert" in text or "cake" in text or "sauce_prepacked" in text:
        return "avoid_for_now", "processed_or_not_core_manual_recipe_ingredient"
    if role == "protein" and protein >= 12 and kcal <= 450:
        return "usable", "protein_item_with_plausible_macros"
    if role == "carb" and carbs >= 10 and kcal <= 450:
        return "usable", "carb_item_with_plausible_macros"
    if role == "veg" and kcal <= 120:
        return "usable", "vegetable_item_with_low_energy"
    if role == "dairy_fat" and kcal <= 500 and (protein > 0 or fat > 0):
        return "usable", "dairy_or_fat_item_with_plausible_macros"
    if role == "fruit" and carbs >= 5 and kcal <= 150:
        return "usable", "fruit_item_with_plausible_macros"
    return "avoid_for_now", "not_selected_for_clean_manual_batch"


if __name__ == "__main__":
    main()
