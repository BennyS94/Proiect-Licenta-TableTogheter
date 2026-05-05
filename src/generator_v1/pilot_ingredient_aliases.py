from __future__ import annotations


PILOT_SAFE_INGREDIENT_ALIASES: dict[str, str] = {
    "flank_steak": "food_beef_flank_steak_raw",
    "beef_short_ribs": "food_beef_short_ribs_raw",
    "olive_oil": "food_olive_oil_extra_virgin",
    "white_sugar": "food_sugar_white",
    "salt": "food_salt_white_sea_igneous_or_rock_no_enrichment",
    "egg": "food_egg_raw",
    "eggs": "food_egg_raw",
    "onion": "food_onion_raw",
    "garlic": "food_garlic_fresh",
}


PILOT_NEEDS_REVIEW_INGREDIENT_ALIASES: dict[str, str] = {
    # TODO: necesita decizie separata pentru carne cu os / mix de bucati.
    "cut_up_chicken_parts": "food_chicken_meat_and_skin_raw",
    "korean_style_short_ribs": "food_beef_short_ribs_raw",
    "korean_style_short_ribs_beef_chuck_flanken": "food_beef_short_ribs_raw",
    # TODO: necesita decizie generic butter: sarat vs nesarat.
    "butter": "food_butter_82_fat_unsalted",
}
