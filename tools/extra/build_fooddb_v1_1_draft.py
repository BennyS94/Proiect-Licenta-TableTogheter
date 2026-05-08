from __future__ import annotations

import argparse
import csv
import math
import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

CURRENT_FOODDB_PATH = REPO_ROOT / "data" / "fooddb" / "current" / "fooddb_v1_core_master_draft.csv"
GAP_CANDIDATES_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_gap_additions_candidates.csv"
GAP_REVIEW_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_gap_additions_review.csv"

OUT_DRAFT_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft.csv"
OUT_APPLIED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_additions_applied.csv"
OUT_DEFERRED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_additions_deferred.csv"
OUT_SUMMARY_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_draft_summary.txt"

V1_1_BASE_FOODDB_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft.csv"
ROUND2_OUT_DRAFT_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round2.csv"
ROUND2_OUT_APPLIED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round2_additions_applied.csv"
ROUND2_OUT_DEFERRED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round2_additions_deferred.csv"
ROUND2_OUT_SUMMARY_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round2_summary.txt"
ROUND3_OUT_DRAFT_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round3.csv"
ROUND3_OUT_APPLIED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round3_additions_applied.csv"
ROUND3_OUT_DEFERRED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round3_additions_deferred.csv"
ROUND3_OUT_SUMMARY_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round3_summary.txt"
ROUND5_OUT_DRAFT_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round5.csv"
ROUND5_OUT_APPLIED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round5_additions_applied.csv"
ROUND5_OUT_DEFERRED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round5_additions_deferred.csv"
ROUND5_OUT_SUMMARY_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round5_summary.txt"
ROUND7_OUT_DRAFT_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round7.csv"
ROUND7_OUT_APPLIED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round7_additions_applied.csv"
ROUND7_OUT_DEFERRED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round7_additions_deferred.csv"
ROUND7_OUT_SUMMARY_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round7_summary.txt"
ROUND8_OUT_DRAFT_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round8.csv"
ROUND8_OUT_APPLIED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round8_additions_applied.csv"
ROUND8_OUT_DEFERRED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round8_additions_deferred.csv"
ROUND8_OUT_SUMMARY_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round8_summary.txt"
ROUND9_OUT_DRAFT_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round9.csv"
ROUND9_OUT_APPLIED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round9_additions_applied.csv"
ROUND9_OUT_DEFERRED_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round9_additions_deferred.csv"
ROUND9_OUT_SUMMARY_PATH = REPO_ROOT / "data" / "fooddb" / "audit" / "fooddb_v1_1_round9_summary.txt"

APPROVED_ADDITIONS = {"green beans", "parmesan cheese", "cornstarch"}
EXPLICITLY_DEFERRED = {
    "chicken thighs",
    "tomato sauce",
    "chicken broth",
    "beef",
    "turkey",
    "rice",
    "milk",
    "onion",
    "yellow onion",
    "red onion",
    "potatoes",
    "red potatoes",
    "green onions",
    "butter",
    "mushrooms",
}

ROUND2_APPROVED_ADDITIONS = {"chicken broth"}
ROUND2_EXISTING_ALIAS_TARGETS = {
    "butter": "food_butter_82_fat_unsalted",
    "carrot": "food_carrot_raw",
    "carrots": "food_carrot_raw",
    "green onions": "food_chive_or_spring_onion_fresh",
    "lemon": "food_lemon_pulp_raw",
}
ROUND2_EXPLICITLY_DEFERRED = {
    "beef": "generic_beef_remains_too_broad_for_auto_mapping",
    "turkey": "generic_turkey_remains_review_until recipe context is explicit",
    "chicken thighs": "source_energy_suspicious_possible_kj_or_non_kcal_unit",
    "tomato sauce": "source_energy_suspicious_possible_kj_or_non_kcal_unit",
    "mushrooms": "not_in_round2_priority_scope",
    "potatoes": "not_in_round2_priority_scope",
    "red potatoes": "not_in_round2_priority_scope",
}
ROUND3_EXISTING_ALIAS_TARGETS = {
    "beef_ground_rows_only": "food_beef_minced_steak_15_fat_raw",
    "beef_lean_ground_rows_only": "food_beef_minced_steak_10_fat_raw",
    "cubed_beef_stew_meat": "food_beef_stewing_meat_raw",
    "garlic_cloves": "food_garlic_fresh",
    "jasmine_rice_cooked_rows_only": "food_rice_thai_cooked",
    "kosher_salt": "food_salt_white_sea_igneous_or_rock_no_enrichment",
    "mozzarella_cheese": "food_mozzarella_cheese_from_cow_s_milk",
    "onions": "food_onion_raw",
    "pork_loin": "food_pork_loin_raw",
    "pork_shoulder": "food_pork_shoulder_raw",
    "potatoes_clear_raw_rows_only": "food_potato_peeled_raw",
    "potatoes_clear_cooked_rows_only": "food_potato_boiled_cooked_in_water",
    "white_rice_cooked_rows_only": "food_rice_cooked_unsalted",
    "white_rice_uncooked_rows_only": "food_rice_raw",
}
ROUND3_EXPLICITLY_DEFERRED = {
    "red potatoes": "exact_red_potato_item_missing_keep_review",
    "chicken thighs": "exact_safe_chicken_thigh_item_or_edible_yield_rule_missing",
    "bone in chicken pieces": "needs_edible_yield_rule_before_mapping",
    "turkey": "ground_turkey_exact_item_missing_keep_review",
    "pork": "generic_pork_remains_broad_keep_review",
    "generic_beef": "generic_beef_still_not_promoted_globally",
}

ROUND5_EXISTING_ALIAS_TARGETS = {
    "potatoes_raw_rows_only": "food_potato_peeled_raw",
    "red_potatoes_cultivar_collapse": "food_potato_peeled_raw",
    "yukon_gold_potatoes_cultivar_collapse": "food_potato_peeled_raw",
    "butter_macro_draft": "food_butter_82_fat_unsalted",
    "unsalted_butter_macro_draft": "food_butter_82_fat_unsalted",
    "salted_butter_macro_draft": "food_butter_82_fat_unsalted",
    "ground_turkey_rows_pilot": "food_turkey_meat_raw",
    "boneless_skinless_chicken_thighs_pilot": "food_chicken_leg_meat_raw",
    "chicken_thighs_pilot_edible_yield": "food_chicken_leg_meat_and_skin_raw",
    "bone_in_chicken_pieces_pilot_edible_yield": "food_chicken_meat_and_skin_raw",
    "pork_chop_exact": "food_pork_chop_raw",
    "pork_tenderloin_exact": "food_pork_tenderloin_lean_raw",
}

ROUND5_ALIAS_NOTES = {
    "potatoes_raw_rows_only": "round5_manual_decision; raw_potatoes_only_no_prepared_dish_signal",
    "red_potatoes_cultivar_collapse": "round5_manual_decision; cultivar_collapsed_to_generic_potato_v1_1",
    "yukon_gold_potatoes_cultivar_collapse": "round5_manual_decision; cultivar_collapsed_to_generic_potato_v1_1",
    "butter_macro_draft": "round5_manual_decision; butter_salted_unsalted_macro_difference_not_material_for_kcal_macros",
    "unsalted_butter_macro_draft": "round5_manual_decision; exact_unsalted_butter_existing",
    "salted_butter_macro_draft": "round5_manual_decision; salted_collapsed_to_generic_butter_for_macro_draft",
    "ground_turkey_rows_pilot": "round5_manual_decision; exact_ground_turkey_missing_use_generic_turkey_meat_raw_for_explicit_ground_turkey_rows",
    "boneless_skinless_chicken_thighs_pilot": "round5_manual_decision; pilot_manual_edible_yield_boneless_skinless_to_chicken_leg_meat_raw",
    "chicken_thighs_pilot_edible_yield": "round5_manual_decision; pilot_manual_edible_yield_chicken_thigh_to_leg_meat_and_skin_raw",
    "bone_in_chicken_pieces_pilot_edible_yield": "round5_manual_decision; pilot_manual_edible_yield_bone_in_chicken_pieces_to_chicken_meat_and_skin_raw",
    "pork_chop_exact": "round5_manual_decision; exact_pork_chop_existing",
    "pork_tenderloin_exact": "round5_manual_decision; exact_pork_tenderloin_existing",
}

ROUND5_EXPLICITLY_DEFERRED = {
    "exact_chicken_thigh_source_row": "source_energy_suspicious_possible_kj_or_non_kcal_unit_not_added",
    "turkey_breast": "exact_turkey_breast_item_missing_keep_review",
    "generic_turkey": "generic_turkey_not_globally_promoted_except_explicit_ground_turkey_rows",
    "generic_beef": "generic_beef_still_not_promoted_globally",
    "generic_pork": "generic_pork_remains_broad_keep_review",
    "ground_pork": "exact_ground_pork_item_missing_keep_review",
    "pork_neck_bones": "pork_neck_bones_need_bone_specific_yield_and_source_keep_deferred",
    "rice_vinegar_variants": "vinegar_variants_not_macro_blocker_and_unsafe_for_rice_mapping",
    "tomato_sauce": "source_energy_suspicious_possible_kj_or_non_kcal_unit",
}

ROUND7_EXISTING_ALIAS_TARGETS = {
    "mayonnaise_exact": "food_mayonnaise_70_fat_and_more_prepacked",
    "ham_exact": "food_cooked_ham_choice",
    "bay_scallops_to_scallop_raw": "food_scallop_without_coral_raw",
    "pork_sausage_exact": "food_sausage_meat_pure_pork_raw",
    "mild_italian_sausage_to_generic_pork_sausage": "food_sausage_meat_pure_pork_raw",
    "spicy_pork_sausage_to_generic_pork_sausage": "food_sausage_meat_pure_pork_raw",
}

ROUND7_ALIAS_NOTES = {
    "mayonnaise_exact": "round7_macro_blocker; exact_mayonnaise_prepacked_70_fat",
    "ham_exact": "round7_macro_blocker; exact_cooked_ham_macro_draft",
    "bay_scallops_to_scallop_raw": "round7_macro_blocker; bay_scallops_collapsed_to_scallop_raw_without_coral",
    "pork_sausage_exact": "round7_macro_blocker; generic_pork_sausage_meat_raw",
    "mild_italian_sausage_to_generic_pork_sausage": "round7_macro_blocker; sausage_variant_collapsed_to_generic_pork_sausage_v1_1",
    "spicy_pork_sausage_to_generic_pork_sausage": "round7_macro_blocker; sausage_variant_collapsed_to_generic_pork_sausage_v1_1",
}

ROUND7_ALIAS_SAFETY = {
    "mayonnaise_exact": "safe_auto",
    "ham_exact": "safe_auto",
    "bay_scallops_to_scallop_raw": "safe_auto",
    "pork_sausage_exact": "safe_auto",
    "mild_italian_sausage_to_generic_pork_sausage": "needs_review",
    "spicy_pork_sausage_to_generic_pork_sausage": "needs_review",
}

ROUND7_EXPLICITLY_DEFERRED = {
    "pork_neck_bones": "pork_neck_bones_need_bone_specific_yield_and_source_keep_deferred",
    "generic_pork": "generic_pork_remains_broad_keep_review",
    "generic_beef": "generic_beef_still_not_promoted_globally",
    "generic_turkey": "generic_turkey_not_globally_promoted",
    "salted_cod_fish": "exact_salted_cod_item_missing_keep_review",
    "spinach_pasta_dough": "exact_spinach_pasta_dough_item_missing_keep_review",
    "rice_vinegar_variants": "vinegar_variants_not_macro_blocker_and_unsafe_for_rice_mapping",
    "tomato_sauce": "source_energy_suspicious_possible_kj_or_non_kcal_unit",
    "ambiguous_processed_dishes": "do_not_add_composed_recipes_as_fooddb_items",
}

ROUND8_EXISTING_ALIAS_TARGETS = {
    "mushrooms_raw": "food_button_mushroom_or_cultivated_mushroom_raw",
    "asparagus_raw": "food_asparagus_green_raw",
    "basmati_rice_raw": "food_basmati_rice_raw",
    "basmati_rice_cooked": "food_rice_basmati_cooked",
}

ROUND8_ALIAS_NOTES = {
    "mushrooms_raw": "round8_punctual_mapping; exact_raw_button_mushroom_existing",
    "asparagus_raw": "round8_punctual_mapping; exact_raw_asparagus_existing",
    "basmati_rice_raw": "round8_punctual_mapping; exact_raw_basmati_rice_existing",
    "basmati_rice_cooked": "round8_punctual_mapping; exact_cooked_basmati_rice_existing",
}

ROUND8_ALIAS_SAFETY = {
    "mushrooms_raw": "safe_auto",
    "asparagus_raw": "safe_auto",
    "basmati_rice_raw": "safe_auto",
    "basmati_rice_cooked": "safe_auto",
}

ROUND8_EXPLICITLY_DEFERRED = {
    "tomatillos_raw": "exact_tomatillo_item_missing_keep_unmapped",
    "whole_wheat_flour": "exact_whole_wheat_flour_item_missing_keep_unmapped",
    "salted_cod_fish": "exact_salted_cod_item_missing_keep_review",
    "pork_neck_bones": "pork_neck_bones_need_bone_specific_yield_and_source_keep_deferred",
    "generic_pork": "generic_pork_remains_broad_keep_review",
    "generic_beef": "generic_beef_still_not_promoted_globally",
    "generic_turkey": "generic_turkey_not_globally_promoted",
    "spinach_pasta_dough": "exact_spinach_pasta_dough_item_missing_keep_review",
    "tomato_sauce": "source_energy_suspicious_possible_kj_or_non_kcal_unit",
    "rice_vinegar_variants": "vinegar_variants_not_macro_blocker_and_unsafe_for_rice_mapping",
    "prepared_or_composed_dishes": "do_not_add_composed_recipes_as_fooddb_items",
}

ROUND9_SOURCE_ADDITIONS = {
    "all purpose flour": {
        "suggested_food_id": "food_wheat_flour_white_all_purpose_enriched_unbleached",
        "suggested_canonical_name": "wheat_flour_white_all_purpose_enriched_unbleached",
        "suggested_display_name": "Wheat flour, white, all-purpose, enriched, unbleached",
        "suggested_food_group": "cereal grains and pasta",
        "suggested_role": "carb_ingredient",
        "source_used": "data/fooddb/source/comprehensive_foods_usda.csv",
        "source_row_reference": "usda:168936",
        "energy_kcal_100": "364.0",
        "protein_g_100": "10.3",
        "carbs_g_100": "76.3",
        "fat_g_100": "0.98",
        "frequency_in_v1_1": "8",
        "rows_with_grams": "8",
        "total_grams_affected": "663.75",
        "proposed_action": "add_to_fooddb_round9_draft",
        "safety": "safe_auto",
        "review_notes": "round9_final_mapping; exact_all_purpose_flour_source_macro_sane",
        "example_raw_texts": "1 tablespoon all-purpose flour | 2 cups all-purpose flour | 1/2 cup all-purpose flour for coating",
        "example_recipes": "Creamy Morel Mushroom Soup | Mom's Best Waffles | Braised Beef Short Ribs",
    },
    "whole wheat flour": {
        "suggested_food_id": "food_wheat_flour_whole_grain_soft_wheat",
        "suggested_canonical_name": "wheat_flour_whole_grain_soft_wheat",
        "suggested_display_name": "Wheat flour, whole-grain, soft wheat",
        "suggested_food_group": "cereal grains and pasta",
        "suggested_role": "carb_ingredient",
        "source_used": "data/fooddb/source/comprehensive_foods_usda.csv",
        "source_row_reference": "usda:168944",
        "energy_kcal_100": "332.0",
        "protein_g_100": "9.61",
        "carbs_g_100": "74.5",
        "fat_g_100": "1.95",
        "frequency_in_v1_1": "2",
        "rows_with_grams": "2",
        "total_grams_affected": "660",
        "proposed_action": "add_to_fooddb_round9_draft",
        "safety": "safe_auto",
        "review_notes": "round9_final_mapping; exact_whole_wheat_flour_source_macro_sane",
        "example_raw_texts": "5 cups whole wheat flour | 1/2 cup whole wheat flour",
        "example_recipes": "Bacon-Flavored Dog Biscuits | Harvest Patties",
    },
    "salted cod fish": {
        "suggested_food_id": "food_cod_atlantic_dried_and_salted",
        "suggested_canonical_name": "cod_atlantic_dried_and_salted",
        "suggested_display_name": "Fish, cod, Atlantic, dried and salted",
        "suggested_food_group": "meat, fish, eggs and alternatives",
        "suggested_role": "protein",
        "source_used": "data/fooddb/source/comprehensive_foods_usda.csv",
        "source_row_reference": "usda:174190",
        "energy_kcal_100": "290.0",
        "protein_g_100": "62.8",
        "carbs_g_100": "0.0",
        "fat_g_100": "2.37",
        "frequency_in_v1_1": "1",
        "rows_with_grams": "1",
        "total_grams_affected": "907.18",
        "proposed_action": "add_to_fooddb_round9_draft",
        "safety": "safe_auto",
        "review_notes": "round9_final_mapping; exact_salted_cod_source_macro_sane; do_not_force_generic_cod",
        "example_raw_texts": "2 pounds salted cod fish",
        "example_recipes": "Portuguese Cod Fish Casserole",
    },
}

ROUND9_EXISTING_ALIAS_TARGETS = {
    "chicken meat": "food_chicken_meat_raw",
    "crabmeat": "food_crab_raw",
    "morel mushrooms": "food_morel_raw",
    "ricotta cheese": "food_ricotta_cheese",
    "shiitake mushrooms": "food_shiitake_mushroom_dried",
    "sirloin steak": "food_beef_sirloin_steak_raw",
}

ROUND9_ALIAS_NOTES = {
    "chicken meat": "round9_final_mapping; exact_chicken_meat_existing_fooddb",
    "crabmeat": "round9_final_mapping; fresh_crabmeat_to_raw_crab_existing_fooddb",
    "morel mushrooms": "round9_final_mapping; exact_fresh_morel_mushrooms_existing_fooddb",
    "ricotta cheese": "round9_final_mapping; exact_ricotta_existing_fooddb_no_new_cup_rule",
    "shiitake mushrooms": "round9_final_mapping; exact_dried_shiitake_existing_fooddb",
    "sirloin steak": "round9_final_mapping; exact_sirloin_steak_existing_fooddb",
}

ROUND9_EXPLICITLY_DEFERRED = {
    "tomatillos_raw": "exact_tomatillo_source_exists_but_macro_source_incomplete_or_energy_suspect_keep_deferred_low_macro",
    "cod_fish_raw": "no_non_salted_cod_blocker_in_round9_do_not_add_unused_generic_cod",
    "flour_generic": "generic_flour_not_promoted_to_all_purpose_without_exact_name",
    "rice_state_unclear": "remaining_rice_or_basmati_rows_lack_explicit_cooked_or_dry_state",
    "pork_neck_bones": "pork_neck_bones_need_bone_specific_yield_and_source_keep_deferred",
    "generic_pork": "generic_pork_remains_broad_keep_review",
    "generic_beef": "generic_beef_still_not_promoted_globally",
    "generic_turkey": "generic_turkey_not_globally_promoted",
    "spinach_pasta_dough": "prepared_dough_not_fooddb_atomic_item",
    "tomato_sauce": "source_energy_suspicious_possible_kj_or_non_kcal_unit",
    "rice_vinegar_variants": "vinegar_variants_not_macro_blocker_and_unsafe_for_rice_mapping",
    "prepared_or_composed_dishes": "do_not_add_composed_recipes_as_fooddb_items",
    "meat_count_or_piece_rows": "no_cod_fish_pork_beef_turkey_count_rules_without_edible_yield_decision",
}

MAX_PLAUSIBLE_ENERGY_KCAL_100G = 900.0
MIN_MACRO_ENERGY_RATIO = 0.55
MAX_MACRO_ENERGY_RATIO = 1.55

ADDITION_PROFILES = {
    "green beans": {
        "food_family_name": "Green beans",
        "entity_level": "atomic",
        "food_group": "fruits, vegetables, legumes and nuts",
        "food_subgroup": "vegetables",
        "food_subgroup_detail": "vegetables, cooked",
        "processing_state": "cooked",
        "helper_use_as_protein": "False",
        "helper_use_as_carb_side": "False",
        "helper_use_as_veg_side": "True",
        "helper_is_sweet": "False",
        "helper_is_salty": "False",
        "helper_is_vegetarian": "True",
        "helper_is_vegan": "True",
        "helper_protein_bucket": "veggie",
        "helper_carb_bucket": "",
        "helper_veg_bucket": "cooked_veg",
    },
    "parmesan cheese": {
        "food_family_name": "Parmesan cheese",
        "entity_level": "semi_atomic",
        "food_group": "milk and milk products",
        "food_subgroup": "cheese and similar",
        "food_subgroup_detail": "hard cheeses and similar",
        "processing_state": "",
        "helper_use_as_protein": "True",
        "helper_use_as_carb_side": "False",
        "helper_use_as_veg_side": "False",
        "helper_is_sweet": "False",
        "helper_is_salty": "True",
        "helper_is_vegetarian": "True",
        "helper_is_vegan": "False",
        "helper_protein_bucket": "dairy",
        "helper_carb_bucket": "",
        "helper_veg_bucket": "",
    },
    "cornstarch": {
        "food_family_name": "Cornstarch",
        "entity_level": "semi_atomic",
        "food_group": "miscellaneous",
        "food_subgroup": "miscellaneous ingredients",
        "food_subgroup_detail": "",
        "processing_state": "",
        "helper_use_as_protein": "False",
        "helper_use_as_carb_side": "False",
        "helper_use_as_veg_side": "False",
        "helper_is_sweet": "False",
        "helper_is_salty": "False",
        "helper_is_vegetarian": "True",
        "helper_is_vegan": "True",
        "helper_protein_bucket": "",
        "helper_carb_bucket": "",
        "helper_veg_bucket": "",
    },
    "chicken broth": {
        "food_family_name": "Chicken broth",
        "entity_level": "semi_atomic",
        "food_group": "miscellaneous",
        "food_subgroup": "miscellaneous ingredients",
        "food_subgroup_detail": "broths and stocks",
        "processing_state": "",
        "helper_use_as_protein": "False",
        "helper_use_as_carb_side": "False",
        "helper_use_as_veg_side": "False",
        "helper_is_sweet": "False",
        "helper_is_salty": "True",
        "helper_is_vegetarian": "False",
        "helper_is_vegan": "False",
        "helper_protein_bucket": "",
        "helper_carb_bucket": "",
        "helper_veg_bucket": "",
    },
    "all purpose flour": {
        "food_family_name": "Wheat flour",
        "entity_level": "semi_atomic",
        "food_group": "cereal grains and pasta",
        "food_subgroup": "flours and starches",
        "food_subgroup_detail": "wheat flours",
        "processing_state": "dry",
        "helper_use_as_protein": "False",
        "helper_use_as_carb_side": "False",
        "helper_use_as_veg_side": "False",
        "helper_is_sweet": "False",
        "helper_is_salty": "False",
        "helper_is_vegetarian": "True",
        "helper_is_vegan": "True",
        "helper_protein_bucket": "",
        "helper_carb_bucket": "grain",
        "helper_veg_bucket": "",
    },
    "whole wheat flour": {
        "food_family_name": "Wheat flour",
        "entity_level": "semi_atomic",
        "food_group": "cereal grains and pasta",
        "food_subgroup": "flours and starches",
        "food_subgroup_detail": "whole grain wheat flours",
        "processing_state": "dry",
        "helper_use_as_protein": "False",
        "helper_use_as_carb_side": "False",
        "helper_use_as_veg_side": "False",
        "helper_is_sweet": "False",
        "helper_is_salty": "False",
        "helper_is_vegetarian": "True",
        "helper_is_vegan": "True",
        "helper_protein_bucket": "",
        "helper_carb_bucket": "grain",
        "helper_veg_bucket": "",
    },
    "salted cod fish": {
        "food_family_name": "Cod",
        "entity_level": "atomic",
        "food_group": "meat, fish, eggs and alternatives",
        "food_subgroup": "fish",
        "food_subgroup_detail": "preserved fish",
        "processing_state": "dried_salted",
        "helper_use_as_protein": "True",
        "helper_use_as_carb_side": "False",
        "helper_use_as_veg_side": "False",
        "helper_is_sweet": "False",
        "helper_is_salty": "True",
        "helper_is_vegetarian": "False",
        "helper_is_vegan": "False",
        "helper_protein_bucket": "fish",
        "helper_carb_bucket": "",
        "helper_veg_bucket": "",
    },
}


def read_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    if not path.exists():
        raise FileNotFoundError(f"Lipseste fisierul asteptat: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        return rows, list(reader.fieldnames or [])


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def parse_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
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


def extract_source_uid(source_reference: str) -> str:
    match = re.search(r"fdc_id=([0-9]+)", source_reference or "")
    if match:
        return f"usda:{match.group(1)}"
    return source_reference or ""


def compute_macro_profile(protein_g: float, carbs_g: float, fat_g: float) -> str:
    parts: list[str] = []
    if protein_g >= 10:
        parts.append("protein_high")
    elif protein_g >= 3:
        parts.append("protein_moderate")
    else:
        parts.append("protein_low")

    if carbs_g >= 20:
        parts.append("carb_high")
    elif carbs_g >= 5:
        parts.append("carb_moderate")
    else:
        parts.append("carb_low")

    if fat_g >= 15:
        parts.append("fat_high")
    elif fat_g >= 5:
        parts.append("fat_moderate")
    else:
        parts.append("fat_low")
    return ";".join(parts)


def sanity_check(candidate: dict[str, str]) -> tuple[bool, list[str], dict[str, float]]:
    energy = parse_float(candidate.get("energy_kcal_100"))
    protein = parse_float(candidate.get("protein_g_100"))
    carbs = parse_float(candidate.get("carbs_g_100"))
    fat = parse_float(candidate.get("fat_g_100"))

    values = {
        "energy": energy,
        "protein": protein,
        "carbs": carbs,
        "fat": fat,
    }
    notes: list[str] = []

    if any(value is None for value in values.values()):
        notes.append("missing_required_macro")
        return False, notes, {}

    assert energy is not None
    assert protein is not None
    assert carbs is not None
    assert fat is not None

    if energy <= 0 or energy > MAX_PLAUSIBLE_ENERGY_KCAL_100G:
        notes.append("energy_outside_plausible_kcal_range")
    if protein < 0 or carbs < 0 or fat < 0:
        notes.append("negative_macro_value")

    macro_energy = protein * 4 + carbs * 4 + fat * 9
    if macro_energy > 0:
        ratio = energy / macro_energy
        if ratio < MIN_MACRO_ENERGY_RATIO or ratio > MAX_MACRO_ENERGY_RATIO:
            notes.append(f"macro_energy_mismatch_ratio={ratio:.2f}")
    elif energy > 20:
        notes.append("energy_present_but_macro_energy_zero")

    review_notes = (candidate.get("review_notes") or "").lower()
    if "source_energy_suspicious" in review_notes:
        notes.append("source_energy_flagged_suspicious")

    return not notes, notes or ["sanity_passed"], {
        "energy": energy,
        "protein": protein,
        "carbs": carbs,
        "fat": fat,
    }


def macro_values_from_fooddb_row(food_row: dict[str, str]) -> dict[str, float] | None:
    energy = parse_float(food_row.get("energy_kcal_100g"))
    protein = parse_float(food_row.get("protein_g_100g"))
    carbs = parse_float(food_row.get("carbs_g_100g"))
    fat = parse_float(food_row.get("fat_g_100g"))
    if any(value is None for value in (energy, protein, carbs, fat)):
        return None
    assert energy is not None
    assert protein is not None
    assert carbs is not None
    assert fat is not None
    return {
        "energy": energy,
        "protein": protein,
        "carbs": carbs,
        "fat": fat,
    }


def find_food_row_by_id(food_rows: list[dict[str, str]], food_id: str) -> dict[str, str] | None:
    for row in food_rows:
        if row.get("food_id") == food_id:
            return row
    return None


def build_alias_candidate(
    ingredient_name: str,
    food_row: dict[str, str],
    source_candidate: dict[str, str] | None = None,
) -> dict[str, str]:
    candidate = dict(source_candidate or {})
    candidate["ingredient_name_normalized"] = ingredient_name
    candidate["suggested_food_id"] = food_row.get("food_id", "")
    candidate["suggested_canonical_name"] = food_row.get("canonical_name", "")
    candidate["suggested_display_name"] = food_row.get("display_name", "")
    candidate["suggested_food_group"] = food_row.get("food_group", "")
    candidate["source_used"] = "current_fooddb_existing"
    candidate["proposed_action"] = "alias_to_existing"
    candidate["energy_kcal_100"] = food_row.get("energy_kcal_100g", "")
    candidate["protein_g_100"] = food_row.get("protein_g_100g", "")
    candidate["carbs_g_100"] = food_row.get("carbs_g_100g", "")
    candidate["fat_g_100"] = food_row.get("fat_g_100g", "")
    return candidate


def build_fooddb_row(candidate: dict[str, str], fieldnames: list[str], macro_values: dict[str, float]) -> dict[str, str]:
    ingredient_name = candidate["ingredient_name_normalized"]
    profile = ADDITION_PROFILES[ingredient_name]
    row = {field: "" for field in fieldnames}

    row["food_id"] = candidate["suggested_food_id"]
    row["canonical_name"] = candidate["suggested_canonical_name"]
    row["display_name"] = candidate["suggested_display_name"]
    row["food_family_name"] = profile["food_family_name"]
    row["entity_level"] = profile["entity_level"]
    row["food_group"] = profile["food_group"]
    row["food_subgroup"] = profile["food_subgroup"]
    row["food_subgroup_detail"] = profile["food_subgroup_detail"]
    row["processing_state"] = profile["processing_state"]
    row["preservation_state"] = ""

    row["energy_kcal_100g"] = format_number(macro_values["energy"])
    row["protein_g_100g"] = format_number(macro_values["protein"])
    row["carbs_g_100g"] = format_number(macro_values["carbs"])
    row["fat_g_100g"] = format_number(macro_values["fat"])

    row["helper_macro_profile"] = compute_macro_profile(
        macro_values["protein"],
        macro_values["carbs"],
        macro_values["fat"],
    )
    for field in (
        "helper_use_as_protein",
        "helper_use_as_carb_side",
        "helper_use_as_veg_side",
        "helper_is_sweet",
        "helper_is_salty",
        "helper_is_vegetarian",
        "helper_is_vegan",
        "helper_protein_bucket",
        "helper_carb_bucket",
        "helper_veg_bucket",
    ):
        row[field] = profile[field]

    row["helper_is_drink"] = ""
    row["primary_source_uid"] = extract_source_uid(candidate.get("source_row_reference", ""))
    row["primary_source_name"] = candidate.get("suggested_display_name", "")
    row["primary_source_ciqual_code"] = ""
    row["primary_source_name_tags"] = "fooddb_v1_1_draft_gap_addition"
    row["qc_macro_complete"] = "True"
    row["qc_taxonomy_complete"] = "True"
    row["qc_canonicalization_status"] = "normalized_from_source_name"
    row["qc_scope_status"] = "accepted_core"
    row["qc_source_merge_count"] = "1"
    row["qc_notes"] = (
        "v1_1_draft_gap_addition; "
        f"source_used={candidate.get('source_used', '')}; "
        f"source_row_reference={candidate.get('source_row_reference', '')}; "
        f"candidate_safety={candidate.get('safety', '')}; "
        "approved_minimal_pass"
    )
    return row


def build_audit_row(
    candidate: dict[str, str],
    final_action: str,
    sanity_notes: list[str],
    macro_values: dict[str, float] | None = None,
    defer_reason: str = "",
) -> dict[str, object]:
    return {
        "ingredient_name_normalized": candidate.get("ingredient_name_normalized", ""),
        "suggested_food_id": candidate.get("suggested_food_id", ""),
        "suggested_canonical_name": candidate.get("suggested_canonical_name", ""),
        "suggested_display_name": candidate.get("suggested_display_name", ""),
        "suggested_food_group": candidate.get("suggested_food_group", ""),
        "suggested_role": candidate.get("suggested_role", ""),
        "source_used": candidate.get("source_used", ""),
        "source_row_reference": candidate.get("source_row_reference", ""),
        "energy_kcal_100g": format_number((macro_values or {}).get("energy")),
        "protein_g_100g": format_number((macro_values or {}).get("protein")),
        "carbs_g_100g": format_number((macro_values or {}).get("carbs")),
        "fat_g_100g": format_number((macro_values or {}).get("fat")),
        "frequency_in_v1_1": candidate.get("frequency_in_v1_1", ""),
        "rows_with_grams": candidate.get("rows_with_grams", ""),
        "total_grams_affected": candidate.get("total_grams_affected", ""),
        "proposed_action": candidate.get("proposed_action", ""),
        "final_action": final_action,
        "safety": candidate.get("safety", ""),
        "sanity_status": "passed"
        if final_action
        in {
            "applied_to_v1_1_draft",
            "applied_to_round2_draft",
            "alias_to_existing_round2",
            "alias_to_existing_round3",
            "alias_to_existing_round5",
            "alias_to_existing_round7",
            "alias_to_existing_round8",
            "applied_to_round9_draft",
            "alias_to_existing_round9",
        }
        else "not_applied",
        "sanity_notes": "; ".join(sanity_notes),
        "defer_reason": defer_reason,
        "review_notes": candidate.get("review_notes", ""),
        "example_raw_texts": candidate.get("example_raw_texts", ""),
        "example_recipes": candidate.get("example_recipes", ""),
    }


def load_review_rows(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    rows, _ = read_csv(path)
    return {row.get("ingredient_name_normalized", ""): row for row in rows}


def build_draft(
    current_path: Path,
    candidates_path: Path,
    review_path: Path,
    out_draft_path: Path,
    out_applied_path: Path,
    out_deferred_path: Path,
    out_summary_path: Path,
) -> dict[str, object]:
    current_rows, fooddb_fields = read_csv(current_path)
    candidate_rows, _ = read_csv(candidates_path)
    review_by_name = load_review_rows(review_path)

    current_food_ids = {row.get("food_id", "") for row in current_rows}
    current_canonical_names = {row.get("canonical_name", "") for row in current_rows}

    add_candidates = [
        row
        for row in candidate_rows
        if row.get("proposed_action") == "add_to_fooddb_draft"
        or row.get("ingredient_name_normalized") in APPROVED_ADDITIONS
    ]

    candidate_by_name = {row.get("ingredient_name_normalized", ""): row for row in add_candidates}
    applied_rows: list[dict[str, object]] = []
    deferred_rows: list[dict[str, object]] = []
    new_fooddb_rows: list[dict[str, str]] = []

    for ingredient_name in sorted(candidate_by_name):
        candidate = dict(candidate_by_name[ingredient_name])
        if ingredient_name in review_by_name:
            candidate.setdefault("review_notes", review_by_name[ingredient_name].get("review_notes", ""))

        sanity_ok, sanity_notes, macro_values = sanity_check(candidate)
        suggested_food_id = candidate.get("suggested_food_id", "")
        suggested_canonical_name = candidate.get("suggested_canonical_name", "")

        if ingredient_name not in APPROVED_ADDITIONS:
            reason = "not_approved_for_minimal_v1_1_pass"
            if ingredient_name in EXPLICITLY_DEFERRED:
                reason = "explicitly_deferred_by_current_task"
            deferred_rows.append(build_audit_row(candidate, "deferred", sanity_notes, macro_values, reason))
            continue

        if suggested_food_id in current_food_ids or suggested_canonical_name in current_canonical_names:
            deferred_rows.append(
                build_audit_row(candidate, "deferred", sanity_notes, macro_values, "already_exists_in_current_fooddb")
            )
            continue

        if not sanity_ok:
            deferred_rows.append(
                build_audit_row(candidate, "deferred", sanity_notes, macro_values, "failed_macro_sanity_checks")
            )
            continue

        new_row = build_fooddb_row(candidate, fooddb_fields, macro_values)
        new_fooddb_rows.append(new_row)
        applied_rows.append(build_audit_row(candidate, "applied_to_v1_1_draft", sanity_notes, macro_values))

    draft_rows = current_rows + new_fooddb_rows

    audit_fields = [
        "ingredient_name_normalized",
        "suggested_food_id",
        "suggested_canonical_name",
        "suggested_display_name",
        "suggested_food_group",
        "suggested_role",
        "source_used",
        "source_row_reference",
        "energy_kcal_100g",
        "protein_g_100g",
        "carbs_g_100g",
        "fat_g_100g",
        "frequency_in_v1_1",
        "rows_with_grams",
        "total_grams_affected",
        "proposed_action",
        "final_action",
        "safety",
        "sanity_status",
        "sanity_notes",
        "defer_reason",
        "review_notes",
        "example_raw_texts",
        "example_recipes",
    ]

    write_csv(out_draft_path, draft_rows, fooddb_fields)
    write_csv(out_applied_path, applied_rows, audit_fields)
    write_csv(out_deferred_path, deferred_rows, audit_fields)
    write_summary(out_summary_path, current_rows, applied_rows, deferred_rows, draft_rows)

    return {
        "current_count": len(current_rows),
        "applied_count": len(applied_rows),
        "deferred_count": len(deferred_rows),
        "final_count": len(draft_rows),
        "applied_rows": applied_rows,
        "deferred_rows": deferred_rows,
    }


def build_round2_draft(
    base_fooddb_path: Path,
    candidates_path: Path,
    review_path: Path,
    out_draft_path: Path,
    out_applied_path: Path,
    out_deferred_path: Path,
    out_summary_path: Path,
) -> dict[str, object]:
    base_rows, fooddb_fields = read_csv(base_fooddb_path)
    candidate_rows, _ = read_csv(candidates_path)
    review_by_name = load_review_rows(review_path)
    candidate_by_name = {row.get("ingredient_name_normalized", ""): row for row in candidate_rows}
    current_food_ids = {row.get("food_id", "") for row in base_rows}
    current_canonical_names = {row.get("canonical_name", "") for row in base_rows}

    applied_rows: list[dict[str, object]] = []
    deferred_rows: list[dict[str, object]] = []
    new_fooddb_rows: list[dict[str, str]] = []
    processed_names: set[str] = set()

    for ingredient_name, food_id in sorted(ROUND2_EXISTING_ALIAS_TARGETS.items()):
        processed_names.add(ingredient_name)
        food_row = find_food_row_by_id(base_rows, food_id)
        source_candidate = candidate_by_name.get(ingredient_name) or review_by_name.get(ingredient_name)
        if not food_row:
            deferred_rows.append(
                build_audit_row(
                    source_candidate or {"ingredient_name_normalized": ingredient_name},
                    "deferred",
                    ["round2_existing_alias_target_missing"],
                    None,
                    "safe_existing_fooddb_item_not_found",
                )
            )
            continue

        macro_values = macro_values_from_fooddb_row(food_row)
        if not macro_values:
            deferred_rows.append(
                build_audit_row(
                    build_alias_candidate(ingredient_name, food_row, source_candidate),
                    "deferred",
                    ["existing_fooddb_item_missing_macros"],
                    None,
                    "existing_fooddb_item_missing_required_macros",
                )
            )
            continue

        applied_rows.append(
            build_audit_row(
                build_alias_candidate(ingredient_name, food_row, source_candidate),
                "alias_to_existing_round2",
                ["round2_alias_to_existing; no_fooddb_row_appended"],
                macro_values,
                "",
            )
        )

    for ingredient_name in sorted(ROUND2_APPROVED_ADDITIONS):
        processed_names.add(ingredient_name)
        candidate = dict(candidate_by_name.get(ingredient_name, {}))
        if not candidate:
            deferred_rows.append(
                build_audit_row(
                    {"ingredient_name_normalized": ingredient_name},
                    "deferred",
                    ["round2_candidate_missing"],
                    None,
                    "missing_gap_candidate_row",
                )
            )
            continue
        if ingredient_name in review_by_name:
            candidate.setdefault("review_notes", review_by_name[ingredient_name].get("review_notes", ""))

        sanity_ok, sanity_notes, macro_values = sanity_check(candidate)
        suggested_food_id = candidate.get("suggested_food_id", "")
        suggested_canonical_name = candidate.get("suggested_canonical_name", "")

        if suggested_food_id in current_food_ids or suggested_canonical_name in current_canonical_names:
            applied_rows.append(
                build_audit_row(candidate, "alias_to_existing_round2", sanity_notes, macro_values, "")
            )
            continue

        if not sanity_ok:
            deferred_rows.append(
                build_audit_row(candidate, "deferred", sanity_notes, macro_values, "failed_macro_sanity_checks")
            )
            continue

        new_row = build_fooddb_row(candidate, fooddb_fields, macro_values)
        new_fooddb_rows.append(new_row)
        current_food_ids.add(new_row.get("food_id", ""))
        current_canonical_names.add(new_row.get("canonical_name", ""))
        applied_rows.append(build_audit_row(candidate, "applied_to_round2_draft", sanity_notes, macro_values))

    for ingredient_name, reason in sorted(ROUND2_EXPLICITLY_DEFERRED.items()):
        if ingredient_name in processed_names:
            continue
        candidate = dict(candidate_by_name.get(ingredient_name, {}))
        if not candidate:
            candidate = {"ingredient_name_normalized": ingredient_name}
        sanity_ok, sanity_notes, macro_values = sanity_check(candidate) if candidate.get("energy_kcal_100") else (
            False,
            ["not_evaluated_for_round2"],
            {},
        )
        deferred_rows.append(build_audit_row(candidate, "deferred", sanity_notes, macro_values, reason))

    draft_rows = base_rows + new_fooddb_rows
    audit_fields = [
        "ingredient_name_normalized",
        "suggested_food_id",
        "suggested_canonical_name",
        "suggested_display_name",
        "suggested_food_group",
        "suggested_role",
        "source_used",
        "source_row_reference",
        "energy_kcal_100g",
        "protein_g_100g",
        "carbs_g_100g",
        "fat_g_100g",
        "frequency_in_v1_1",
        "rows_with_grams",
        "total_grams_affected",
        "proposed_action",
        "final_action",
        "safety",
        "sanity_status",
        "sanity_notes",
        "defer_reason",
        "review_notes",
        "example_raw_texts",
        "example_recipes",
    ]

    write_csv(out_draft_path, draft_rows, fooddb_fields)
    write_csv(out_applied_path, applied_rows, audit_fields)
    write_csv(out_deferred_path, deferred_rows, audit_fields)
    write_round2_summary(out_summary_path, base_rows, applied_rows, deferred_rows, draft_rows)

    return {
        "current_count": len(base_rows),
        "applied_count": len(applied_rows),
        "deferred_count": len(deferred_rows),
        "final_count": len(draft_rows),
        "applied_rows": applied_rows,
        "deferred_rows": deferred_rows,
    }


def build_round3_draft(
    base_fooddb_path: Path,
    out_draft_path: Path,
    out_applied_path: Path,
    out_deferred_path: Path,
    out_summary_path: Path,
) -> dict[str, object]:
    base_rows, fooddb_fields = read_csv(base_fooddb_path)
    applied_rows: list[dict[str, object]] = []
    deferred_rows: list[dict[str, object]] = []

    for alias_key, food_id in sorted(ROUND3_EXISTING_ALIAS_TARGETS.items()):
        food_row = find_food_row_by_id(base_rows, food_id)
        if not food_row:
            deferred_rows.append(
                build_audit_row(
                    {"ingredient_name_normalized": alias_key, "suggested_food_id": food_id},
                    "deferred",
                    ["round3_existing_alias_target_missing"],
                    None,
                    "safe_existing_fooddb_item_not_found",
                )
            )
            continue
        macro_values = macro_values_from_fooddb_row(food_row)
        if not macro_values:
            deferred_rows.append(
                build_audit_row(
                    build_alias_candidate(alias_key, food_row),
                    "deferred",
                    ["existing_fooddb_item_missing_macros"],
                    None,
                    "existing_fooddb_item_missing_required_macros",
                )
            )
            continue
        applied_rows.append(
            build_audit_row(
                build_alias_candidate(alias_key, food_row),
                "alias_to_existing_round3",
                ["round3_alias_to_existing; no_fooddb_row_appended"],
                macro_values,
            )
        )

    for ingredient_name, reason in sorted(ROUND3_EXPLICITLY_DEFERRED.items()):
        deferred_rows.append(
            build_audit_row(
                {"ingredient_name_normalized": ingredient_name},
                "deferred",
                ["not_evaluated_for_round3_append"],
                None,
                reason,
            )
        )

    audit_fields = [
        "ingredient_name_normalized",
        "suggested_food_id",
        "suggested_canonical_name",
        "suggested_display_name",
        "suggested_food_group",
        "suggested_role",
        "source_used",
        "source_row_reference",
        "energy_kcal_100g",
        "protein_g_100g",
        "carbs_g_100g",
        "fat_g_100g",
        "frequency_in_v1_1",
        "rows_with_grams",
        "total_grams_affected",
        "proposed_action",
        "final_action",
        "safety",
        "sanity_status",
        "sanity_notes",
        "defer_reason",
        "review_notes",
        "example_raw_texts",
        "example_recipes",
    ]

    write_csv(out_draft_path, base_rows, fooddb_fields)
    write_csv(out_applied_path, applied_rows, audit_fields)
    write_csv(out_deferred_path, deferred_rows, audit_fields)
    write_round3_summary(out_summary_path, base_rows, applied_rows, deferred_rows)

    return {
        "current_count": len(base_rows),
        "applied_count": len(applied_rows),
        "deferred_count": len(deferred_rows),
        "final_count": len(base_rows),
        "applied_rows": applied_rows,
        "deferred_rows": deferred_rows,
    }


def build_round5_draft(
    base_fooddb_path: Path,
    out_draft_path: Path,
    out_applied_path: Path,
    out_deferred_path: Path,
    out_summary_path: Path,
) -> dict[str, object]:
    base_rows, fooddb_fields = read_csv(base_fooddb_path)
    applied_rows: list[dict[str, object]] = []
    deferred_rows: list[dict[str, object]] = []

    for alias_key, food_id in sorted(ROUND5_EXISTING_ALIAS_TARGETS.items()):
        food_row = find_food_row_by_id(base_rows, food_id)
        if not food_row:
            deferred_rows.append(
                build_audit_row(
                    {"ingredient_name_normalized": alias_key, "suggested_food_id": food_id},
                    "deferred",
                    ["round5_existing_alias_target_missing"],
                    None,
                    "safe_existing_fooddb_item_not_found",
                )
            )
            continue
        macro_values = macro_values_from_fooddb_row(food_row)
        if not macro_values:
            deferred_rows.append(
                build_audit_row(
                    build_alias_candidate(alias_key, food_row),
                    "deferred",
                    ["existing_fooddb_item_missing_macros"],
                    None,
                    "existing_fooddb_item_missing_required_macros",
                )
            )
            continue
        applied_rows.append(
            build_audit_row(
                build_alias_candidate(alias_key, food_row),
                "alias_to_existing_round5",
                [ROUND5_ALIAS_NOTES.get(alias_key, "round5_manual_decision")],
                macro_values,
            )
        )

    for ingredient_name, reason in sorted(ROUND5_EXPLICITLY_DEFERRED.items()):
        deferred_rows.append(
            build_audit_row(
                {"ingredient_name_normalized": ingredient_name},
                "deferred",
                ["not_appended_in_round5_manual_decision_pass"],
                None,
                reason,
            )
        )

    audit_fields = [
        "ingredient_name_normalized",
        "suggested_food_id",
        "suggested_canonical_name",
        "suggested_display_name",
        "suggested_food_group",
        "suggested_role",
        "source_used",
        "source_row_reference",
        "energy_kcal_100g",
        "protein_g_100g",
        "carbs_g_100g",
        "fat_g_100g",
        "frequency_in_v1_1",
        "rows_with_grams",
        "total_grams_affected",
        "proposed_action",
        "final_action",
        "safety",
        "sanity_status",
        "sanity_notes",
        "defer_reason",
        "review_notes",
        "example_raw_texts",
        "example_recipes",
    ]

    write_csv(out_draft_path, base_rows, fooddb_fields)
    write_csv(out_applied_path, applied_rows, audit_fields)
    write_csv(out_deferred_path, deferred_rows, audit_fields)
    write_round5_summary(out_summary_path, base_rows, applied_rows, deferred_rows)

    return {
        "current_count": len(base_rows),
        "applied_count": len(applied_rows),
        "deferred_count": len(deferred_rows),
        "final_count": len(base_rows),
        "applied_rows": applied_rows,
        "deferred_rows": deferred_rows,
    }


def build_round7_draft(
    base_fooddb_path: Path,
    out_draft_path: Path,
    out_applied_path: Path,
    out_deferred_path: Path,
    out_summary_path: Path,
) -> dict[str, object]:
    base_rows, fooddb_fields = read_csv(base_fooddb_path)
    applied_rows: list[dict[str, object]] = []
    deferred_rows: list[dict[str, object]] = []

    for alias_key, food_id in sorted(ROUND7_EXISTING_ALIAS_TARGETS.items()):
        food_row = find_food_row_by_id(base_rows, food_id)
        source_candidate = {
            "ingredient_name_normalized": alias_key,
            "suggested_food_id": food_id,
            "safety": ROUND7_ALIAS_SAFETY.get(alias_key, "needs_review"),
            "review_notes": ROUND7_ALIAS_NOTES.get(alias_key, "round7_macro_blocker"),
        }
        if not food_row:
            deferred_rows.append(
                build_audit_row(
                    source_candidate,
                    "deferred",
                    ["round7_existing_alias_target_missing"],
                    None,
                    "safe_existing_fooddb_item_not_found",
                )
            )
            continue
        macro_values = macro_values_from_fooddb_row(food_row)
        if not macro_values:
            deferred_rows.append(
                build_audit_row(
                    build_alias_candidate(alias_key, food_row, source_candidate),
                    "deferred",
                    ["existing_fooddb_item_missing_macros"],
                    None,
                    "existing_fooddb_item_missing_required_macros",
                )
            )
            continue
        applied_rows.append(
            build_audit_row(
                build_alias_candidate(alias_key, food_row, source_candidate),
                "alias_to_existing_round7",
                [ROUND7_ALIAS_NOTES.get(alias_key, "round7_macro_blocker")],
                macro_values,
            )
        )

    for ingredient_name, reason in sorted(ROUND7_EXPLICITLY_DEFERRED.items()):
        deferred_rows.append(
            build_audit_row(
                {"ingredient_name_normalized": ingredient_name, "safety": "needs_review"},
                "deferred",
                ["not_appended_in_round7_macro_blocker_pass"],
                None,
                reason,
            )
        )

    audit_fields = [
        "ingredient_name_normalized",
        "suggested_food_id",
        "suggested_canonical_name",
        "suggested_display_name",
        "suggested_food_group",
        "suggested_role",
        "source_used",
        "source_row_reference",
        "energy_kcal_100g",
        "protein_g_100g",
        "carbs_g_100g",
        "fat_g_100g",
        "frequency_in_v1_1",
        "rows_with_grams",
        "total_grams_affected",
        "proposed_action",
        "final_action",
        "safety",
        "sanity_status",
        "sanity_notes",
        "defer_reason",
        "review_notes",
        "example_raw_texts",
        "example_recipes",
    ]

    write_csv(out_draft_path, base_rows, fooddb_fields)
    write_csv(out_applied_path, applied_rows, audit_fields)
    write_csv(out_deferred_path, deferred_rows, audit_fields)
    write_round7_summary(out_summary_path, base_rows, applied_rows, deferred_rows)

    return {
        "current_count": len(base_rows),
        "applied_count": len(applied_rows),
        "deferred_count": len(deferred_rows),
        "final_count": len(base_rows),
        "applied_rows": applied_rows,
        "deferred_rows": deferred_rows,
    }


def build_round8_draft(
    base_fooddb_path: Path,
    out_draft_path: Path,
    out_applied_path: Path,
    out_deferred_path: Path,
    out_summary_path: Path,
) -> dict[str, object]:
    base_rows, fooddb_fields = read_csv(base_fooddb_path)
    applied_rows: list[dict[str, object]] = []
    deferred_rows: list[dict[str, object]] = []

    for alias_key, food_id in sorted(ROUND8_EXISTING_ALIAS_TARGETS.items()):
        food_row = find_food_row_by_id(base_rows, food_id)
        source_candidate = {
            "ingredient_name_normalized": alias_key,
            "suggested_food_id": food_id,
            "safety": ROUND8_ALIAS_SAFETY.get(alias_key, "needs_review"),
            "review_notes": ROUND8_ALIAS_NOTES.get(alias_key, "round8_punctual_mapping"),
        }
        if not food_row:
            deferred_rows.append(
                build_audit_row(
                    source_candidate,
                    "deferred",
                    ["round8_existing_alias_target_missing"],
                    None,
                    "safe_existing_fooddb_item_not_found",
                )
            )
            continue
        macro_values = macro_values_from_fooddb_row(food_row)
        if not macro_values:
            deferred_rows.append(
                build_audit_row(
                    build_alias_candidate(alias_key, food_row, source_candidate),
                    "deferred",
                    ["existing_fooddb_item_missing_macros"],
                    None,
                    "existing_fooddb_item_missing_required_macros",
                )
            )
            continue
        applied_rows.append(
            build_audit_row(
                build_alias_candidate(alias_key, food_row, source_candidate),
                "alias_to_existing_round8",
                [ROUND8_ALIAS_NOTES.get(alias_key, "round8_punctual_mapping")],
                macro_values,
            )
        )

    for ingredient_name, reason in sorted(ROUND8_EXPLICITLY_DEFERRED.items()):
        deferred_rows.append(
            build_audit_row(
                {"ingredient_name_normalized": ingredient_name, "safety": "needs_review"},
                "deferred",
                ["not_appended_in_round8_unit_rules_punctual_mapping_pass"],
                None,
                reason,
            )
        )

    audit_fields = [
        "ingredient_name_normalized",
        "suggested_food_id",
        "suggested_canonical_name",
        "suggested_display_name",
        "suggested_food_group",
        "suggested_role",
        "source_used",
        "source_row_reference",
        "energy_kcal_100g",
        "protein_g_100g",
        "carbs_g_100g",
        "fat_g_100g",
        "frequency_in_v1_1",
        "rows_with_grams",
        "total_grams_affected",
        "proposed_action",
        "final_action",
        "safety",
        "sanity_status",
        "sanity_notes",
        "defer_reason",
        "review_notes",
        "example_raw_texts",
        "example_recipes",
    ]

    write_csv(out_draft_path, base_rows, fooddb_fields)
    write_csv(out_applied_path, applied_rows, audit_fields)
    write_csv(out_deferred_path, deferred_rows, audit_fields)
    write_round8_summary(out_summary_path, base_rows, applied_rows, deferred_rows)

    return {
        "current_count": len(base_rows),
        "applied_count": len(applied_rows),
        "deferred_count": len(deferred_rows),
        "final_count": len(base_rows),
        "applied_rows": applied_rows,
        "deferred_rows": deferred_rows,
    }


def build_round9_draft(
    base_fooddb_path: Path,
    out_draft_path: Path,
    out_applied_path: Path,
    out_deferred_path: Path,
    out_summary_path: Path,
) -> dict[str, object]:
    base_rows, fooddb_fields = read_csv(base_fooddb_path)
    current_food_ids = {row.get("food_id", "") for row in base_rows}
    current_canonical_names = {row.get("canonical_name", "") for row in base_rows}
    applied_rows: list[dict[str, object]] = []
    deferred_rows: list[dict[str, object]] = []
    new_fooddb_rows: list[dict[str, str]] = []

    for ingredient_name, source_candidate in sorted(ROUND9_SOURCE_ADDITIONS.items()):
        candidate = dict(source_candidate)
        candidate["ingredient_name_normalized"] = ingredient_name
        sanity_ok, sanity_notes, macro_values = sanity_check(candidate)
        suggested_food_id = candidate.get("suggested_food_id", "")
        suggested_canonical_name = candidate.get("suggested_canonical_name", "")
        if suggested_food_id in current_food_ids or suggested_canonical_name in current_canonical_names:
            deferred_rows.append(
                build_audit_row(candidate, "deferred", sanity_notes, macro_values, "already_exists_in_round8_fooddb")
            )
            continue
        if not sanity_ok:
            deferred_rows.append(
                build_audit_row(candidate, "deferred", sanity_notes, macro_values, "failed_macro_sanity_checks")
            )
            continue

        new_row = build_fooddb_row(candidate, fooddb_fields, macro_values)
        new_fooddb_rows.append(new_row)
        applied_rows.append(build_audit_row(candidate, "applied_to_round9_draft", sanity_notes, macro_values))

    draft_rows = base_rows + new_fooddb_rows

    for ingredient_name, food_id in sorted(ROUND9_EXISTING_ALIAS_TARGETS.items()):
        food_row = find_food_row_by_id(draft_rows, food_id)
        source_candidate = {
            "ingredient_name_normalized": ingredient_name,
            "suggested_food_id": food_id,
            "safety": "safe_auto",
            "review_notes": ROUND9_ALIAS_NOTES.get(ingredient_name, "round9_final_mapping"),
        }
        if not food_row:
            deferred_rows.append(
                build_audit_row(
                    source_candidate,
                    "deferred",
                    ["round9_existing_alias_target_missing"],
                    None,
                    "safe_existing_fooddb_item_not_found",
                )
            )
            continue
        macro_values = macro_values_from_fooddb_row(food_row)
        if not macro_values:
            deferred_rows.append(
                build_audit_row(
                    build_alias_candidate(ingredient_name, food_row, source_candidate),
                    "deferred",
                    ["existing_fooddb_item_missing_macros"],
                    None,
                    "existing_fooddb_item_missing_required_macros",
                )
            )
            continue
        applied_rows.append(
            build_audit_row(
                build_alias_candidate(ingredient_name, food_row, source_candidate),
                "alias_to_existing_round9",
                [ROUND9_ALIAS_NOTES.get(ingredient_name, "round9_final_mapping")],
                macro_values,
            )
        )

    for ingredient_name, reason in sorted(ROUND9_EXPLICITLY_DEFERRED.items()):
        deferred_rows.append(
            build_audit_row(
                {"ingredient_name_normalized": ingredient_name, "safety": "needs_review"},
                "deferred",
                ["not_appended_in_round9_final_mapping_pass"],
                None,
                reason,
            )
        )

    audit_fields = [
        "ingredient_name_normalized",
        "suggested_food_id",
        "suggested_canonical_name",
        "suggested_display_name",
        "suggested_food_group",
        "suggested_role",
        "source_used",
        "source_row_reference",
        "energy_kcal_100g",
        "protein_g_100g",
        "carbs_g_100g",
        "fat_g_100g",
        "frequency_in_v1_1",
        "rows_with_grams",
        "total_grams_affected",
        "proposed_action",
        "final_action",
        "safety",
        "sanity_status",
        "sanity_notes",
        "defer_reason",
        "review_notes",
        "example_raw_texts",
        "example_recipes",
    ]

    write_csv(out_draft_path, draft_rows, fooddb_fields)
    write_csv(out_applied_path, applied_rows, audit_fields)
    write_csv(out_deferred_path, deferred_rows, audit_fields)
    write_round9_summary(out_summary_path, base_rows, new_fooddb_rows, applied_rows, deferred_rows)

    return {
        "current_count": len(base_rows),
        "applied_count": len(applied_rows),
        "deferred_count": len(deferred_rows),
        "final_count": len(draft_rows),
        "appended_count": len(new_fooddb_rows),
        "applied_rows": applied_rows,
        "deferred_rows": deferred_rows,
    }


def write_summary(
    path: Path,
    current_rows: list[dict[str, str]],
    applied_rows: list[dict[str, object]],
    deferred_rows: list[dict[str, object]],
    draft_rows: list[dict[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("Food_DB v1.1 draft minimal additions summary")
    lines.append("=" * 48)
    lines.append("")
    lines.append(f"Current Food_DB row count: {len(current_rows)}")
    lines.append(f"Approved additions requested: {len(APPROVED_ADDITIONS)}")
    lines.append(f"Additions applied: {len(applied_rows)}")
    lines.append(f"Additions deferred: {len(deferred_rows)}")
    lines.append(f"Final Food_DB v1.1 draft row count: {len(draft_rows)}")
    lines.append("")

    lines.append("Applied rows")
    lines.append("-" * 12)
    if applied_rows:
        for row in applied_rows:
            lines.append(
                f"- {row['suggested_food_id']} | {row['suggested_canonical_name']} | "
                f"{row['energy_kcal_100g']} kcal, {row['protein_g_100g']}g protein, "
                f"{row['carbs_g_100g']}g carbs, {row['fat_g_100g']}g fat / 100g | "
                f"source={row['source_row_reference']}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.append("Deferred rows")
    lines.append("-" * 13)
    if deferred_rows:
        for row in deferred_rows:
            lines.append(
                f"- {row['ingredient_name_normalized']} | reason={row['defer_reason']} | "
                f"sanity={row['sanity_notes']}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.append("Recommended next patch")
    lines.append("-" * 22)
    lines.append(
        "- Rerun the Recipes_DB v1.1 draft mapping against this Food_DB v1.1 draft, "
        "then do the unit-to-grams rules pass before any nutrition cache rebuild."
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_round2_summary(
    path: Path,
    base_rows: list[dict[str, str]],
    applied_rows: list[dict[str, object]],
    deferred_rows: list[dict[str, object]],
    draft_rows: list[dict[str, str]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    appended_rows = [row for row in applied_rows if row.get("final_action") == "applied_to_round2_draft"]
    alias_rows = [row for row in applied_rows if row.get("final_action") == "alias_to_existing_round2"]

    lines: list[str] = []
    lines.append("Food_DB v1.1 round2 blocker-resolution summary")
    lines.append("=" * 54)
    lines.append("")
    lines.append(f"Base Food_DB v1.1 draft row count: {len(base_rows)}")
    lines.append(f"Round2 items considered: {len(ROUND2_EXISTING_ALIAS_TARGETS) + len(ROUND2_APPROVED_ADDITIONS)}")
    lines.append(f"Round2 alias_to_existing applied: {len(alias_rows)}")
    lines.append(f"Round2 new rows appended: {len(appended_rows)}")
    lines.append(f"Round2 deferred rows: {len(deferred_rows)}")
    lines.append(f"Final Food_DB v1.1 round2 draft row count: {len(draft_rows)}")
    lines.append("")

    lines.append("Round2 applied")
    lines.append("-" * 14)
    if applied_rows:
        for row in applied_rows:
            lines.append(
                f"- {row['ingredient_name_normalized']} | {row['final_action']} | "
                f"{row['suggested_food_id']} | {row['suggested_canonical_name']} | "
                f"{row['energy_kcal_100g']} kcal, {row['protein_g_100g']}g protein, "
                f"{row['carbs_g_100g']}g carbs, {row['fat_g_100g']}g fat / 100g"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.append("Round2 deferred")
    lines.append("-" * 15)
    if deferred_rows:
        for row in deferred_rows:
            lines.append(
                f"- {row['ingredient_name_normalized']} | reason={row['defer_reason']} | "
                f"sanity={row['sanity_notes']}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.append("Recommended next patch")
    lines.append("-" * 22)
    lines.append(
        "- Rerun Recipes_DB v1.1 mapping against the round2 Food_DB draft, then decide whether "
        "coverage is sufficient for a first nutrition cache draft."
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_round3_summary(
    path: Path,
    base_rows: list[dict[str, str]],
    applied_rows: list[dict[str, object]],
    deferred_rows: list[dict[str, object]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("Food_DB v1.1 round3 targeted blocker summary")
    lines.append("=" * 52)
    lines.append("")
    lines.append(f"Base Food_DB round2 row count: {len(base_rows)}")
    lines.append("Round3 new rows appended: 0")
    lines.append(f"Round3 alias_to_existing decisions: {len(applied_rows)}")
    lines.append(f"Round3 deferred decisions: {len(deferred_rows)}")
    lines.append(f"Final Food_DB round3 draft row count: {len(base_rows)}")
    lines.append("")

    lines.append("Round3 alias_to_existing decisions")
    lines.append("-" * 36)
    for row in applied_rows:
        lines.append(
            f"- {row['ingredient_name_normalized']} -> {row['suggested_food_id']} | "
            f"{row['energy_kcal_100g']} kcal, {row['protein_g_100g']}g protein, "
            f"{row['carbs_g_100g']}g carbs, {row['fat_g_100g']}g fat / 100g"
        )
    lines.append("")

    lines.append("Round3 deferred")
    lines.append("-" * 15)
    for row in deferred_rows:
        lines.append(f"- {row['ingredient_name_normalized']} | reason={row['defer_reason']}")
    lines.append("")
    lines.append("Recommended next patch")
    lines.append("-" * 22)
    lines.append("- Rerun mapping with round3 targeted blocker promotions, then rebuild nutrition cache draft.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_round5_summary(
    path: Path,
    base_rows: list[dict[str, str]],
    applied_rows: list[dict[str, object]],
    deferred_rows: list[dict[str, object]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("Food_DB v1.1 round5 manual-decision summary")
    lines.append("=" * 54)
    lines.append("")
    lines.append(f"Base Food_DB round3 row count: {len(base_rows)}")
    lines.append("Round5 new rows appended: 0")
    lines.append(f"Round5 alias/manual decisions applied: {len(applied_rows)}")
    lines.append(f"Round5 deferred decisions: {len(deferred_rows)}")
    lines.append(f"Final Food_DB round5 draft row count: {len(base_rows)}")
    lines.append("")

    lines.append("Round5 applied")
    lines.append("-" * 14)
    for row in applied_rows:
        lines.append(
            f"- {row['ingredient_name_normalized']} -> {row['suggested_food_id']} | "
            f"{row['energy_kcal_100g']} kcal, {row['protein_g_100g']}g protein, "
            f"{row['carbs_g_100g']}g carbs, {row['fat_g_100g']}g fat / 100g | "
            f"notes={row['sanity_notes']}"
        )
    lines.append("")

    lines.append("Round5 deferred")
    lines.append("-" * 15)
    for row in deferred_rows:
        lines.append(f"- {row['ingredient_name_normalized']} | reason={row['defer_reason']}")
    lines.append("")
    lines.append("Recommended next patch")
    lines.append("-" * 22)
    lines.append(
        "- Rerun mapping with round5 manual decisions, then rebuild nutrition cache draft with edible-yield support."
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_round7_summary(
    path: Path,
    base_rows: list[dict[str, str]],
    applied_rows: list[dict[str, object]],
    deferred_rows: list[dict[str, object]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("Food_DB v1.1 round7 macro-blocker summary")
    lines.append("=" * 50)
    lines.append("")
    lines.append(f"Base Food_DB round5 row count: {len(base_rows)}")
    lines.append("Round7 new rows appended: 0")
    lines.append(f"Round7 alias/manual decisions applied: {len(applied_rows)}")
    lines.append(f"Round7 deferred decisions: {len(deferred_rows)}")
    lines.append(f"Final Food_DB round7 draft row count: {len(base_rows)}")
    lines.append("")

    lines.append("Round7 applied")
    lines.append("-" * 14)
    if applied_rows:
        for row in applied_rows:
            lines.append(
                f"- {row['ingredient_name_normalized']} -> {row['suggested_food_id']} | "
                f"{row['energy_kcal_100g']} kcal, {row['protein_g_100g']}g protein, "
                f"{row['carbs_g_100g']}g carbs, {row['fat_g_100g']}g fat / 100g | "
                f"safety={row['safety']} | notes={row['sanity_notes']}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.append("Round7 deferred")
    lines.append("-" * 15)
    if deferred_rows:
        for row in deferred_rows:
            lines.append(f"- {row['ingredient_name_normalized']} | reason={row['defer_reason']}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Recommended next patch")
    lines.append("-" * 22)
    lines.append("- Rerun mapping with round7 macro-blocker decisions, then rebuild nutrition cache draft.")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_round8_summary(
    path: Path,
    base_rows: list[dict[str, str]],
    applied_rows: list[dict[str, object]],
    deferred_rows: list[dict[str, object]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    lines.append("Food_DB v1.1 round8 unit-rules/punctual-mapping summary")
    lines.append("=" * 62)
    lines.append("")
    lines.append(f"Base Food_DB round7 row count: {len(base_rows)}")
    lines.append("Round8 new rows appended: 0")
    lines.append(f"Round8 alias/manual decisions applied: {len(applied_rows)}")
    lines.append(f"Round8 deferred decisions: {len(deferred_rows)}")
    lines.append(f"Final Food_DB round8 draft row count: {len(base_rows)}")
    lines.append("")

    lines.append("Round8 applied")
    lines.append("-" * 14)
    if applied_rows:
        for row in applied_rows:
            lines.append(
                f"- {row['ingredient_name_normalized']} -> {row['suggested_food_id']} | "
                f"{row['energy_kcal_100g']} kcal, {row['protein_g_100g']}g protein, "
                f"{row['carbs_g_100g']}g carbs, {row['fat_g_100g']}g fat / 100g | "
                f"safety={row['safety']} | notes={row['sanity_notes']}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.append("Round8 deferred")
    lines.append("-" * 15)
    if deferred_rows:
        for row in deferred_rows:
            lines.append(f"- {row['ingredient_name_normalized']} | reason={row['defer_reason']}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Recommended next patch")
    lines.append("-" * 22)
    lines.append(
        "- Rerun mapping with round8 punctual decisions and round8 unit rules, then rebuild nutrition cache draft."
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_round9_summary(
    path: Path,
    base_rows: list[dict[str, str]],
    new_fooddb_rows: list[dict[str, str]],
    applied_rows: list[dict[str, object]],
    deferred_rows: list[dict[str, object]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    alias_rows = [row for row in applied_rows if row.get("final_action") == "alias_to_existing_round9"]
    appended_rows = [row for row in applied_rows if row.get("final_action") == "applied_to_round9_draft"]

    lines: list[str] = []
    lines.append("Food_DB v1.1 round9 final mapping/source summary")
    lines.append("=" * 56)
    lines.append("")
    lines.append(f"Base Food_DB round8 row count: {len(base_rows)}")
    lines.append(f"Round9 new rows appended: {len(new_fooddb_rows)}")
    lines.append(f"Round9 source additions applied: {len(appended_rows)}")
    lines.append(f"Round9 alias/manual decisions applied: {len(alias_rows)}")
    lines.append(f"Round9 deferred decisions: {len(deferred_rows)}")
    lines.append(f"Final Food_DB round9 draft row count: {len(base_rows) + len(new_fooddb_rows)}")
    lines.append("")

    lines.append("Round9 applied")
    lines.append("-" * 14)
    if applied_rows:
        for row in applied_rows:
            lines.append(
                f"- {row['ingredient_name_normalized']} | {row['final_action']} | "
                f"{row['suggested_food_id']} | {row['suggested_canonical_name']} | "
                f"{row['energy_kcal_100g']} kcal, {row['protein_g_100g']}g protein, "
                f"{row['carbs_g_100g']}g carbs, {row['fat_g_100g']}g fat / 100g | "
                f"safety={row['safety']} | notes={row['sanity_notes']}"
            )
    else:
        lines.append("- none")
    lines.append("")

    lines.append("Round9 deferred")
    lines.append("-" * 15)
    if deferred_rows:
        for row in deferred_rows:
            lines.append(f"- {row['ingredient_name_normalized']} | reason={row['defer_reason']}")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Recommended next patch")
    lines.append("-" * 22)
    lines.append(
        "- Rerun mapping/nutrition cache with round9, then stop mapping passes unless the gain is unexpectedly large."
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a minimal Food_DB v1.1 draft from safe gap additions.")
    parser.add_argument("--round2", action="store_true", help="Build the round2 draft/audit outputs.")
    parser.add_argument("--round3", action="store_true", help="Build the round3 targeted blocker draft/audit outputs.")
    parser.add_argument("--round5", action="store_true", help="Build the round5 manual-decision draft/audit outputs.")
    parser.add_argument("--round7", action="store_true", help="Build the round7 macro-blocker draft/audit outputs.")
    parser.add_argument("--round8", action="store_true", help="Build the round8 unit-rules/punctual-mapping draft/audit outputs.")
    parser.add_argument("--round9", action="store_true", help="Build the round9 final mapping/source draft/audit outputs.")
    parser.add_argument("--current_fooddb", default=str(CURRENT_FOODDB_PATH))
    parser.add_argument("--gap_candidates", default=str(GAP_CANDIDATES_PATH))
    parser.add_argument("--gap_review", default=str(GAP_REVIEW_PATH))
    parser.add_argument("--out_draft", default=str(OUT_DRAFT_PATH))
    parser.add_argument("--out_applied", default=str(OUT_APPLIED_PATH))
    parser.add_argument("--out_deferred", default=str(OUT_DEFERRED_PATH))
    parser.add_argument("--out_summary", default=str(OUT_SUMMARY_PATH))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.round9:
        result = build_round9_draft(
            ROUND8_OUT_DRAFT_PATH if args.current_fooddb == str(CURRENT_FOODDB_PATH) else Path(args.current_fooddb),
            ROUND9_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else Path(args.out_draft),
            ROUND9_OUT_APPLIED_PATH if args.out_applied == str(OUT_APPLIED_PATH) else Path(args.out_applied),
            ROUND9_OUT_DEFERRED_PATH if args.out_deferred == str(OUT_DEFERRED_PATH) else Path(args.out_deferred),
            ROUND9_OUT_SUMMARY_PATH if args.out_summary == str(OUT_SUMMARY_PATH) else Path(args.out_summary),
        )
        print("Food_DB v1.1 round9 draft built")
        print(f"base_rows={result['current_count']}")
        print(f"round9_new_rows={result['appended_count']}")
        print(f"round9_applied_decisions={result['applied_count']}")
        print(f"round9_deferred={result['deferred_count']}")
        print(f"final_rows={result['final_count']}")
        print(f"draft={ROUND9_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else args.out_draft}")
        return

    if args.round8:
        result = build_round8_draft(
            ROUND7_OUT_DRAFT_PATH if args.current_fooddb == str(CURRENT_FOODDB_PATH) else Path(args.current_fooddb),
            ROUND8_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else Path(args.out_draft),
            ROUND8_OUT_APPLIED_PATH if args.out_applied == str(OUT_APPLIED_PATH) else Path(args.out_applied),
            ROUND8_OUT_DEFERRED_PATH if args.out_deferred == str(OUT_DEFERRED_PATH) else Path(args.out_deferred),
            ROUND8_OUT_SUMMARY_PATH if args.out_summary == str(OUT_SUMMARY_PATH) else Path(args.out_summary),
        )
        print("Food_DB v1.1 round8 draft built")
        print(f"base_rows={result['current_count']}")
        print(f"round8_manual_decisions={result['applied_count']}")
        print(f"round8_deferred={result['deferred_count']}")
        print(f"final_rows={result['final_count']}")
        print(f"draft={ROUND8_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else args.out_draft}")
        return

    if args.round7:
        result = build_round7_draft(
            ROUND5_OUT_DRAFT_PATH if args.current_fooddb == str(CURRENT_FOODDB_PATH) else Path(args.current_fooddb),
            ROUND7_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else Path(args.out_draft),
            ROUND7_OUT_APPLIED_PATH if args.out_applied == str(OUT_APPLIED_PATH) else Path(args.out_applied),
            ROUND7_OUT_DEFERRED_PATH if args.out_deferred == str(OUT_DEFERRED_PATH) else Path(args.out_deferred),
            ROUND7_OUT_SUMMARY_PATH if args.out_summary == str(OUT_SUMMARY_PATH) else Path(args.out_summary),
        )
        print("Food_DB v1.1 round7 draft built")
        print(f"base_rows={result['current_count']}")
        print(f"round7_manual_decisions={result['applied_count']}")
        print(f"round7_deferred={result['deferred_count']}")
        print(f"final_rows={result['final_count']}")
        print(f"draft={ROUND7_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else args.out_draft}")
        return

    if args.round5:
        result = build_round5_draft(
            ROUND3_OUT_DRAFT_PATH if args.current_fooddb == str(CURRENT_FOODDB_PATH) else Path(args.current_fooddb),
            ROUND5_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else Path(args.out_draft),
            ROUND5_OUT_APPLIED_PATH if args.out_applied == str(OUT_APPLIED_PATH) else Path(args.out_applied),
            ROUND5_OUT_DEFERRED_PATH if args.out_deferred == str(OUT_DEFERRED_PATH) else Path(args.out_deferred),
            ROUND5_OUT_SUMMARY_PATH if args.out_summary == str(OUT_SUMMARY_PATH) else Path(args.out_summary),
        )
        print("Food_DB v1.1 round5 draft built")
        print(f"base_rows={result['current_count']}")
        print(f"round5_manual_decisions={result['applied_count']}")
        print(f"round5_deferred={result['deferred_count']}")
        print(f"final_rows={result['final_count']}")
        print(f"draft={ROUND5_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else args.out_draft}")
        return

    if args.round3:
        result = build_round3_draft(
            ROUND2_OUT_DRAFT_PATH if args.current_fooddb == str(CURRENT_FOODDB_PATH) else Path(args.current_fooddb),
            ROUND3_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else Path(args.out_draft),
            ROUND3_OUT_APPLIED_PATH if args.out_applied == str(OUT_APPLIED_PATH) else Path(args.out_applied),
            ROUND3_OUT_DEFERRED_PATH if args.out_deferred == str(OUT_DEFERRED_PATH) else Path(args.out_deferred),
            ROUND3_OUT_SUMMARY_PATH if args.out_summary == str(OUT_SUMMARY_PATH) else Path(args.out_summary),
        )
        print("Food_DB v1.1 round3 draft built")
        print(f"base_rows={result['current_count']}")
        print(f"round3_alias_decisions={result['applied_count']}")
        print(f"round3_deferred={result['deferred_count']}")
        print(f"final_rows={result['final_count']}")
        print(f"draft={ROUND3_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else args.out_draft}")
        return

    if args.round2:
        result = build_round2_draft(
            V1_1_BASE_FOODDB_PATH if args.current_fooddb == str(CURRENT_FOODDB_PATH) else Path(args.current_fooddb),
            Path(args.gap_candidates),
            Path(args.gap_review),
            ROUND2_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else Path(args.out_draft),
            ROUND2_OUT_APPLIED_PATH if args.out_applied == str(OUT_APPLIED_PATH) else Path(args.out_applied),
            ROUND2_OUT_DEFERRED_PATH if args.out_deferred == str(OUT_DEFERRED_PATH) else Path(args.out_deferred),
            ROUND2_OUT_SUMMARY_PATH if args.out_summary == str(OUT_SUMMARY_PATH) else Path(args.out_summary),
        )
        print("Food_DB v1.1 round2 draft built")
        print(f"base_rows={result['current_count']}")
        print(f"round2_applied={result['applied_count']}")
        print(f"round2_deferred={result['deferred_count']}")
        print(f"final_rows={result['final_count']}")
        print(f"draft={ROUND2_OUT_DRAFT_PATH if args.out_draft == str(OUT_DRAFT_PATH) else args.out_draft}")
        return

    result = build_draft(
        Path(args.current_fooddb),
        Path(args.gap_candidates),
        Path(args.gap_review),
        Path(args.out_draft),
        Path(args.out_applied),
        Path(args.out_deferred),
        Path(args.out_summary),
    )
    print("Food_DB v1.1 draft built")
    print(f"current_rows={result['current_count']}")
    print(f"additions_applied={result['applied_count']}")
    print(f"additions_deferred={result['deferred_count']}")
    print(f"final_rows={result['final_count']}")
    print(f"draft={args.out_draft}")


if __name__ == "__main__":
    main()
