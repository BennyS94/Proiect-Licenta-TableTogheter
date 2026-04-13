import argparse
import re
import unicodedata
from pathlib import Path

import pandas as pd


DEFAULT_INPUT = Path("data/fooddb/triage/foods_fooddb_candidates.csv")
DEFAULT_CORE_OUT = Path("data/fooddb/draft/fooddb_v1_core_master_draft.csv")
DEFAULT_MISSING_MACROS_OUT = Path(
    "data/fooddb/draft/fooddb_v1_excluded_missing_macros.csv"
)
DEFAULT_SCOPE_REVIEW_OUT = Path("data/fooddb/draft/fooddb_v1_scope_review.csv")
DEFAULT_MERGE_REVIEW_OUT = Path("data/fooddb/draft/fooddb_v1_merge_review.csv")

MINERAL_WATER_PATTERN = re.compile(
    r"^(Mineral (?:still|sparkling) water) \([^)]+\), (.+)$",
    flags=re.IGNORECASE,
)

SAFE_TEXT_REPLACEMENTS = [
    (re.compile(r"\bBrear\b", flags=re.IGNORECASE), "Bread"),
    (re.compile(r"\bVicoria\b", flags=re.IGNORECASE), "Victoria"),
    (re.compile(r"cabbageor", flags=re.IGNORECASE), "cabbage or"),
    (re.compile(r"(?<=\s)w(?=\s)", flags=re.IGNORECASE), "with"),
    (re.compile(r"(?<=\s)ou(?=\s)", flags=re.IGNORECASE), "or"),
    (re.compile(r"\baverage food\b", flags=re.IGNORECASE), ""),
]

GENERIC_WATER_PATTERNS = [
    (
        re.compile(
            r"^Mineral water, [^,]+, bottled, (very lightly|lightly|averagely|strongly) mineralized$",
            flags=re.IGNORECASE,
        ),
        lambda match: f"Mineral still water, bottled, {match.group(1).lower()} mineralized",
    ),
    (
        re.compile(
            r"^Spring water, [^,]+, bottled, (very lightly|lightly|averagely|strongly) mineralized$",
            flags=re.IGNORECASE,
        ),
        lambda match: f"Spring water, bottled, {match.group(1).lower()} mineralized",
    ),
    (
        re.compile(r"^Spring still water, [^,]+, bottled$", flags=re.IGNORECASE),
        lambda match: "Spring still water, bottled",
    ),
    (
        re.compile(r"^Water, mineral, non-carbonated, [^,]+$", flags=re.IGNORECASE),
        lambda match: "Mineral still water, bottled",
    ),
    (
        re.compile(
            r"^Water, mineral, carbonated or non-carbonated, [^,]+$",
            flags=re.IGNORECASE,
        ),
        lambda match: "Mineral water, bottled",
    ),
]

SCOPE_REVIEW_PHRASES = {
    "aioli",
    "barbecue sauce",
    "guacamole",
    "hummus",
    "ketchup",
    "tzatziki",
}

DROP_FROM_SCOPE_PHRASES = {
    "american-style sauce",
    "armorican-style sauce",
    "carbonara sauce",
    "meal replacement",
    "chicken, nugget",
    "spanish-style tortilla",
}

NON_WATER_BEVERAGE_PHRASES = {
    " juice",
    " nectar",
    "smoothie",
    "kombucha",
    "lemonade",
    "energy drink",
    "tonic drink",
    "cola",
    "cocktail",
    "beer",
    "cider",
    "wine",
    "liqueur",
    "vodka",
    "whisky",
    "whiskey",
    "rum",
    "champagne",
    "aperitif",
    "tea",
    "coffee",
    "cappuccino",
    "milkshake",
    "drink, plain",
    "drink, sweet",
    "drink not sweet",
}

NON_WATER_BEVERAGE_EXCEPTIONS = {
    "coconut milk or coconut cream",
}

FAMILY_NAME_NOISE_PATTERNS = [
    re.compile(r",?\s*sampled in the island of la martinique.*$", flags=re.IGNORECASE),
    re.compile(r",?\s*from la martinique.*$", flags=re.IGNORECASE),
    re.compile(r",?\s*from the island la reunion.*$", flags=re.IGNORECASE),
    re.compile(r",?\s*from the island la reunion.*$", flags=re.IGNORECASE),
]

STATE_TOKEN_MAP = {
    "raw": "raw",
    "cooked": "cooked",
    "boiled/cooked in water": "cooked",
    "boiled": "cooked",
    "steamed": "cooked",
    "fried": "fried",
    "grilled": "grilled",
    "roasted/baked": "baked",
    "baked": "baked",
    "roasted": "roasted",
    "smoked": "smoked",
    "uht": "uht",
    "pasteurised": "pasteurised",
    "pasteurized": "pasteurised",
    "fermented": "fermented",
    "frozen": "frozen",
    "canned": "canned",
    "dried": "dried",
    "dehydrated": "dried",
    "refrigerated": "refrigerated",
    "bottled": "bottled",
    "drained": "drained",
}

PROCESSING_STATES = {
    "raw",
    "cooked",
    "fried",
    "grilled",
    "baked",
    "roasted",
    "smoked",
    "uht",
    "pasteurised",
    "fermented",
}

PRESERVATION_STATES = {
    "frozen",
    "canned",
    "dried",
    "refrigerated",
    "bottled",
    "drained",
}

FAMILY_DROP_TOKENS = {
    "raw",
    "cooked",
    "steamed",
    "fried",
    "grilled",
    "baked",
    "roasted",
    "smoked",
    "uht",
    "pasteurised",
    "fermented",
    "frozen",
    "canned",
    "dried",
    "refrigerated",
    "bottled",
    "drained",
    "lightly mineralized",
    "very lightly mineralized",
    "averagely mineralized",
    "strongly mineralized",
    "non-carbonated",
    "carbonated",
    "crunchy",
    "tender",
}

OUT_OF_SCOPE_SUBGROUPS = {
    "sweet biscuits",
    "cakes and pastry",
    "viennese pastries",
    "soft drinks",
    "ice cream",
    "sorbet",
    "dairy desserts",
    "other desserts",
    "breakfast cereals",
    "savoury biscuits",
    "alcoholic beverages",
}

OUT_OF_SCOPE_SUB_SUB_GROUPS = {
    "dairy desserts",
    "other desserts",
}

CORE_COLUMNS = [
    "food_id",
    "canonical_name",
    "display_name",
    "food_family_name",
    "entity_level",
    "food_group",
    "food_subgroup",
    "energy_kcal_100g",
    "protein_g_100g",
    "carbs_g_100g",
    "fat_g_100g",
    "food_subgroup_detail",
    "processing_state",
    "preservation_state",
    "fibre_g_100g",
    "sugars_g_100g",
    "salt_g_100g",
    "water_g_100g",
    "alcohol_g_100g",
    "helper_macro_profile",
    "helper_use_as_protein",
    "helper_use_as_carb_side",
    "helper_use_as_veg_side",
    "helper_is_sweet",
    "helper_is_salty",
    "helper_is_drink",
    "helper_is_vegetarian",
    "helper_is_vegan",
    "helper_protein_bucket",
    "helper_carb_bucket",
    "helper_veg_bucket",
    "primary_source_uid",
    "primary_source_name",
    "primary_source_ciqual_code",
    "primary_source_name_tags",
    "qc_macro_complete",
    "qc_taxonomy_complete",
    "qc_canonicalization_status",
    "qc_scope_status",
    "qc_source_merge_count",
    "qc_notes",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Construieste un draft Food_DB v1 din candidatii triati."
    )
    parser.add_argument("--input", default=str(DEFAULT_INPUT))
    parser.add_argument("--out-core", default=str(DEFAULT_CORE_OUT))
    parser.add_argument("--out-missing-macros", default=str(DEFAULT_MISSING_MACROS_OUT))
    parser.add_argument("--out-scope-review", default=str(DEFAULT_SCOPE_REVIEW_OUT))
    parser.add_argument("--out-merge-review", default=str(DEFAULT_MERGE_REVIEW_OUT))
    return parser.parse_args()


def strip_accents(text):
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def normalize_spaces(text):
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r"(?:,\s*){2,}", ", ", text)
    parts = [part.strip() for part in text.split(",") if part.strip()]
    return ", ".join(parts).strip(" ,")


def normalize_slug(text):
    text = strip_accents(text.lower())
    text = re.sub(r"[()]", " ", text)
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_")


def normalize_match_text(text):
    text = strip_accents(str(text).lower())
    text = re.sub(r"[()]", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return f" {re.sub(r'\\s+', ' ', text).strip()} "


def apply_safe_text_replacements(text):
    clean_text = str(text)
    for pattern, replacement in SAFE_TEXT_REPLACEMENTS:
        clean_text = pattern.sub(replacement, clean_text)
    clean_text = re.sub(r"\bwithith\b", "with", clean_text, flags=re.IGNORECASE)
    return clean_text


def normalize_water_display_name(display_name):
    for pattern, replacement_builder in GENERIC_WATER_PATTERNS:
        match = pattern.match(display_name)
        if match:
            return replacement_builder(match)
    return display_name


def is_water_display(display_name):
    lowered = strip_accents(str(display_name).lower()).strip()
    return lowered.startswith(
        (
            "mineral still water",
            "mineral sparkling water",
            "mineral water",
            "spring still water",
            "spring water",
            "water, bottled",
            "water, municipal",
        )
    )


def is_non_water_beverage_like_display(display_name):
    normalized = normalize_match_text(display_name)
    for exception in NON_WATER_BEVERAGE_EXCEPTIONS:
        if normalize_match_text(exception) in normalized:
            return False
    if is_water_display(display_name):
        return False
    return any(normalize_match_text(phrase) in normalized for phrase in NON_WATER_BEVERAGE_PHRASES)


def is_truthy(value):
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    try:
        return float(value) != 0.0
    except Exception:
        return bool(value)


def maybe_bool(value):
    if pd.isna(value):
        return pd.NA
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y"}:
            return True
        if lowered in {"0", "false", "no", "n"}:
            return False
        return pd.NA
    try:
        return bool(int(value))
    except Exception:
        return pd.NA


def contains_phrase(text, phrase):
    return normalize_match_text(phrase) in normalize_match_text(text)


def dedupe_preserve_order(values):
    out = []
    seen = set()
    for value in values:
        if not value or value in seen:
            continue
        out.append(value)
        seen.add(value)
    return out


def build_display_name(source_name):
    if pd.isna(source_name):
        return ""

    text = str(source_name).strip()
    mineral_match = MINERAL_WATER_PATTERN.match(text)
    if mineral_match:
        text = f"{mineral_match.group(1)}, {mineral_match.group(2)}"

    text = text.replace("(", ", ")
    text = text.replace(")", "")
    text = apply_safe_text_replacements(text)
    text = normalize_spaces(text)
    text = normalize_water_display_name(text)
    text = normalize_spaces(text)
    return text


def is_mineral_water_display(display_name):
    lowered = strip_accents(display_name.lower())
    return lowered.startswith("mineral still water") or lowered.startswith("mineral sparkling water")


def humanize_guess(canonical_name_guess):
    if pd.isna(canonical_name_guess):
        return ""

    text = str(canonical_name_guess).replace("_", " ").strip()
    text = normalize_spaces(text)
    if not text:
        return ""
    return text[:1].upper() + text[1:]


def extract_state_tokens(display_name, name_tags):
    text = f"{display_name} {'' if pd.isna(name_tags) else name_tags}"
    normalized = normalize_match_text(text)

    found = []
    for token, mapped_value in STATE_TOKEN_MAP.items():
        if normalize_match_text(token) in normalized:
            found.append(mapped_value)

    return dedupe_preserve_order(found)


def build_processing_state(display_name, name_tags):
    tokens = [token for token in extract_state_tokens(display_name, name_tags) if token in PROCESSING_STATES]
    return ";".join(tokens) if tokens else pd.NA


def build_preservation_state(display_name, name_tags):
    tokens = [token for token in extract_state_tokens(display_name, name_tags) if token in PRESERVATION_STATES]
    return ";".join(tokens) if tokens else pd.NA


def build_food_family_name(display_name, canonical_name_guess):
    preferred = humanize_guess(canonical_name_guess)
    candidate = display_name if is_water_display(display_name) else (preferred or display_name)

    if not candidate:
        return ""

    if is_mineral_water_display(candidate):
        return candidate.split(",")[0].strip()

    segments = [segment.strip() for segment in candidate.split(",") if segment.strip()]
    while segments:
        tail = strip_accents(segments[-1].lower())
        if tail in FAMILY_DROP_TOKENS:
            segments.pop()
            continue
        break

    if not segments:
        family_name = candidate
    else:
        family_name = ", ".join(segments)

    family_name = apply_safe_text_replacements(family_name)
    for pattern in FAMILY_NAME_NOISE_PATTERNS:
        family_name = pattern.sub("", family_name)
    family_name = re.sub(r"\bbread(?=\d)", "bread ", family_name, flags=re.IGNORECASE)
    family_name = normalize_spaces(family_name)
    return family_name or display_name


def pick_display_name(display_name, food_family_name):
    if display_name:
        return display_name
    return food_family_name


def build_canonical_name(display_name):
    return normalize_slug(display_name)


def build_food_id(canonical_name):
    return f"food_{canonical_name}"


def build_helper_is_drink(row, display_name):
    main_group = str(row.get("main_group") or "").strip().lower()
    sub_group = str(row.get("sub_group") or "").strip().lower()
    if main_group == "beverages":
        return True
    if sub_group in {"water", "non-alcoholic beverages", "alcoholic beverages"}:
        return True
    if is_water_display(display_name) or is_non_water_beverage_like_display(display_name):
        return True
    return pd.NA


def build_helper_is_vegetarian(row):
    if is_truthy(row.get("tag_vegetarian_safe")):
        return True
    return pd.NA


def build_helper_is_vegan(row):
    if is_truthy(row.get("tag_vegan_safe")):
        return True
    return pd.NA


def build_qc_notes(parts):
    clean_parts = []
    for part in parts:
        if pd.isna(part):
            continue
        text = str(part).strip()
        if text:
            clean_parts.append(text)
    return " | ".join(clean_parts) if clean_parts else pd.NA


def clean_helper_fields(base_df):
    clean_df = base_df.copy()
    bool_columns = [
        "helper_use_as_protein",
        "helper_use_as_carb_side",
        "helper_use_as_veg_side",
        "helper_is_sweet",
        "helper_is_salty",
        "helper_is_drink",
        "helper_is_vegetarian",
        "helper_is_vegan",
    ]
    for column in bool_columns:
        clean_df[column] = clean_df[column].astype("boolean")

    drink_mask = clean_df["display_name"].map(is_non_water_beverage_like_display) | clean_df["display_name"].map(is_water_display)
    drink_mask = drink_mask | clean_df["food_group"].fillna("").str.lower().eq("beverages")

    for column in [
        "helper_use_as_protein",
        "helper_use_as_carb_side",
        "helper_use_as_veg_side",
        "helper_protein_bucket",
        "helper_carb_bucket",
        "helper_veg_bucket",
    ]:
        clean_df.loc[drink_mask, column] = pd.NA

    meat_mask = clean_df["food_group"].fillna("").str.lower().eq("meat, egg and fish")
    clean_df.loc[meat_mask & clean_df["helper_use_as_veg_side"].eq(True), "helper_use_as_veg_side"] = pd.NA

    non_egg_mask = clean_df["food_subgroup"].fillna("").str.lower().ne("eggs")
    clean_df.loc[non_egg_mask & clean_df["helper_protein_bucket"].eq("eggs"), "helper_protein_bucket"] = pd.NA

    non_fish_meat_mask = clean_df["food_group"].fillna("").str.lower().ne("meat, egg and fish")
    fish_bucket_mask = clean_df["helper_protein_bucket"].isin(["fish_white", "fish_fatty"])
    clean_df.loc[non_fish_meat_mask & fish_bucket_mask, "helper_protein_bucket"] = pd.NA

    clean_df.loc[drink_mask & clean_df["helper_is_salty"].eq(True), "helper_is_salty"] = pd.NA

    return clean_df


def has_min_macro_set(row):
    required_cols = [
        "energy_kcal_100g",
        "protein_g_100g",
        "carbs_g_100g",
        "fat_g_100g",
    ]
    return row[required_cols].notna().all()


def has_min_taxonomy(row):
    return pd.notna(row["food_group"]) and pd.notna(row["food_subgroup"])


def evaluate_scope(row):
    # Lipsa taxonomiei nu blocheaza salvarea row-ului, dar il trimite la review.
    if not row["qc_taxonomy_complete"]:
        return "review_missing_taxonomy", "missing food_group or food_subgroup"

    display_name = str(row["display_name"] or "")
    main_group = str(row["food_group"]).strip().lower()
    sub_group = str(row["food_subgroup"]).strip().lower()
    sub_sub_group = str(row["food_subgroup_detail"] or "").strip().lower()

    for phrase in DROP_FROM_SCOPE_PHRASES:
        if contains_phrase(display_name, phrase):
            return "drop_from_fooddb_scope", f"explicit out-of-scope rule: {phrase}"

    for phrase in SCOPE_REVIEW_PHRASES:
        if contains_phrase(display_name, phrase):
            return "review_non_ingredient_first", f"explicit scope review rule: {phrase}"

    if is_non_water_beverage_like_display(display_name):
        return "review_non_ingredient_first", "beverage-like row leaked into core via taxonomy"

    if main_group == "ice cream and sorbet":
        return "review_non_ingredient_first", "ice cream and sorbet is out of Food_DB v1 scope"

    if main_group == "beverages" and sub_group != "water":
        return "review_non_ingredient_first", "non-water beverages are deferred from Food_DB v1 core"

    if main_group == "sugar and confectionery" and sub_group != "sugars and honey":
        return "review_non_ingredient_first", "sugar and confectionery row is not ingredient-first enough"

    if sub_group in OUT_OF_SCOPE_SUBGROUPS:
        return "review_non_ingredient_first", f"subgroup out of scope: {sub_group}"

    if sub_sub_group in OUT_OF_SCOPE_SUB_SUB_GROUPS:
        return "review_non_ingredient_first", f"sub_sub_group out of scope: {sub_sub_group}"

    if is_truthy(row.get("tag_dessert_like")):
        return "review_non_ingredient_first", "dessert-like row is deferred from Food_DB v1 core"

    return "accepted_core", ""


def select_representative_row(group):
    ranked = group.copy()
    ranked["qc_rank_non_null"] = ranked.notna().sum(axis=1)
    ranked = ranked.sort_values(
        by=["qc_taxonomy_complete", "qc_macro_complete", "qc_rank_non_null", "primary_source_uid"],
        ascending=[False, False, False, True],
    )
    return ranked.iloc[0].copy()


def collapse_core_duplicates(core_df):
    if core_df.empty:
        empty_merge = pd.DataFrame(
            columns=[
                "canonical_name",
                "display_name",
                "food_family_name",
                "qc_source_merge_count",
                "qc_canonicalization_status",
                "merged_source_uids",
                "merged_source_names",
                "qc_notes",
            ]
        )
        return core_df, empty_merge

    final_rows = []
    merge_rows = []

    for canonical_name, group in core_df.groupby("canonical_name", dropna=False, sort=True):
        representative = select_representative_row(group)
        source_merge_count = int(len(group))
        representative["qc_source_merge_count"] = source_merge_count

        is_all_mineral_water = group["display_name"].map(is_mineral_water_display).all()
        if source_merge_count > 1 and is_all_mineral_water:
            representative["qc_canonicalization_status"] = "collapsed_mineral_water_brand"
        elif source_merge_count > 1:
            representative["qc_canonicalization_status"] = "collapsed_duplicate_canonical_name"
        else:
            representative["qc_canonicalization_status"] = "normalized_from_source_name"

        if source_merge_count > 1:
            representative["qc_notes"] = build_qc_notes(
                [
                    representative["qc_notes"] if pd.notna(representative["qc_notes"]) else "",
                    f"collapsed {source_merge_count} source rows into one canonical row",
                ]
            )

            merge_rows.append(
                {
                    "canonical_name": representative["canonical_name"],
                    "display_name": representative["display_name"],
                    "food_family_name": representative["food_family_name"],
                    "qc_source_merge_count": source_merge_count,
                    "qc_canonicalization_status": representative["qc_canonicalization_status"],
                    "merged_source_uids": "; ".join(group["primary_source_uid"].astype(str).tolist()),
                    "merged_source_names": " || ".join(group["primary_source_name"].astype(str).tolist()),
                    "qc_notes": representative["qc_notes"],
                }
            )

        final_rows.append(representative[CORE_COLUMNS].to_dict())

    final_df = pd.DataFrame(final_rows, columns=CORE_COLUMNS)
    merge_df = pd.DataFrame(merge_rows)
    return final_df, merge_df


def build_base_frame(df):
    base = pd.DataFrame()

    base["display_name"] = df["name"].map(build_display_name)
    base["food_family_name"] = [
        build_food_family_name(display_name, canonical_name_guess)
        for display_name, canonical_name_guess in zip(base["display_name"], df["canonical_name_guess"])
    ]
    base["display_name"] = [
        pick_display_name(display_name, food_family_name)
        for display_name, food_family_name in zip(base["display_name"], base["food_family_name"])
    ]
    base["canonical_name"] = base["display_name"].map(build_canonical_name)
    base["food_id"] = base["canonical_name"].map(build_food_id)

    base["entity_level"] = df["entity_level_guess"]
    base["food_group"] = df["main_group"]
    base["food_subgroup"] = df["sub_group"]
    base["food_subgroup_detail"] = df["sub_sub_group"]

    base["processing_state"] = [
        build_processing_state(display_name, name_tags)
        for display_name, name_tags in zip(base["display_name"], df["name_tags"])
    ]
    base["preservation_state"] = [
        build_preservation_state(display_name, name_tags)
        for display_name, name_tags in zip(base["display_name"], df["name_tags"])
    ]

    base["energy_kcal_100g"] = df["kcal_sanitized"]
    base["protein_g_100g"] = df["protein_g_100g_ml"]
    base["carbs_g_100g"] = df["carbohydrate_g_100g_ml"]
    base["fat_g_100g"] = df["fat_g_100g_ml"]
    base["fibre_g_100g"] = df["fibres_g_100g_ml"]
    base["sugars_g_100g"] = df["sugars_g_100g_ml"]
    base["salt_g_100g"] = df["salt_g_100g_ml"]
    base["water_g_100g"] = df["water_g_100g_ml"]
    base["alcohol_g_100g"] = df["alcohol_g_100g_ml"]

    base["helper_macro_profile"] = df["macro_tags"]
    base["helper_use_as_protein"] = df["role_protein"].map(maybe_bool)
    base["helper_use_as_carb_side"] = df["role_side_carb"].map(maybe_bool)
    base["helper_use_as_veg_side"] = df["role_side_veg"].map(maybe_bool)
    base["helper_is_sweet"] = df["is_sweet"].map(maybe_bool)
    base["helper_is_salty"] = df["is_salty"].map(maybe_bool)
    base["helper_is_drink"] = [
        build_helper_is_drink(row, display_name)
        for (_, row), display_name in zip(df.iterrows(), base["display_name"])
    ]
    base["helper_is_vegetarian"] = [build_helper_is_vegetarian(row) for _, row in df.iterrows()]
    base["helper_is_vegan"] = [build_helper_is_vegan(row) for _, row in df.iterrows()]
    base["helper_protein_bucket"] = df["protein_bucket"]
    base["helper_carb_bucket"] = df["carb_bucket"]
    base["helper_veg_bucket"] = df["veg_bucket"]

    base["primary_source_uid"] = df["uid"]
    base["primary_source_name"] = df["name"]
    base["primary_source_ciqual_code"] = df["ciqual_code"]
    base["primary_source_name_tags"] = df["name_tags"]

    base["qc_macro_complete"] = base.apply(has_min_macro_set, axis=1)
    base["qc_taxonomy_complete"] = base.apply(has_min_taxonomy, axis=1)
    base["qc_canonicalization_status"] = "normalized_from_source_name"
    base["qc_scope_status"] = pd.NA
    base["qc_source_merge_count"] = 1
    base["qc_notes"] = pd.NA

    base = clean_helper_fields(base)
    return base[CORE_COLUMNS].copy()


def split_outputs(base_df):
    excluded_missing_macros = base_df[~base_df["qc_macro_complete"]].copy()
    excluded_missing_macros["qc_scope_status"] = "excluded_missing_min_macros"
    excluded_missing_macros["qc_notes"] = [
        build_qc_notes(
            [
                "missing minimum macro set",
                f"missing: {', '.join(missing_cols)}" if missing_cols else "",
                "taxonomy incomplete" if not qc_taxonomy_complete else "",
            ]
        )
        for missing_cols, qc_taxonomy_complete in zip(
            excluded_missing_macros.apply(
                lambda row: [
                    col
                    for col in [
                        "energy_kcal_100g",
                        "protein_g_100g",
                        "carbs_g_100g",
                        "fat_g_100g",
                    ]
                    if pd.isna(row[col])
                ],
                axis=1,
            ),
            excluded_missing_macros["qc_taxonomy_complete"],
        )
    ]

    remaining = base_df[base_df["qc_macro_complete"]].copy()

    scope_statuses = []
    scope_notes = []
    for _, row in remaining.iterrows():
        status, note = evaluate_scope(row)
        scope_statuses.append(status)
        scope_notes.append(note)

    remaining["qc_scope_status"] = scope_statuses
    remaining["qc_notes"] = [
        build_qc_notes([current, extra])
        for current, extra in zip(remaining["qc_notes"], scope_notes)
    ]

    scope_review = remaining[remaining["qc_scope_status"] != "accepted_core"].copy()
    core_candidates = remaining[remaining["qc_scope_status"] == "accepted_core"].copy()

    return excluded_missing_macros[CORE_COLUMNS], scope_review[CORE_COLUMNS], core_candidates[CORE_COLUMNS]


def ensure_parent(path):
    path.parent.mkdir(parents=True, exist_ok=True)


def main():
    args = parse_args()

    input_path = Path(args.input)
    out_core = Path(args.out_core)
    out_missing_macros = Path(args.out_missing_macros)
    out_scope_review = Path(args.out_scope_review)
    out_merge_review = Path(args.out_merge_review)

    df = pd.read_csv(input_path)
    base_df = build_base_frame(df)

    excluded_missing_macros, scope_review, core_candidates = split_outputs(base_df)
    core_master, merge_review = collapse_core_duplicates(core_candidates)
    classified = pd.concat(
        [excluded_missing_macros, scope_review, core_candidates],
        ignore_index=True,
    )

    for path in [out_core, out_missing_macros, out_scope_review, out_merge_review]:
        ensure_parent(path)

    core_master.to_csv(out_core, index=False, encoding="utf-8")
    excluded_missing_macros.to_csv(out_missing_macros, index=False, encoding="utf-8")
    scope_review.to_csv(out_scope_review, index=False, encoding="utf-8")
    merge_review.to_csv(out_merge_review, index=False, encoding="utf-8")

    print(f"Input rows: {len(df)}")
    print(f"Core master rows: {len(core_master)}")
    print(f"Excluded missing macros rows: {len(excluded_missing_macros)}")
    print(f"Scope review rows: {len(scope_review)}")
    print(f"Merge review rows: {len(merge_review)}")

    print("\nqc_scope_status counts:")
    print(classified["qc_scope_status"].fillna("<NA>").value_counts().to_string())
    print("\ncore qc_canonicalization_status counts:")
    if len(core_master):
        print(core_master["qc_canonicalization_status"].fillna("<NA>").value_counts().to_string())
    else:
        print("(none)")

    print(f"\nWritten: {out_core}")
    print(f"Written: {out_missing_macros}")
    print(f"Written: {out_scope_review}")
    print(f"Written: {out_merge_review}")


if __name__ == "__main__":
    main()
