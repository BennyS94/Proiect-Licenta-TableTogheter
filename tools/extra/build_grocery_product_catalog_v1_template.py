from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.generator_v1.candidate_filter import (  # noqa: E402
    build_household_preference_context,
    filter_recipe_candidates,
)
from src.generator_v1.data_loader import (  # noqa: E402
    V1_2_DEMO_FINAL_INGREDIENTS_PATH,
    V1_2_DEMO_FINAL_NUTRITION_PATH,
    V1_2_DEMO_FINAL_PROFILE,
    V1_2_DEMO_FINAL_RECIPES_PATH,
    load_fooddb_current,
    load_recipe_candidate_pool,
)
from src.generator_v1.grocery_list import (  # noqa: E402
    build_grocery_list,
    grocery_list_rows,
    normalize_grocery_category,
    normalize_grocery_display_name,
)
from src.generator_v1.multi_day_selector import (  # noqa: E402
    MULTI_DAY_MODE_GLOBAL,
    generate_multi_day_plan,
)
from src.generator_v1.profile_loader import load_member_profile  # noqa: E402
from src.generator_v1.slot_candidates import build_slot_candidates  # noqa: E402
from src.generator_v1.target_builder import build_nutrition_target  # noqa: E402


PROFILE_PATH = ROOT / "profiles/member_profile_demo_v1.json"
PURCHASE_RULES_PATH = ROOT / "data/grocery/reference/grocery_purchase_rules_v1.csv"
COOKED_TO_RAW_RULES_PATH = ROOT / "data/grocery/reference/grocery_cooked_to_raw_rules_v1.csv"
FOODDB_DRAFT_PATH = ROOT / "data/fooddb/draft/fooddb_v1_2_core_master_manual_batch2.csv"
GROCERY_DRAFT_DIR = ROOT / "data/grocery/draft"
GROCERY_AUDIT_DIR = ROOT / "data/grocery/audit"
CATALOG_OUT = GROCERY_DRAFT_DIR / "grocery_product_catalog_v1_price_research_template.csv"
SUMMARY_OUT = GROCERY_AUDIT_DIR / "grocery_product_catalog_v1_template_summary.txt"
ITEMS_OUT = GROCERY_AUDIT_DIR / "grocery_product_catalog_v1_template_items.csv"
PROMPT_OUT = GROCERY_AUDIT_DIR / "grocery_product_catalog_v1_price_research_prompt.txt"

CATALOG_COLUMNS = [
    "catalog_item_id",
    "match_key",
    "mapped_food_id",
    "canonical_ingredient_name",
    "display_name_ro",
    "display_name_en",
    "shopping_category",
    "purchase_format_suggestion",
    "package_type",
    "package_size_g",
    "package_size_ml",
    "unit_count",
    "sold_by",
    "price_method",
    "reference_product_name",
    "store_name",
    "reference_price",
    "price_per_kg",
    "price_per_liter",
    "currency",
    "source_url",
    "captured_at",
    "confidence",
    "decision_status",
    "qc_notes",
    "priority",
    "used_in_demo_grocery",
    "occurrence_count",
    "total_needed_grams_in_sample",
]

HIGH_PRIORITY_CATEGORIES = {
    "meat_fish",
    "dairy_eggs",
    "carbs_grains",
    "vegetables",
    "fruits",
    "legumes_beans",
    "oils_fats",
    "pantry_basics",
}

MAX_CATALOG_ROWS = 100
TARGET_CATALOG_ROWS = 90


def main() -> None:
    GROCERY_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    GROCERY_AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    purchase_rules = _read_csv(PURCHASE_RULES_PATH)
    cooked_rules = _read_csv(COOKED_TO_RAW_RULES_PATH)
    fooddb = _load_fooddb_draft()
    food_lookup = _food_lookup(fooddb)
    demo_grocery = _build_representative_grocery()
    demo_items = grocery_list_rows(demo_grocery, include_pantry_basics=True)
    ingredients = _read_csv(ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH)

    rows_by_key: dict[str, dict[str, Any]] = {}
    omitted_candidates: list[dict[str, Any]] = []

    for item in demo_items:
        _upsert_catalog_row(rows_by_key, _row_from_demo_item(item), source_rank=1)

    for _, rule in purchase_rules.iterrows():
        _upsert_catalog_row(rows_by_key, _row_from_purchase_rule(rule), source_rank=2)

    top_rows = _top_fooddb_catalog_rows(ingredients, food_lookup, purchase_rules)
    for row in top_rows:
        if len(rows_by_key) < TARGET_CATALOG_ROWS:
            _upsert_catalog_row(rows_by_key, row, source_rank=3)
        else:
            omitted_candidates.append(row)

    rows = list(rows_by_key.values())
    rows.sort(key=_catalog_sort_key)
    if len(rows) > MAX_CATALOG_ROWS:
        omitted_candidates.extend(rows[MAX_CATALOG_ROWS:])
        rows = rows[:MAX_CATALOG_ROWS]

    for index, row in enumerate(rows, start=1):
        row["catalog_item_id"] = f"grocery_catalog_v1_{index:03d}"

    template = pd.DataFrame(rows, columns=CATALOG_COLUMNS).fillna("")
    template.to_csv(CATALOG_OUT, index=False)
    template.to_csv(ITEMS_OUT, index=False)
    SUMMARY_OUT.write_text(
        _summary_text(
            rows=rows,
            omitted_count=len(omitted_candidates),
            purchase_rules_count=len(purchase_rules),
            cooked_rules_count=len(cooked_rules),
            demo_grocery=demo_grocery,
        ),
        encoding="utf-8",
    )
    PROMPT_OUT.write_text(_research_prompt(), encoding="utf-8")

    print("Grocery Product Catalog v1 template written")
    print(f"catalog={CATALOG_OUT}")
    print(f"summary={SUMMARY_OUT}")
    print(f"items={ITEMS_OUT}")
    print(f"prompt={PROMPT_OUT}")
    print(
        "rows="
        f"{len(rows)}; high_priority={sum(1 for row in rows if row['priority'] == 'high')}; "
        f"used_in_demo={sum(1 for row in rows if row['used_in_demo_grocery'] == 'true')}; "
        f"omitted={len(omitted_candidates)}"
    )


def _build_representative_grocery() -> dict[str, Any]:
    profile = load_member_profile(PROFILE_PATH)
    target = build_nutrition_target(profile)
    pool = load_recipe_candidate_pool(
        recipes_path=ROOT / V1_2_DEMO_FINAL_RECIPES_PATH,
        ingredients_path=ROOT / V1_2_DEMO_FINAL_INGREDIENTS_PATH,
        nutrition_path=ROOT / V1_2_DEMO_FINAL_NUTRITION_PATH,
        dataset_profile=V1_2_DEMO_FINAL_PROFILE,
    )
    fooddb = load_fooddb_current()
    preference_context = build_household_preference_context(profile)
    filtered_candidates = filter_recipe_candidates(
        eligible_candidates=pool.eligible_candidates,
        ingredients=pool.ingredients,
        context=preference_context,
    )
    slot_candidates = build_slot_candidates(
        target=target,
        filtered_candidates=filtered_candidates,
        time_sensitivity=preference_context.time_sensitivity,
        ingredients=pool.ingredients,
        fooddb=fooddb,
        portion_policy_mode="target_aware",
    )
    plan = generate_multi_day_plan(
        profile=profile,
        target=target,
        slot_candidates=slot_candidates,
        days=3,
        config={
            "selection_mode": "balanced_day",
            "portion_policy": "target_aware",
            "meal_realism_mode": "practical",
            "quality_gate": "demo_safe",
            "alternative_count": 3,
            "return_alternatives": True,
            "multi_day_mode": MULTI_DAY_MODE_GLOBAL,
            "candidate_day_alternative_count": 10,
            "global_max_candidates_per_slot": 26,
            "day_candidate_pool_size_target": 75,
            "day_candidate_pool_max": 150,
            "include_slot_forced_variants": True,
            "no_repeat_policy": "hard",
            "multi_day_speed_mode": "fast",
            "day_candidate_builder": "direct_from_slots",
            "direct_slot_shortlist_size": 12,
        },
    )
    grocery_list = build_grocery_list(
        plan,
        pool.ingredients,
        fooddb_df=fooddb,
        config={
            "include_pantry_basics": True,
            "include_purchase_suggestions": True,
            "enable_cooked_to_raw_conversion": True,
            "exclude_water": True,
            "round_grams_for_display": True,
        },
    )
    grocery_list["round60_plan_summary"] = plan.get("multi_day_summary", {})
    return grocery_list


def _row_from_demo_item(item: dict[str, Any]) -> dict[str, Any]:
    display_name = _clean_text(item.get("display_name_clean")) or _clean_text(item.get("display_name"))
    mapped_ids = _join_values(item.get("source_mapped_food_ids")) or _clean_text(item.get("mapped_food_id"))
    match_key = f"display_alias:{_normalise_key(display_name)}"
    package_fields = _package_fields_from_item(item)
    return _blank_row(
        match_key=match_key,
        mapped_food_id=mapped_ids,
        canonical_name=_clean_text(item.get("canonical_name")) or display_name,
        display_name_en=display_name,
        shopping_category=_clean_text(item.get("grocery_category")),
        purchase_format=_clean_text(item.get("purchase_display"))
        or _clean_text(item.get("display_grams")),
        priority="high",
        used_in_demo=True,
        occurrence_count=_clean_text(item.get("meal_count")) or _clean_text(item.get("recipe_count")),
        total_needed_grams=_clean_text(item.get("total_grams")),
        qc_notes="representative demo grocery item; prices intentionally blank",
        **package_fields,
    )


def _row_from_purchase_rule(rule: pd.Series) -> dict[str, Any]:
    match_type = _clean_text(rule.get("match_type"))
    match_value = _clean_text(rule.get("match_value"))
    match_key = f"{match_type}:{_normalise_key(match_value)}"
    display_name = _display_name_from_rule(rule)
    package_fields = _package_fields_from_rule(rule)
    category = _clean_text(rule.get("grocery_category")) or "other_review"
    priority = "medium" if category in HIGH_PRIORITY_CATEGORIES else "low"
    if match_type == "category_keyword":
        priority = "low"
    return _blank_row(
        match_key=match_key,
        mapped_food_id="",
        canonical_name=_normalise_key(match_value),
        display_name_en=display_name,
        shopping_category=category,
        purchase_format=_purchase_format_from_rule(rule),
        priority=priority,
        used_in_demo=False,
        occurrence_count="",
        total_needed_grams="",
        qc_notes=f"from purchase rule {rule.get('rule_id')}; prices intentionally blank",
        **package_fields,
    )


def _top_fooddb_catalog_rows(
    ingredients: pd.DataFrame,
    food_lookup: dict[str, dict[str, Any]],
    purchase_rules: pd.DataFrame,
) -> list[dict[str, Any]]:
    if ingredients.empty or "mapped_food_id" not in ingredients.columns:
        return []
    data = ingredients.copy().fillna("")
    data["mapped_food_id"] = data["mapped_food_id"].map(_clean_text)
    data["quantity_grams_estimated"] = data.get("quantity_grams_estimated", 0).map(_to_float)
    data = data[
        (data["mapped_food_id"] != "")
        & (data["quantity_grams_estimated"] > 0)
        & (~data["mapped_food_id"].str.contains("water", case=False, na=False))
    ]
    grouped = (
        data.groupby("mapped_food_id")
        .agg(
            occurrence_count=("mapped_food_id", "size"),
            total_grams=("quantity_grams_estimated", "sum"),
            ingredient_name=("ingredient_name_normalized", "first"),
        )
        .reset_index()
        .sort_values(["occurrence_count", "total_grams"], ascending=[False, False])
    )
    rows: list[dict[str, Any]] = []
    for _, group in grouped.head(80).iterrows():
        food_id = _clean_text(group.get("mapped_food_id"))
        food = food_lookup.get(food_id, {})
        display_name = _clean_text(food.get("display_name")) or _clean_text(group.get("ingredient_name"))
        canonical_name = _clean_text(food.get("canonical_name")) or _normalise_key(display_name)
        item_for_category = {
            "display_name": display_name,
            "canonical_name": canonical_name,
            "ingredient_names_seen": [_clean_text(group.get("ingredient_name"))],
            "total_grams": group.get("total_grams"),
        }
        display_clean = normalize_grocery_display_name(item_for_category)
        item_for_category["display_name_clean"] = display_clean
        category = normalize_grocery_category(item_for_category)
        if category not in HIGH_PRIORITY_CATEGORIES:
            continue
        package_fields = _package_fields_from_category(display_clean, category, purchase_rules)
        priority = "high" if int(group.get("occurrence_count") or 0) >= 10 else "medium"
        rows.append(
            _blank_row(
                match_key=f"mapped_food_id:{food_id}",
                mapped_food_id=food_id,
                canonical_name=canonical_name,
                display_name_en=display_clean,
                shopping_category=category,
                purchase_format=package_fields.pop("purchase_format_suggestion"),
                priority=priority,
                used_in_demo=False,
                occurrence_count=str(int(group.get("occurrence_count") or 0)),
                total_needed_grams=str(round(float(group.get("total_grams") or 0.0), 1)),
                qc_notes="high/common mapped ingredient candidate; prices intentionally blank",
                **package_fields,
            )
        )
    return rows


def _blank_row(
    *,
    match_key: str,
    mapped_food_id: str,
    canonical_name: str,
    display_name_en: str,
    shopping_category: str,
    purchase_format: str,
    package_type: str,
    package_size_g: str,
    package_size_ml: str,
    unit_count: str,
    sold_by: str,
    price_method: str,
    priority: str,
    used_in_demo: bool,
    occurrence_count: str,
    total_needed_grams: str,
    qc_notes: str,
) -> dict[str, Any]:
    row = {column: "" for column in CATALOG_COLUMNS}
    row.update(
        {
            "catalog_item_id": "",
            "match_key": match_key,
            "mapped_food_id": mapped_food_id,
            "canonical_ingredient_name": canonical_name,
            "display_name_ro": "",
            "display_name_en": display_name_en,
            "shopping_category": shopping_category,
            "purchase_format_suggestion": purchase_format,
            "package_type": package_type,
            "package_size_g": package_size_g,
            "package_size_ml": package_size_ml,
            "unit_count": unit_count,
            "sold_by": sold_by,
            "price_method": price_method,
            "reference_product_name": "",
            "store_name": "",
            "reference_price": "",
            "price_per_kg": "",
            "price_per_liter": "",
            "currency": "",
            "source_url": "",
            "captured_at": "",
            "confidence": "unknown",
            "decision_status": "pending_research",
            "qc_notes": qc_notes,
            "priority": priority,
            "used_in_demo_grocery": "true" if used_in_demo else "false",
            "occurrence_count": occurrence_count,
            "total_needed_grams_in_sample": total_needed_grams,
        }
    )
    return row


def _package_fields_from_item(item: dict[str, Any]) -> dict[str, str]:
    unit_type = _clean_text(item.get("purchase_unit_type"))
    package_type, sold_by, price_method = _package_type_fields(unit_type)
    quantity = _to_float(item.get("purchase_quantity"))
    amount_grams = _to_float(item.get("purchase_amount_grams"))
    package_size_g = ""
    package_size_ml = ""
    if quantity and amount_grams:
        unit_size = round(amount_grams / quantity, 1)
        if unit_type == "carton":
            package_size_ml = _format_number(unit_size)
        elif unit_type not in {"pieces", "pantry_check", "review"}:
            package_size_g = _format_number(unit_size)
    return {
        "package_type": package_type,
        "package_size_g": package_size_g,
        "package_size_ml": package_size_ml,
        "unit_count": "",
        "sold_by": sold_by,
        "price_method": price_method,
    }


def _package_fields_from_rule(rule: pd.Series) -> dict[str, str]:
    unit_type = _clean_text(rule.get("purchase_unit_type"))
    package_type, sold_by, price_method = _package_type_fields(unit_type)
    default_package_grams = _clean_text(rule.get("default_package_grams"))
    grams_per_unit = _clean_text(rule.get("grams_per_unit"))
    display_unit = _clean_text(rule.get("display_unit")).lower()
    package_size_g = ""
    package_size_ml = ""
    if unit_type in {"carton", "bottle"} or display_unit in {"l", "liter", "litre"}:
        package_size_ml = default_package_grams or grams_per_unit
    elif default_package_grams:
        package_size_g = default_package_grams
    return {
        "package_type": package_type,
        "package_size_g": package_size_g,
        "package_size_ml": package_size_ml,
        "unit_count": "",
        "sold_by": sold_by,
        "price_method": price_method,
    }


def _package_fields_from_category(
    display_name: str,
    category: str,
    purchase_rules: pd.DataFrame,
) -> dict[str, str]:
    rule = _matching_purchase_rule(display_name, category, purchase_rules)
    if rule is not None:
        fields = _package_fields_from_rule(rule)
        fields["purchase_format_suggestion"] = _purchase_format_from_rule(rule)
        return fields
    if category in {"meat_fish", "vegetables", "fruits"}:
        return {
            "purchase_format_suggestion": "loose weight / grams",
            "package_type": "loose_weight",
            "package_size_g": "",
            "package_size_ml": "",
            "unit_count": "",
            "sold_by": "per_kg",
            "price_method": "price_per_kg",
        }
    if category == "pantry_basics":
        return {
            "purchase_format_suggestion": "check pantry",
            "package_type": "pantry_check",
            "package_size_g": "",
            "package_size_ml": "",
            "unit_count": "",
            "sold_by": "pantry_check",
            "price_method": "pantry_check",
        }
    return {
        "purchase_format_suggestion": "research package format",
        "package_type": "other",
        "package_size_g": "",
        "package_size_ml": "",
        "unit_count": "",
        "sold_by": "unknown",
        "price_method": "unknown",
    }


def _package_type_fields(unit_type: str) -> tuple[str, str, str]:
    if unit_type == "pantry_check":
        return "pantry_check", "pantry_check", "pantry_check"
    if unit_type == "grams":
        return "loose_weight", "per_kg", "price_per_kg"
    if unit_type == "pieces":
        return "piece", "per_piece", "price_per_piece"
    if unit_type in {"package", "pack"}:
        return "pack", "per_package", "price_per_package"
    if unit_type in {"bag", "tub", "carton", "bottle", "jar", "can", "bunch"}:
        return unit_type, "per_package", "price_per_package"
    return "other", "unknown", "unknown"


def _purchase_format_from_rule(rule: pd.Series) -> str:
    unit_type = _clean_text(rule.get("purchase_unit_type"))
    label = _clean_text(rule.get("default_package_label"))
    grams_per_unit = _clean_text(rule.get("grams_per_unit"))
    display_unit = _clean_text(rule.get("display_unit"))
    if unit_type == "pantry_check":
        return "check pantry"
    if label:
        return label
    if unit_type == "grams":
        return "loose weight / grams"
    if unit_type == "pieces" and grams_per_unit:
        return f"{display_unit or 'piece'}; approx {grams_per_unit}g each"
    return unit_type or "research package format"


def _display_name_from_rule(rule: pd.Series) -> str:
    match_value = _clean_text(rule.get("match_value"))
    if _clean_text(rule.get("match_type")) == "category_keyword":
        return match_value.replace("_", " ").title()
    return _humanise(match_value)


def _matching_purchase_rule(
    display_name: str,
    category: str,
    purchase_rules: pd.DataFrame,
) -> pd.Series | None:
    name_key = _normalise_key(display_name)
    for _, rule in purchase_rules.iterrows():
        match_type = _clean_text(rule.get("match_type"))
        match_value = _normalise_key(rule.get("match_value"))
        if match_type in {"display_alias", "normalized_name_keyword"} and match_value in name_key:
            return rule
    for _, rule in purchase_rules.iterrows():
        if (
            _clean_text(rule.get("match_type")) == "category_keyword"
            and _normalise_key(rule.get("match_value")) == category
        ):
            return rule
    return None


def _upsert_catalog_row(
    rows_by_key: dict[str, dict[str, Any]],
    row: dict[str, Any],
    source_rank: int,
) -> None:
    key = _clean_text(row.get("match_key"))
    if not key:
        return
    existing = rows_by_key.get(key)
    if not existing:
        row["_source_rank"] = source_rank
        rows_by_key[key] = row
        return
    if row.get("used_in_demo_grocery") == "true":
        existing["used_in_demo_grocery"] = "true"
        existing["priority"] = "high"
        existing["occurrence_count"] = row.get("occurrence_count", "")
        existing["total_needed_grams_in_sample"] = row.get("total_needed_grams_in_sample", "")
    for field in [
        "mapped_food_id",
        "canonical_ingredient_name",
        "display_name_en",
        "shopping_category",
        "purchase_format_suggestion",
        "package_type",
        "package_size_g",
        "package_size_ml",
        "sold_by",
        "price_method",
    ]:
        if not _clean_text(existing.get(field)) and _clean_text(row.get(field)):
            existing[field] = row[field]
    notes = [existing.get("qc_notes", ""), row.get("qc_notes", "")]
    existing["qc_notes"] = "; ".join(dict.fromkeys(_clean_text(item) for item in notes if _clean_text(item)))


def _catalog_sort_key(row: dict[str, Any]) -> tuple[int, int, str]:
    priority_rank = {"high": 0, "medium": 1, "low": 2}.get(str(row.get("priority")), 9)
    used_rank = 0 if row.get("used_in_demo_grocery") == "true" else 1
    return priority_rank, used_rank, str(row.get("display_name_en"))


def _summary_text(
    rows: list[dict[str, Any]],
    omitted_count: int,
    purchase_rules_count: int,
    cooked_rules_count: int,
    demo_grocery: dict[str, Any],
) -> str:
    priority_counts = _counts(row.get("priority") for row in rows)
    category_counts = _counts(row.get("shopping_category") for row in rows)
    price_blank_count = sum(
        1
        for row in rows
        if not any(
            _clean_text(row.get(field))
            for field in ["reference_price", "price_per_kg", "price_per_liter", "source_url"]
        )
    )
    demo_count = sum(1 for row in rows if row.get("used_in_demo_grocery") == "true")
    plan_summary = demo_grocery.get("round60_plan_summary", {})
    if not isinstance(plan_summary, dict):
        plan_summary = {}
    top_rows = rows[:20]
    lines = [
        "Grocery Product Catalog v1 price research template summary",
        "",
        f"dataset_profile={V1_2_DEMO_FINAL_PROFILE}",
        f"representative_plan_days={plan_summary.get('actual_days_generated', 3)}",
        f"representative_plan_accept_days={plan_summary.get('accept_day_count', 0)}",
        "",
        f"catalog_rows={len(rows)}",
        f"purchase_rules_loaded={purchase_rules_count}",
        f"cooked_to_raw_rules_loaded={cooked_rules_count}",
        f"used_in_demo_grocery_count={demo_count}",
        f"price_fields_blank_count={price_blank_count}",
        f"omitted_candidate_count={omitted_count}",
        f"priority_counts={_format_counts(priority_counts)}",
        f"category_counts={_format_counts(category_counts)}",
        f"ready_for_research={str(len(rows) > 0 and price_blank_count == len(rows)).lower()}",
        "",
        "No price/source fields were filled by this script.",
        "All rows start with decision_status=pending_research.",
        "",
        "Top 20 rows to research first:",
        *[
            (
                f"- {row.get('display_name_en')} | category={row.get('shopping_category')} | "
                f"priority={row.get('priority')} | used={row.get('used_in_demo_grocery')} | "
                f"format={row.get('purchase_format_suggestion')}"
            )
            for row in top_rows
        ],
    ]
    return "\n".join(lines) + "\n"


def _research_prompt() -> str:
    columns = ",".join(CATALOG_COLUMNS)
    return f"""You are filling a Grocery Product Catalog v1 price research template.

Use the CSV at:
data/grocery/draft/grocery_product_catalog_v1_price_research_template.csv

Rules:
- Complete only price/source/research fields. Preserve all existing columns and row identity.
- Do not invent prices.
- Use Romanian or European supermarket sources where possible.
- Kaufland, Carrefour, Auchan, Mega Image, Lidl, or official product pages are acceptable.
- If the exact item is not found, use a close generic equivalent and mark confidence as medium or low.
- Use RON currency.
- Include source_url and captured_at for every researched price.
- For per-kg products, fill price_per_kg.
- For per-liter products, fill price_per_liter.
- For package products, fill reference_price and package_size_g/package_size_ml/unit_count where relevant.
- If uncertain, set decision_status=needs_review.
- If no source is found, set decision_status=keep_deferred.
- Set decision_status=safe_to_use_demo only when the row has source-backed price data and a reasonable product match.
- Return CSV-ready output with the same columns, in the same order.

Columns:
{columns}

Important limitations:
- This is not live price fetching.
- This is not store, brand, or basket optimization.
- Prices are approximate demo inputs and must remain source-backed.
"""


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path).fillna("")


def _load_fooddb_draft() -> pd.DataFrame:
    if FOODDB_DRAFT_PATH.exists():
        return pd.read_csv(FOODDB_DRAFT_PATH).fillna("")
    return load_fooddb_current().fillna("")


def _food_lookup(fooddb: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if fooddb.empty or "food_id" not in fooddb.columns:
        return {}
    return {
        _clean_text(row.get("food_id")): row.to_dict()
        for _, row in fooddb.iterrows()
        if _clean_text(row.get("food_id"))
    }


def _counts(values: Any) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        key = _clean_text(value) or "blank"
        result[key] = result.get(key, 0) + 1
    return dict(sorted(result.items(), key=lambda item: (-item[1], item[0])))


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return "; ".join(f"{key}={value}" for key, value in counts.items())


def _join_values(value: Any) -> str:
    if isinstance(value, list):
        return ";".join(_clean_text(item) for item in value if _clean_text(item))
    return _clean_text(value)


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _normalise_key(value: Any) -> str:
    text = _clean_text(value).lower().replace("_", " ")
    for char in ["(", ")", ",", "-", "/"]:
        text = text.replace(char, " ")
    return " ".join(text.split())


def _humanise(value: Any) -> str:
    text = _normalise_key(value)
    if not text:
        return "Unknown item"
    return " ".join(part.capitalize() for part in text.split())


def _to_float(value: Any) -> float:
    text = _clean_text(value)
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        return 0.0


def _format_number(value: float) -> str:
    if abs(value - round(value)) < 0.01:
        return str(int(round(value)))
    return str(round(value, 1))


if __name__ == "__main__":
    main()
