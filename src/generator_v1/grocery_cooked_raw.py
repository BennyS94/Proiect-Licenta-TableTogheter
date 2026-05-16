from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_COOKED_TO_RAW_RULES_PATH = (
    PROJECT_ROOT / "data/grocery/reference/grocery_cooked_to_raw_rules_v1.csv"
)


def load_cooked_to_raw_rules(path: Path | str | None = None) -> list[dict[str, Any]]:
    resolved_path = _resolve_rules_path(path)
    with resolved_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return [_normalise_rule(row) for row in rows if _clean_text(row.get("rule_id"))]


def detect_cooked_raw_applicability(
    grocery_item: dict[str, Any],
    rules: list[dict[str, Any]],
) -> dict[str, Any] | None:
    for match_type in [
        "mapped_food_id",
        "display_alias",
        "normalized_name_keyword",
        "category_keyword",
    ]:
        for rule in rules:
            if rule.get("match_type") != match_type:
                continue
            if not _rule_category_matches(grocery_item, rule):
                continue
            if not _cooked_state_matches(grocery_item, rule):
                continue
            if _rule_matches_item(grocery_item, rule, match_type):
                return rule
    return None


def apply_cooked_to_raw_conversion(
    grocery_item: dict[str, Any],
    matched_rule: dict[str, Any] | None,
) -> dict[str, Any]:
    if not matched_rule:
        return _empty_conversion_fields()

    original_grams = _to_float(grocery_item.get("total_grams"))
    factor = _to_float(matched_rule.get("raw_equivalent_factor"))
    if original_grams <= 0 or factor <= 0:
        return _empty_conversion_fields()

    raw_grams = round(original_grams * factor, 1)
    warning_code = _clean_text(matched_rule.get("warning_code")) or "cooked_to_raw_estimate"
    return {
        "cooked_to_raw_applied": True,
        "cooked_to_raw_rule_id": _clean_text(matched_rule.get("rule_id")),
        "original_needed_grams": round(original_grams, 1),
        "raw_equivalent_grams": raw_grams,
        "raw_purchase_display_name": _clean_text(
            matched_rule.get("raw_purchase_display_name")
        ),
        "raw_equivalent_factor": factor,
        "raw_equivalent_basis": _clean_text(matched_rule.get("raw_equivalent_basis")),
        "cooked_to_raw_confidence": _clean_text(matched_rule.get("confidence")) or "low",
        "cooked_to_raw_warning": warning_code,
    }


def _empty_conversion_fields() -> dict[str, Any]:
    return {
        "cooked_to_raw_applied": False,
        "cooked_to_raw_rule_id": "",
        "original_needed_grams": None,
        "raw_equivalent_grams": None,
        "raw_purchase_display_name": "",
        "raw_equivalent_factor": None,
        "raw_equivalent_basis": "",
        "cooked_to_raw_confidence": "none",
        "cooked_to_raw_warning": "",
    }


def _resolve_rules_path(path: Path | str | None) -> Path:
    if path is None or str(path).strip() == "":
        return DEFAULT_COOKED_TO_RAW_RULES_PATH
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    root_candidate = PROJECT_ROOT / candidate
    if root_candidate.exists():
        return root_candidate
    return candidate


def _normalise_rule(row: dict[str, Any]) -> dict[str, Any]:
    rule = dict(row)
    for key in [
        "rule_id",
        "match_type",
        "match_value",
        "cooked_state_keywords",
        "raw_purchase_display_name",
        "raw_equivalent_basis",
        "confidence",
        "applies_to_category",
        "notes",
        "warning_code",
    ]:
        rule[key] = _clean_text(rule.get(key))
    rule["raw_equivalent_factor"] = _to_float(rule.get("raw_equivalent_factor"))
    rule["_match_values"] = _match_values(rule.get("match_value"))
    rule["_cooked_state_keywords"] = _match_values(rule.get("cooked_state_keywords"))
    return rule


def _rule_category_matches(item: dict[str, Any], rule: dict[str, Any]) -> bool:
    applies_to = _normalise_text(rule.get("applies_to_category"))
    if not applies_to:
        return True
    category = _normalise_text(item.get("grocery_category"))
    return category == applies_to


def _cooked_state_matches(item: dict[str, Any], rule: dict[str, Any]) -> bool:
    keywords = rule.get("_cooked_state_keywords", [])
    if not keywords:
        return True
    text = _item_search_text(item)
    return any(_contains_phrase(text, keyword) for keyword in keywords)


def _rule_matches_item(
    item: dict[str, Any],
    rule: dict[str, Any],
    match_type: str,
) -> bool:
    values = rule.get("_match_values", [])
    if not values:
        return False

    if match_type == "mapped_food_id":
        source_ids = set(_list_values(item.get("source_mapped_food_ids")))
        mapped_food_id = _clean_text(item.get("mapped_food_id"))
        if mapped_food_id:
            source_ids.add(mapped_food_id)
        normalised_ids = {_normalise_text(value) for value in source_ids}
        return any(_normalise_text(value) in normalised_ids for value in values)

    if match_type == "display_alias":
        display_name = _normalise_text(item.get("display_name_clean") or item.get("display_name"))
        return any(
            display_name == _normalise_text(value)
            or _contains_phrase(display_name, value)
            for value in values
        )

    if match_type == "normalized_name_keyword":
        text = _item_search_text(item)
        return any(_contains_phrase(text, value) for value in values)

    if match_type == "category_keyword":
        category = _normalise_text(item.get("grocery_category"))
        category_label = _normalise_text(item.get("category_label"))
        return any(
            _normalise_text(value) in {category, category_label}
            for value in values
        )

    return False


def _item_search_text(item: dict[str, Any]) -> str:
    values = [
        item.get("display_name_clean"),
        item.get("display_name"),
        item.get("canonical_name"),
        item.get("category_label"),
        item.get("grocery_category"),
    ]
    values.extend(_list_values(item.get("source_item_names")))
    values.extend(_list_values(item.get("ingredient_names_seen")))
    return _normalise_text(" ".join(_clean_text(value) for value in values))


def _match_values(value: Any) -> list[str]:
    text = _clean_text(value)
    if not text:
        return []
    return [item.strip() for item in text.split("|") if item.strip()]


def _list_values(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_clean_text(item) for item in value if _clean_text(item)]
    text = _clean_text(value)
    if not text:
        return []
    if ";" in text:
        return [item.strip() for item in text.split(";") if item.strip()]
    return [text]


def _contains_phrase(text: str, phrase: Any) -> bool:
    clean_text = f" {_normalise_text(text)} "
    clean_phrase = _normalise_text(phrase)
    if not clean_phrase:
        return False
    return f" {clean_phrase} " in clean_text


def _normalise_text(value: Any) -> str:
    text = _clean_text(value).lower().replace("_", " ").replace("-", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_float(value: Any, fallback: float = 0.0) -> float:
    try:
        if value is None or str(value).strip() == "":
            return fallback
        return float(value)
    except (TypeError, ValueError):
        return fallback
