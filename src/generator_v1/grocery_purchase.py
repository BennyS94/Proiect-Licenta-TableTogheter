from __future__ import annotations

import csv
import math
import re
from pathlib import Path
from typing import Any

from src.generator_v1.grocery_cooked_raw import (
    apply_cooked_to_raw_conversion,
    detect_cooked_raw_applicability,
    load_cooked_to_raw_rules,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PURCHASE_RULES_PATH = (
    PROJECT_ROOT / "data/grocery/reference/grocery_purchase_rules_v1.csv"
)

DEFAULT_CONFIG = {
    "fallback_rounding_strategy": "round_to_50g",
    "enable_cooked_to_raw_conversion": False,
    "cooked_to_raw_rules_path": None,
}


def load_grocery_purchase_rules(path: Path | str | None = None) -> list[dict[str, Any]]:
    resolved_path = _resolve_rules_path(path)
    with resolved_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    rules = [_normalise_rule(row) for row in rows if _clean_text(row.get("rule_id"))]
    return rules


def apply_purchase_rules(
    grocery_items: list[dict[str, Any]],
    rules: list[dict[str, Any]],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    config_data = dict(DEFAULT_CONFIG)
    if config:
        config_data.update(config)

    updated_items: list[dict[str, Any]] = []
    warnings: list[str] = []
    confidence_counts = {"high": 0, "medium": 0, "low": 0, "none": 0}
    warning_counts: dict[str, int] = {}
    package_rounded_count = 0
    piece_rounded_count = 0
    fallback_grams_only_count = 0
    pantry_basic_count = 0
    cooked_to_raw_converted_count = 0
    cooked_to_raw_warning_count = 0
    cooked_to_raw_rules = []
    if bool(config_data.get("enable_cooked_to_raw_conversion", False)):
        cooked_to_raw_rules = load_cooked_to_raw_rules(
            config_data.get("cooked_to_raw_rules_path")
        )

    for item in grocery_items:
        conversion = _cooked_to_raw_conversion(item, cooked_to_raw_rules)
        purchase_item = _purchase_item_for_conversion(item, conversion)
        match = _match_rule(purchase_item, rules)
        suggestion = build_purchase_suggestion(
            purchase_item,
            match.get("rule"),
            config=config_data,
            match_type=match.get("match_type", ""),
            confidence=match.get("confidence", "none"),
        )
        if conversion.get("cooked_to_raw_applied"):
            suggestion = _apply_conversion_display_to_suggestion(suggestion, conversion)
            cooked_to_raw_converted_count += 1
            if conversion.get("cooked_to_raw_warning"):
                cooked_to_raw_warning_count += 1
        updated = dict(item)
        updated.update(conversion)
        updated.update(
            {
                "needed_grams_exact": suggestion["needed_grams_exact"],
                "needed_grams_display": suggestion["needed_grams_display"],
                "purchase_display": suggestion["purchase_display"],
                "purchase_unit_type": suggestion["purchase_unit_type"],
                "purchase_quantity": suggestion["purchase_quantity"],
                "purchase_amount_grams": suggestion["purchase_amount_grams"],
                "estimated_leftover_grams": suggestion["estimated_leftover_grams"],
                "purchase_rule_id": suggestion["purchase_rule_id"],
                "purchase_rule_match_type": suggestion["purchase_rule_match_type"],
                "purchase_rule_confidence": suggestion["purchase_rule_confidence"],
                "purchase_warnings": suggestion["purchase_warnings"],
                "purchase_is_pantry_basic": suggestion["purchase_is_pantry_basic"],
                "purchase_rounding_strategy": suggestion["purchase_rounding_strategy"],
                "purchase_basis_grams": suggestion["purchase_basis_grams"],
            }
        )
        updated_items.append(updated)

        confidence = str(updated["purchase_rule_confidence"] or "none")
        confidence_counts[confidence] = confidence_counts.get(confidence, 0) + 1
        strategy = str(updated.get("purchase_rounding_strategy") or "")
        unit_type = str(updated.get("purchase_unit_type") or "")
        if strategy == "ceil_to_package" or unit_type in {"package", "bag", "tub", "carton", "bottle"}:
            package_rounded_count += 1
        if strategy == "ceil_to_piece" or unit_type == "pieces":
            piece_rounded_count += 1
        if "purchase_fallback_grams_only" in updated["purchase_warnings"]:
            fallback_grams_only_count += 1
        if updated.get("purchase_is_pantry_basic"):
            pantry_basic_count += 1
        for warning in updated["purchase_warnings"]:
            warning_counts[warning] = warning_counts.get(warning, 0) + 1
            warnings.append(warning)

    cooked_raw_ambiguity_count = sum(
        1
        for item in updated_items
        if "cooked_raw_purchase_ambiguity" in item.get("warnings", [])
        or "cooked_raw_purchase_ambiguity" in item.get("purchase_warnings", [])
    )
    summary = {
        "purchase_item_count": len(updated_items),
        "items_with_purchase_suggestions": sum(
            1 for item in updated_items if item.get("purchase_rule_confidence") != "none"
        ),
        "purchase_confidence_counts": dict(sorted(confidence_counts.items())),
        "pantry_basic_count": pantry_basic_count,
        "package_rounded_items_count": package_rounded_count,
        "piece_rounded_items_count": piece_rounded_count,
        "fallback_grams_only_count": fallback_grams_only_count,
        "cooked_raw_ambiguity_count": cooked_raw_ambiguity_count,
        "cooked_to_raw_converted_item_count": cooked_to_raw_converted_count,
        "cooked_to_raw_warning_count": cooked_to_raw_warning_count,
        "cooked_raw_no_conversion_count": max(
            0,
            cooked_raw_ambiguity_count - cooked_to_raw_converted_count,
        ),
        "purchase_warning_counts": dict(sorted(warning_counts.items())),
        "rules_loaded_count": len(rules),
        "cooked_to_raw_rules_loaded_count": len(cooked_to_raw_rules),
    }
    return {
        "items": updated_items,
        "summary": summary,
        "warnings": sorted(dict.fromkeys(warnings)),
    }


def build_purchase_suggestion(
    item: dict[str, Any],
    matched_rule: dict[str, Any] | None,
    config: dict[str, Any] | None = None,
    match_type: str = "",
    confidence: str = "none",
) -> dict[str, Any]:
    config_data = dict(DEFAULT_CONFIG)
    if config:
        config_data.update(config)

    needed_grams = max(0.0, _to_float(item.get("total_grams")))
    purchase_warnings = _base_purchase_warnings(item)

    if not matched_rule:
        if "unclear_grocery_item_name" in purchase_warnings:
            purchase_warnings.extend(["purchase_rule_missing", "purchase_review_before_buying"])
            return _suggestion_result(
                item=item,
                rule={"rule_id": "", "match_type": "", "rounding_strategy": "review"},
                needed_grams=needed_grams,
                purchase_display=f"review item; need about {_format_amount_grams(needed_grams)}",
                purchase_unit_type="review",
                purchase_quantity=None,
                purchase_amount_grams=None,
                match_type="",
                confidence="none",
                warnings=purchase_warnings,
                is_pantry_basic=False,
            )
        matched_rule = {
            "rule_id": "",
            "match_type": "",
            "purchase_unit_type": "grams",
            "rounding_strategy": str(config_data.get("fallback_rounding_strategy")),
            "display_unit": "g",
            "is_pantry_basic": False,
            "allow_purchase_rounding": True,
        }
        confidence = "none"
        purchase_warnings.extend(["purchase_rule_missing", "purchase_fallback_grams_only"])

    rule = matched_rule
    unit_type = _clean_text(rule.get("purchase_unit_type")) or "grams"
    strategy = _clean_text(rule.get("rounding_strategy")) or "exact_grams"
    is_pantry_basic = _truthy(rule.get("is_pantry_basic")) or bool(
        item.get("is_pantry_basic")
    )

    if strategy == "pantry_check" or unit_type == "pantry_check":
        purchase_warnings.append("purchase_pantry_check")
        return _suggestion_result(
            item=item,
            rule=rule,
            needed_grams=needed_grams,
            purchase_display=f"check pantry; need about {_format_amount_grams(needed_grams)}",
            purchase_unit_type="pantry_check",
            purchase_quantity=None,
            purchase_amount_grams=None,
            match_type=match_type or _clean_text(rule.get("match_type")),
            confidence=confidence,
            warnings=purchase_warnings,
            is_pantry_basic=True,
        )

    if strategy == "ceil_to_piece" or unit_type == "pieces":
        grams_per_unit = max(0.1, _to_float(rule.get("grams_per_unit"), fallback=50.0))
        quantity = int(math.ceil(needed_grams / grams_per_unit)) if needed_grams > 0 else 0
        purchase_amount = quantity * grams_per_unit
        display_unit = _clean_text(rule.get("display_unit")) or "pieces"
        purchase_display = _piece_purchase_display(quantity, display_unit, purchase_amount)
        return _suggestion_result(
            item=item,
            rule=rule,
            needed_grams=needed_grams,
            purchase_display=purchase_display,
            purchase_unit_type="pieces",
            purchase_quantity=quantity,
            purchase_amount_grams=purchase_amount,
            match_type=match_type or _clean_text(rule.get("match_type")),
            confidence=confidence,
            warnings=purchase_warnings,
            is_pantry_basic=is_pantry_basic,
        )

    if strategy == "ceil_to_package" or unit_type in {"package", "bag", "tub", "bottle"}:
        package_grams = max(
            0.1,
            _to_float(
                rule.get("default_package_grams"),
                fallback=_to_float(rule.get("grams_per_unit"), fallback=100.0),
            ),
        )
        quantity = int(math.ceil(needed_grams / package_grams)) if needed_grams > 0 else 0
        purchase_amount = quantity * package_grams
        package_label = _clean_text(rule.get("default_package_label")) or (
            f"{_format_amount_grams(package_grams)} {unit_type}"
        )
        purchase_display = _package_purchase_display(quantity, package_label)
        return _suggestion_result(
            item=item,
            rule=rule,
            needed_grams=needed_grams,
            purchase_display=purchase_display,
            purchase_unit_type=unit_type,
            purchase_quantity=quantity,
            purchase_amount_grams=purchase_amount,
            match_type=match_type or _clean_text(rule.get("match_type")),
            confidence=confidence,
            warnings=purchase_warnings,
            is_pantry_basic=is_pantry_basic,
        )

    if strategy in {"ceil_to_liter", "ceil_to_half_liter"} or unit_type == "carton":
        step = 500.0 if strategy == "ceil_to_half_liter" else 1000.0
        grams_per_unit = max(0.1, _to_float(rule.get("grams_per_unit"), fallback=step))
        step = min(step, grams_per_unit) if grams_per_unit > 0 else step
        quantity = int(math.ceil(needed_grams / step)) if needed_grams > 0 else 0
        purchase_amount = quantity * step
        purchase_display = _liter_purchase_display(purchase_amount, unit_type)
        return _suggestion_result(
            item=item,
            rule=rule,
            needed_grams=needed_grams,
            purchase_display=purchase_display,
            purchase_unit_type=unit_type,
            purchase_quantity=quantity,
            purchase_amount_grams=purchase_amount,
            match_type=match_type or _clean_text(rule.get("match_type")),
            confidence=confidence,
            warnings=purchase_warnings,
            is_pantry_basic=is_pantry_basic,
        )

    if strategy == "round_to_100g":
        purchase_amount = _ceil_to_step(needed_grams, 100.0)
    elif strategy == "round_to_50g":
        purchase_amount = _ceil_to_step(needed_grams, 50.0)
    else:
        purchase_amount = needed_grams

    if confidence == "none":
        purchase_warnings.append("purchase_fallback_grams_only")
    purchase_display = f"about {_format_amount_grams(purchase_amount)}"
    return _suggestion_result(
        item=item,
        rule=rule,
        needed_grams=needed_grams,
        purchase_display=purchase_display,
        purchase_unit_type=unit_type,
        purchase_quantity=None,
        purchase_amount_grams=purchase_amount,
        match_type=match_type or _clean_text(rule.get("match_type")),
        confidence=confidence,
        warnings=purchase_warnings,
        is_pantry_basic=is_pantry_basic,
    )


def _resolve_rules_path(path: Path | str | None) -> Path:
    if path is None or str(path).strip() == "":
        return DEFAULT_PURCHASE_RULES_PATH
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
        "grocery_category",
        "purchase_unit_type",
        "default_package_label",
        "rounding_strategy",
        "display_unit",
        "notes",
    ]:
        rule[key] = _clean_text(rule.get(key))
    for key in ["grams_per_unit", "default_package_grams"]:
        raw_value = _clean_text(rule.get(key))
        rule[key] = None if not raw_value else _to_float(raw_value)
    rule["is_pantry_basic"] = _truthy(rule.get("is_pantry_basic"))
    rule["allow_purchase_rounding"] = _truthy(rule.get("allow_purchase_rounding"))
    rule["_match_values"] = _match_values(rule.get("match_value"))
    return rule


def _match_rule(item: dict[str, Any], rules: list[dict[str, Any]]) -> dict[str, Any]:
    for match_type in [
        "mapped_food_id",
        "display_alias",
        "normalized_name_keyword",
        "category_keyword",
    ]:
        if match_type == "display_alias":
            for rule in rules:
                if rule.get("match_type") != match_type:
                    continue
                matched = _rule_matches_item(item, rule, match_type)
                if matched == "exact":
                    return {
                        "rule": rule,
                        "match_type": match_type,
                        "confidence": _match_confidence(match_type, matched),
                    }
            for rule in rules:
                if rule.get("match_type") != match_type:
                    continue
                matched = _rule_matches_item(item, rule, match_type)
                if matched:
                    return {
                        "rule": rule,
                        "match_type": match_type,
                        "confidence": _match_confidence(match_type, matched),
                    }
            continue
        for rule in rules:
            if rule.get("match_type") != match_type:
                continue
            matched = _rule_matches_item(item, rule, match_type)
            if matched:
                return {
                    "rule": rule,
                    "match_type": match_type,
                    "confidence": _match_confidence(match_type, matched),
                }
    return {"rule": None, "match_type": "", "confidence": "none"}


def _rule_matches_item(
    item: dict[str, Any],
    rule: dict[str, Any],
    match_type: str,
) -> str:
    values = rule.get("_match_values", [])
    if not values:
        return ""
    if match_type == "mapped_food_id":
        source_ids = set(_list_values(item.get("source_mapped_food_ids")))
        mapped_food_id = _clean_text(item.get("mapped_food_id"))
        if mapped_food_id:
            source_ids.add(mapped_food_id)
        normalised_ids = {_normalise_text(value) for value in source_ids}
        for value in values:
            if _normalise_text(value) in normalised_ids:
                return "exact"
        return ""

    if match_type == "display_alias":
        display_name = _normalise_text(item.get("display_name_clean") or item.get("display_name"))
        for value in values:
            value_text = _normalise_text(value)
            if display_name == value_text:
                return "exact"
        for value in values:
            if _contains_phrase(display_name, value):
                return "contains"
        return ""

    if match_type == "normalized_name_keyword":
        text = _item_search_text(item)
        for value in values:
            if _contains_phrase(text, value):
                return "contains"
        return ""

    if match_type == "category_keyword":
        category = _normalise_text(item.get("grocery_category"))
        category_label = _normalise_text(item.get("category_label"))
        for value in values:
            value_text = _normalise_text(value)
            if category == value_text or category_label == value_text:
                return "exact"
        return ""

    return ""


def _match_confidence(match_type: str, matched: str) -> str:
    if match_type == "mapped_food_id":
        return "high"
    if match_type == "display_alias" and matched == "exact":
        return "high"
    if match_type in {"display_alias", "normalized_name_keyword"}:
        return "medium"
    if match_type == "category_keyword":
        return "low"
    return "none"


def _suggestion_result(
    item: dict[str, Any],
    rule: dict[str, Any],
    needed_grams: float,
    purchase_display: str,
    purchase_unit_type: str,
    purchase_quantity: int | float | None,
    purchase_amount_grams: float | None,
    match_type: str,
    confidence: str,
    warnings: list[str],
    is_pantry_basic: bool,
) -> dict[str, Any]:
    if purchase_amount_grams is None:
        leftover = None
    else:
        leftover = round(float(purchase_amount_grams) - needed_grams, 1)
    return {
        "needed_grams_exact": round(needed_grams, 1),
        "needed_grams_display": _format_needed_grams(needed_grams),
        "purchase_basis_grams": round(needed_grams, 1),
        "purchase_display": purchase_display,
        "purchase_unit_type": purchase_unit_type,
        "purchase_quantity": purchase_quantity,
        "purchase_amount_grams": None
        if purchase_amount_grams is None
        else round(float(purchase_amount_grams), 1),
        "estimated_leftover_grams": leftover,
        "purchase_rule_id": _clean_text(rule.get("rule_id")),
        "purchase_rule_match_type": match_type,
        "purchase_rule_confidence": confidence or "none",
        "purchase_warnings": sorted(dict.fromkeys(item for item in warnings if item)),
        "purchase_is_pantry_basic": is_pantry_basic,
        "purchase_rounding_strategy": _clean_text(rule.get("rounding_strategy")),
    }


def _base_purchase_warnings(item: dict[str, Any]) -> list[str]:
    warnings: list[str] = []
    item_warnings = set(str(warning) for warning in item.get("warnings", []))
    if "cooked_raw_purchase_ambiguity" in item_warnings:
        warnings.extend(
            ["cooked_raw_purchase_ambiguity", "cooked_raw_not_converted_to_raw"]
        )
    if "unclear_grocery_item_name" in item_warnings:
        warnings.append("unclear_grocery_item_name")
    if "normalized_name_fallback" in item_warnings:
        warnings.append("normalized_name_fallback")
    return warnings


def _cooked_to_raw_conversion(
    item: dict[str, Any],
    cooked_to_raw_rules: list[dict[str, Any]],
) -> dict[str, Any]:
    if not cooked_to_raw_rules:
        return apply_cooked_to_raw_conversion(item, None)
    matched_rule = detect_cooked_raw_applicability(item, cooked_to_raw_rules)
    return apply_cooked_to_raw_conversion(item, matched_rule)


def _purchase_item_for_conversion(
    item: dict[str, Any],
    conversion: dict[str, Any],
) -> dict[str, Any]:
    if not conversion.get("cooked_to_raw_applied"):
        return dict(item)

    raw_name = _clean_text(conversion.get("raw_purchase_display_name"))
    raw_grams = _to_float(conversion.get("raw_equivalent_grams"))
    purchase_item = dict(item)
    purchase_item["total_grams"] = raw_grams
    purchase_item["display_name_clean"] = raw_name
    purchase_item["display_name"] = raw_name
    purchase_item["canonical_name"] = raw_name
    purchase_item["ingredient_names_seen"] = [raw_name]
    return purchase_item


def _apply_conversion_display_to_suggestion(
    suggestion: dict[str, Any],
    conversion: dict[str, Any],
) -> dict[str, Any]:
    updated = dict(suggestion)
    original_grams = _to_float(conversion.get("original_needed_grams"))
    raw_grams = _to_float(conversion.get("raw_equivalent_grams"))
    raw_name = _raw_purchase_phrase(conversion.get("raw_purchase_display_name"))
    warning_code = _clean_text(conversion.get("cooked_to_raw_warning"))
    purchase_warnings = [
        warning
        for warning in updated.get("purchase_warnings", [])
        if warning != "cooked_raw_not_converted_to_raw"
    ]
    if warning_code:
        purchase_warnings.append(warning_code)

    updated["needed_grams_exact"] = round(original_grams, 1)
    updated["needed_grams_display"] = f"{_format_needed_grams(original_grams)} cooked"
    updated["purchase_basis_grams"] = round(raw_grams, 1)
    raw_display = _format_needed_grams(raw_grams).lstrip("~")
    updated["purchase_display"] = f"about {raw_display} {raw_name}"
    updated["purchase_unit_type"] = "grams"
    updated["purchase_quantity"] = None
    updated["purchase_amount_grams"] = round(raw_grams, 1)
    updated["estimated_leftover_grams"] = 0.0
    updated["purchase_rounding_strategy"] = "cooked_to_raw_estimate"
    updated["purchase_warnings"] = sorted(
        dict.fromkeys(warning for warning in purchase_warnings if warning)
    )
    return updated


def _raw_purchase_phrase(value: Any) -> str:
    text = _clean_text(value)
    normalised = _normalise_text(text)
    if normalised == "rice raw":
        return "raw rice"
    if normalised == "pasta dry":
        return "dry pasta"
    if normalised == "beans dry":
        return "dry beans"
    return text.lower() or "raw equivalent"


def _piece_purchase_display(
    quantity: int,
    display_unit: str,
    purchase_amount_grams: float,
) -> str:
    if quantity == 0:
        return "none"
    unit = _pluralise(display_unit, quantity)
    if display_unit in {"egg", "eggs", "piece", "pieces"}:
        return f"{quantity} {unit}"
    return f"{quantity} {unit} / about {_format_amount_grams(purchase_amount_grams)}"


def _package_purchase_display(quantity: int, package_label: str) -> str:
    if quantity == 0:
        return "none"
    label = package_label
    if quantity != 1:
        label = _pluralise_label(package_label)
    return f"{quantity} x {label}"


def _liter_purchase_display(purchase_amount_grams: float, unit_type: str) -> str:
    if purchase_amount_grams <= 0:
        return "none"
    liters = purchase_amount_grams / 1000.0
    container = "carton" if unit_type == "carton" else unit_type
    if abs(liters - round(liters)) < 0.01:
        quantity = int(round(liters))
        return f"{quantity} x 1L {_pluralise(container, quantity)}"
    return f"1 x {liters:.1f}L {container}"


def _ceil_to_step(value: float, step: float) -> float:
    if value <= 0:
        return 0.0
    return math.ceil(value / step) * step


def _format_needed_grams(value: float) -> str:
    if value >= 1000:
        kg = value / 1000.0
        if abs(kg - round(kg)) < 0.01:
            return f"~{int(round(kg))}kg"
        amount = f"{kg:.2f}".rstrip("0").rstrip(".")
        return f"~{amount}kg"
    if value >= 100:
        return f"~{int(round(value))}g"
    if value >= 10:
        return f"~{int(round(value))}g"
    if value >= 1:
        amount = f"{value:.1f}".rstrip("0").rstrip(".")
        return f"~{amount}g"
    return "<1g"


def _format_amount_grams(value: float) -> str:
    if value >= 1000:
        kg = value / 1000.0
        if abs(kg - round(kg)) < 0.01:
            return f"{int(round(kg))}kg"
        return f"{kg:.2f}".rstrip("0").rstrip(".") + "kg"
    if abs(value - round(value)) < 0.05:
        return f"{int(round(value))}g"
    return f"{value:.1f}".rstrip("0").rstrip(".") + "g"


def _pluralise(value: str, quantity: int) -> str:
    text = value.strip()
    if quantity == 1 or text.endswith("s"):
        return text
    return text + "s"


def _pluralise_label(value: str) -> str:
    text = value.strip()
    words = text.split()
    if not words:
        return text
    words[-1] = _pluralise(words[-1], 2)
    return " ".join(words)


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


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return _clean_text(value).lower() in {"true", "1", "yes", "y"}
