from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_SIMILARITY_DATASET_PATH = Path("data/recipesdb/current")
DATASET_PROFILE_PATHS = {
    "current": DEFAULT_SIMILARITY_DATASET_PATH,
    "v1_2_demo_final": DEFAULT_SIMILARITY_DATASET_PATH,
    "v1_2_demo_final_time_layer": DEFAULT_SIMILARITY_DATASET_PATH,
}

NUMERIC_FEATURE_COLUMNS = [
    "kcal_per_serving",
    "protein_g_per_serving",
    "carbs_g_per_serving",
    "fat_g_per_serving",
    "effective_time_min",
]

DEFAULT_CONFIG = {
    "numeric_weight": 0.68,
    "slot_bonus": 0.10,
    "kind_bonus": 0.06,
    "category_bonus": 0.04,
    "family_bonus": 0.04,
    "main_protein_bonus": 0.08,
    "incompatible_slot_penalty": 0.35,
    "macro_mismatch_penalty": 0.16,
    "missing_nutrition_penalty": 0.18,
    "missing_time_penalty": 0.04,
}


def load_recipe_similarity_source(dataset_profile_or_path: str) -> dict[str, Any]:
    dataset_path = _resolve_dataset_path(dataset_profile_or_path)
    recipes_path = dataset_path / "recipes.csv"
    nutrition_path = dataset_path / "recipe_nutrition_cache.csv"
    ingredients_path = dataset_path / "recipe_ingredients.csv"

    if not recipes_path.exists():
        raise FileNotFoundError(f"Lipseste recipes.csv pentru similarity source: {recipes_path}")
    if not nutrition_path.exists():
        raise FileNotFoundError(
            f"Lipseste recipe_nutrition_cache.csv pentru similarity source: {nutrition_path}"
        )

    recipes_df = pd.read_csv(recipes_path)
    nutrition_df = pd.read_csv(nutrition_path)
    ingredients_df = (
        pd.read_csv(ingredients_path)
        if ingredients_path.exists()
        else pd.DataFrame(columns=["recipe_id"])
    )
    return {
        "dataset_profile_or_path": dataset_profile_or_path,
        "dataset_path": dataset_path,
        "recipes": recipes_df,
        "nutrition": nutrition_df,
        "ingredients": ingredients_df,
    }


def build_recipe_similarity_features(
    recipes_df: pd.DataFrame,
    nutrition_df: pd.DataFrame,
    ingredients_df: pd.DataFrame | None = None,
) -> pd.DataFrame:
    _require_columns(recipes_df, {"recipe_id", "display_name"}, "recipes")
    _require_columns(
        nutrition_df,
        {
            "recipe_id",
            "energy_kcal_per_serving",
            "protein_g_per_serving",
            "carbs_g_per_serving",
            "fat_g_per_serving",
        },
        "recipe_nutrition_cache",
    )

    ingredient_features = _ingredient_features(ingredients_df)
    features = recipes_df.merge(
        nutrition_df,
        on="recipe_id",
        how="left",
        suffixes=("", "_nutrition"),
    ).merge(ingredient_features, on="recipe_id", how="left")

    features["kcal_per_serving"] = _numeric_series(features, "energy_kcal_per_serving")
    features["protein_g_per_serving"] = _numeric_series(features, "protein_g_per_serving")
    features["carbs_g_per_serving"] = _numeric_series(features, "carbs_g_per_serving")
    features["fat_g_per_serving"] = _numeric_series(features, "fat_g_per_serving")
    features["effective_time_min"] = _best_time_series(features)
    features["allowed_slots"] = features.get(
        "allowed_slots_json",
        pd.Series("", index=features.index),
    ).map(_parse_allowed_slots)
    features["allowed_slots_text"] = features["allowed_slots"].map(
        lambda values: "|".join(sorted(values))
    )
    features["is_active_bool"] = _numeric_series(features, "is_active").fillna(0).eq(1)
    features["has_complete_nutrition"] = features[NUMERIC_FEATURE_COLUMNS[:4]].notna().all(axis=1)
    features["has_usable_time"] = features["effective_time_min"].notna()
    features["nutrition_confidence"] = features.get(
        "cache_status",
        pd.Series("", index=features.index),
    ).map(_nutrition_confidence)
    features["main_protein_family"] = features["main_protein_name"].map(_protein_family)
    features["dominant_ingredient_family"] = features["dominant_ingredient_name"].map(
        _protein_family
    )
    features["similarity_low_confidence"] = features.apply(_low_confidence_reason, axis=1)

    return features[_feature_columns(features)].copy()


def build_recipe_feature_matrix(
    features_df: pd.DataFrame,
    config: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    resolved_config = _config(config)
    matrix = pd.DataFrame(index=features_df.index)
    stats: dict[str, dict[str, float]] = {}
    for column in NUMERIC_FEATURE_COLUMNS:
        values = pd.to_numeric(features_df.get(column), errors="coerce")
        median = float(values.median()) if values.notna().any() else 0.0
        filled = values.fillna(median)
        min_value = float(filled.min()) if len(filled) else 0.0
        max_value = float(filled.max()) if len(filled) else 0.0
        span = max(max_value - min_value, 1.0)
        matrix[column] = (filled - min_value) / span
        stats[column] = {
            "median": median,
            "min": min_value,
            "max": max_value,
            "span": span,
        }
    matrix["recipe_id"] = features_df["recipe_id"].astype(str)
    matrix_config = {
        "config": resolved_config,
        "numeric_stats": stats,
        "numeric_columns": list(NUMERIC_FEATURE_COLUMNS),
    }
    return matrix, matrix_config


def find_similar_recipes(
    recipe_id: str,
    features_df: pd.DataFrame,
    top_k: int = 10,
    filters: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    source_id = str(recipe_id).strip()
    if not source_id:
        return []
    source_rows = features_df.loc[features_df["recipe_id"].astype(str).eq(source_id)]
    if source_rows.empty:
        return []

    resolved_config = _config(config)
    matrix, _matrix_config = build_recipe_feature_matrix(features_df, resolved_config)
    source_index = source_rows.index[0]
    source_recipe = features_df.loc[source_index]
    source_vector = matrix.loc[source_index]
    candidate_rows = _apply_filters(features_df, source_recipe, filters)

    results: list[dict[str, Any]] = []
    for candidate_index, candidate_recipe in candidate_rows.iterrows():
        candidate_id = str(candidate_recipe.get("recipe_id") or "")
        if candidate_id == source_id:
            continue
        numeric_distance = _numeric_distance(source_vector, matrix.loc[candidate_index])
        score_parts = _similarity_score_parts(
            source_recipe,
            candidate_recipe,
            numeric_distance,
            resolved_config,
        )
        why_similar = explain_recipe_similarity(
            source_recipe,
            candidate_recipe,
            source_recipe,
            candidate_recipe,
        )
        warnings = _similarity_warnings(source_recipe, candidate_recipe, score_parts)
        results.append(
            {
                "source_recipe_id": source_id,
                "candidate_recipe_id": candidate_id,
                "source_display_name": _clean_text(source_recipe.get("display_name")),
                "candidate_display_name": _clean_text(candidate_recipe.get("display_name")),
                "similarity_score": round(score_parts["similarity_score"], 4),
                "numeric_distance": round(numeric_distance, 4),
                "macro_delta_kcal": _delta(candidate_recipe, source_recipe, "kcal_per_serving"),
                "macro_delta_protein": _delta(
                    candidate_recipe,
                    source_recipe,
                    "protein_g_per_serving",
                ),
                "macro_delta_carbs": _delta(
                    candidate_recipe,
                    source_recipe,
                    "carbs_g_per_serving",
                ),
                "macro_delta_fat": _delta(
                    candidate_recipe,
                    source_recipe,
                    "fat_g_per_serving",
                ),
                "time_delta_min": _delta(candidate_recipe, source_recipe, "effective_time_min"),
                "slot_match": score_parts["slot_match"],
                "category_match": score_parts["category_match"],
                "family_match": score_parts["family_match"],
                "main_protein_match": score_parts["main_protein_match"],
                "why_similar": "; ".join(why_similar),
                "warnings": "; ".join(warnings),
            }
        )

    results.sort(
        key=lambda row: (
            -float(row["similarity_score"]),
            float(row["numeric_distance"]),
            str(row["candidate_display_name"]),
            str(row["candidate_recipe_id"]),
        )
    )
    return results[: max(int(top_k or 10), 0)]


def explain_recipe_similarity(
    source_recipe: Any,
    candidate_recipe: Any,
    feature_row_source: Any,
    feature_row_candidate: Any,
) -> list[str]:
    reasons: list[str] = []
    source_slots = _as_slot_set(_get_value(feature_row_source, "allowed_slots"))
    candidate_slots = _as_slot_set(_get_value(feature_row_candidate, "allowed_slots"))
    if source_slots and candidate_slots and source_slots & candidate_slots:
        reasons.append("slot_overlap:" + ",".join(sorted(source_slots & candidate_slots)))
    if _same_text(source_recipe, candidate_recipe, "recipe_kind"):
        reasons.append("same_recipe_kind")
    if _same_text(source_recipe, candidate_recipe, "recipe_category"):
        reasons.append("same_category")
    if _same_text(source_recipe, candidate_recipe, "recipe_family_name"):
        reasons.append("same_family")
    if _same_text(source_recipe, candidate_recipe, "main_protein_family"):
        protein = _clean_text(_get_value(source_recipe, "main_protein_family"))
        if protein:
            reasons.append(f"same_main_protein:{protein}")

    kcal_delta = abs(_delta(feature_row_candidate, feature_row_source, "kcal_per_serving") or 0.0)
    protein_delta = abs(
        _delta(feature_row_candidate, feature_row_source, "protein_g_per_serving") or 0.0
    )
    time_delta = abs(_delta(feature_row_candidate, feature_row_source, "effective_time_min") or 0.0)
    if kcal_delta <= 120:
        reasons.append("kcal_close")
    if protein_delta <= 12:
        reasons.append("protein_close")
    if time_delta <= 20:
        reasons.append("time_close")

    return reasons or ["numeric_similarity_only"]


def _resolve_dataset_path(dataset_profile_or_path: str) -> Path:
    value = str(dataset_profile_or_path or "").strip() or "current"
    if value in DATASET_PROFILE_PATHS:
        return DATASET_PROFILE_PATHS[value]
    path = Path(value)
    if path.is_file():
        return path.parent
    return path


def _ingredient_features(ingredients_df: pd.DataFrame | None) -> pd.DataFrame:
    if ingredients_df is None or ingredients_df.empty or "recipe_id" not in ingredients_df.columns:
        return pd.DataFrame(
            columns=[
                "recipe_id",
                "main_protein_name",
                "dominant_ingredient_name",
                "has_ingredient_similarity_signal",
            ]
        )

    ingredients = ingredients_df.copy()
    ingredients["quantity_grams_numeric"] = pd.to_numeric(
        ingredients.get("quantity_grams_estimated"),
        errors="coerce",
    ).fillna(0.0)
    ingredients["ingredient_identity"] = ingredients.apply(_ingredient_identity, axis=1)
    ingredients["is_protein_signal"] = ingredients.apply(_is_protein_signal, axis=1)
    ingredients["usable_for_identity"] = ingredients["ingredient_identity"].astype(str).str.len().gt(0)

    rows = []
    for recipe_id, group in ingredients.groupby("recipe_id", dropna=False):
        usable = group.loc[group["usable_for_identity"]].copy()
        accepted = usable.loc[
            usable.get("mapping_status", "").fillna("").astype(str).eq("accepted_auto")
        ].copy()
        identity_source = accepted if not accepted.empty else usable

        protein_rows = identity_source.loc[identity_source["is_protein_signal"]].copy()
        main_protein = _top_ingredient_name(protein_rows)
        dominant_rows = identity_source.loc[
            ~identity_source["ingredient_role"].fillna("").astype(str).isin(
                ["seasoning", "sauce"]
            )
        ].copy()
        dominant = _top_ingredient_name(dominant_rows)
        rows.append(
            {
                "recipe_id": str(recipe_id),
                "main_protein_name": main_protein,
                "dominant_ingredient_name": dominant,
                "has_ingredient_similarity_signal": bool(main_protein or dominant),
            }
        )
    return pd.DataFrame(rows)


def _top_ingredient_name(rows: pd.DataFrame) -> str:
    if rows.empty:
        return ""
    ordered = rows.sort_values(
        by=["quantity_grams_numeric", "ingredient_position"],
        ascending=[False, True],
        na_position="last",
    )
    return _clean_text(ordered.iloc[0].get("ingredient_identity"))


def _ingredient_identity(row: Any) -> str:
    for column in (
        "mapped_food_canonical_name",
        "ingredient_slot_key",
        "ingredient_name_normalized",
        "ingredient_name_parsed",
    ):
        text = _clean_text(row.get(column))
        if text:
            return text
    return ""


def _is_protein_signal(row: Any) -> bool:
    role = _normalize_text(row.get("ingredient_role"))
    text = _normalize_text(
        " ".join(
            [
                _clean_text(row.get("ingredient_identity")),
                _clean_text(row.get("ingredient_slot_key")),
                _clean_text(row.get("ingredient_raw_text")),
            ]
        )
    )
    if role in {"protein", "protein source", "protein_source"}:
        return True
    return bool(_protein_family(text))


def _protein_family(value: Any) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    mapping = [
        ("chicken", ["chicken", "poultry"]),
        ("turkey", ["turkey"]),
        ("beef", ["beef", "steak", "sirloin", "veal", "short rib"]),
        ("pork", ["pork", "ham", "bacon", "sausage", "prosciutto"]),
        ("fish", ["fish", "cod", "salmon", "tuna", "tilapia", "halibut"]),
        ("seafood", ["shrimp", "scallop", "crab", "clam", "oyster", "mussel"]),
        ("egg", ["egg", "eggs"]),
        ("dairy", ["cheese", "milk", "yogurt", "ricotta", "mozzarella", "cheddar"]),
        ("legume", ["bean", "beans", "lentil", "lentils", "chickpea", "tofu"]),
    ]
    for family, keywords in mapping:
        if any(_contains_word(text, keyword) for keyword in keywords):
            return family
    return ""


def _best_time_series(features: pd.DataFrame) -> pd.Series:
    effective = _numeric_series(features, "effective_time_min_for_scoring")
    total = _numeric_series(features, "total_time_min")
    return effective.where(effective.notna(), total)


def _feature_columns(features: pd.DataFrame) -> list[str]:
    preferred = [
        "recipe_id",
        "display_name",
        "recipe_name",
        "recipe_kind",
        "recipe_category",
        "recipe_subcategory",
        "recipe_family_name",
        "allowed_slots",
        "allowed_slots_text",
        "is_active",
        "is_active_bool",
        "scope_status",
        "cache_status",
        "content_quality_status",
        "kcal_per_serving",
        "protein_g_per_serving",
        "carbs_g_per_serving",
        "fat_g_per_serving",
        "effective_time_min",
        "has_complete_nutrition",
        "has_usable_time",
        "nutrition_confidence",
        "main_protein_name",
        "main_protein_family",
        "dominant_ingredient_name",
        "dominant_ingredient_family",
        "has_ingredient_similarity_signal",
        "similarity_low_confidence",
    ]
    return [column for column in preferred if column in features.columns]


def _apply_filters(
    features_df: pd.DataFrame,
    source_recipe: Any,
    filters: dict[str, Any] | None,
) -> pd.DataFrame:
    filtered = features_df.copy()
    if filters is None:
        filters = {}
    if filters.get("active_only", True) and "is_active_bool" in filtered.columns:
        filtered = filtered.loc[filtered["is_active_bool"]].copy()
    slot = _clean_text(filters.get("slot"))
    if slot:
        filtered = filtered.loc[
            filtered["allowed_slots"].map(lambda values: slot in _as_slot_set(values))
        ].copy()
    if filters.get("same_slot", True):
        source_slots = _as_slot_set(source_recipe.get("allowed_slots"))
        if source_slots:
            filtered = filtered.loc[
                filtered["allowed_slots"].map(lambda values: bool(source_slots & _as_slot_set(values)))
            ].copy()
    return filtered


def _similarity_score_parts(
    source_recipe: Any,
    candidate_recipe: Any,
    numeric_distance: float,
    config: dict[str, Any],
) -> dict[str, Any]:
    numeric_similarity = max(0.0, 1.0 - min(numeric_distance, 1.0))
    source_slots = _as_slot_set(source_recipe.get("allowed_slots"))
    candidate_slots = _as_slot_set(candidate_recipe.get("allowed_slots"))
    slot_match = bool(source_slots and candidate_slots and source_slots & candidate_slots)
    category_match = _same_text(source_recipe, candidate_recipe, "recipe_category")
    family_match = _same_text(source_recipe, candidate_recipe, "recipe_family_name")
    kind_match = _same_text(source_recipe, candidate_recipe, "recipe_kind")
    main_protein_match = _same_text(source_recipe, candidate_recipe, "main_protein_family")

    score = float(config["numeric_weight"]) * numeric_similarity
    if slot_match:
        score += float(config["slot_bonus"])
    if kind_match:
        score += float(config["kind_bonus"])
    if category_match:
        score += float(config["category_bonus"])
    if family_match:
        score += float(config["family_bonus"])
    if main_protein_match and _clean_text(source_recipe.get("main_protein_family")):
        score += float(config["main_protein_bonus"])

    warnings: list[str] = []
    if source_slots and candidate_slots and not slot_match:
        score -= float(config["incompatible_slot_penalty"])
        warnings.append("slot_incompatible")
    if _macro_gap_too_large(source_recipe, candidate_recipe):
        score -= float(config["macro_mismatch_penalty"])
        warnings.append("macro_gap_large")
    if not bool(candidate_recipe.get("has_complete_nutrition", False)):
        score -= float(config["missing_nutrition_penalty"])
        warnings.append("candidate_missing_nutrition")
    if not bool(candidate_recipe.get("has_usable_time", False)):
        score -= float(config["missing_time_penalty"])
        warnings.append("candidate_missing_time")

    return {
        "similarity_score": max(0.0, min(1.0, score)),
        "slot_match": slot_match,
        "category_match": category_match,
        "family_match": family_match,
        "kind_match": kind_match,
        "main_protein_match": main_protein_match,
        "warnings": warnings,
    }


def _similarity_warnings(
    source_recipe: Any,
    candidate_recipe: Any,
    score_parts: dict[str, Any],
) -> list[str]:
    warnings = list(score_parts.get("warnings", []))
    for prefix, recipe in (("source", source_recipe), ("candidate", candidate_recipe)):
        reason = _clean_text(recipe.get("similarity_low_confidence"))
        if reason:
            warnings.append(f"{prefix}_{reason}")
    return _dedupe(warnings)


def _numeric_distance(source_vector: Any, candidate_vector: Any) -> float:
    squared_sum = 0.0
    count = 0
    for column in NUMERIC_FEATURE_COLUMNS:
        source_value = _to_float(source_vector.get(column))
        candidate_value = _to_float(candidate_vector.get(column))
        if source_value is None or candidate_value is None:
            continue
        squared_sum += (source_value - candidate_value) ** 2
        count += 1
    if count == 0:
        return 1.0
    return min(math.sqrt(squared_sum / count), 1.0)


def _macro_gap_too_large(source_recipe: Any, candidate_recipe: Any) -> bool:
    source_kcal = _to_float(source_recipe.get("kcal_per_serving"))
    candidate_kcal = _to_float(candidate_recipe.get("kcal_per_serving"))
    source_protein = _to_float(source_recipe.get("protein_g_per_serving"))
    candidate_protein = _to_float(candidate_recipe.get("protein_g_per_serving"))
    kcal_gap = (
        abs(candidate_kcal - source_kcal) / max(source_kcal, 1.0)
        if source_kcal is not None and candidate_kcal is not None
        else 0.0
    )
    protein_gap = (
        abs(candidate_protein - source_protein) / max(source_protein, 1.0)
        if source_protein is not None and candidate_protein is not None
        else 0.0
    )
    return kcal_gap > 0.55 or protein_gap > 0.70


def _low_confidence_reason(row: Any) -> str:
    reasons = []
    if not bool(row.get("has_complete_nutrition", False)):
        reasons.append("nutrition_incomplete")
    if not bool(row.get("has_usable_time", False)):
        reasons.append("time_missing")
    if not bool(row.get("has_ingredient_similarity_signal", False)):
        reasons.append("ingredient_signal_missing")
    return ",".join(reasons)


def _nutrition_confidence(cache_status: Any) -> str:
    text = _clean_text(cache_status)
    if text == "usable_from_mapped_ingredients":
        return "high"
    if text == "partial_from_mapped_ingredients":
        return "medium"
    return "low" if text else "missing"


def _parse_allowed_slots(value: Any) -> set[str]:
    text = _clean_text(value)
    if not text:
        return set()
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = [part.strip() for part in text.split(",") if part.strip()]
    if isinstance(parsed, list):
        return {_normalize_text(item) for item in parsed if _normalize_text(item)}
    return set()


def _as_slot_set(value: Any) -> set[str]:
    if isinstance(value, set):
        return {_normalize_text(item) for item in value if _normalize_text(item)}
    if isinstance(value, list):
        return {_normalize_text(item) for item in value if _normalize_text(item)}
    if isinstance(value, tuple):
        return {_normalize_text(item) for item in value if _normalize_text(item)}
    return _parse_allowed_slots(value)


def _delta(candidate_recipe: Any, source_recipe: Any, column: str) -> float | None:
    candidate_value = _to_float(candidate_recipe.get(column))
    source_value = _to_float(source_recipe.get(column))
    if candidate_value is None or source_value is None:
        return None
    return round(candidate_value - source_value, 4)


def _same_text(left: Any, right: Any, key: str) -> bool:
    left_text = _normalize_text(_get_value(left, key))
    right_text = _normalize_text(_get_value(right, key))
    return bool(left_text and right_text and left_text == right_text)


def _get_value(row: Any, key: str) -> Any:
    if hasattr(row, "get"):
        return row.get(key)
    return getattr(row, key, None)


def _config(config: dict[str, Any] | None) -> dict[str, Any]:
    result = dict(DEFAULT_CONFIG)
    if config:
        result.update(config)
    return result


def _numeric_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series([float("nan")] * len(df), index=df.index)
    return pd.to_numeric(df[column], errors="coerce")


def _require_columns(df: pd.DataFrame, required_columns: set[str], source_name: str) -> None:
    missing = sorted(required_columns - set(df.columns))
    if missing:
        raise ValueError(f"Lipsesc coloane in {source_name}: {', '.join(missing)}")


def _contains_word(text: str, word: str) -> bool:
    normalized = _normalize_text(word)
    if not normalized:
        return False
    return re.search(rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])", text) is not None


def _normalize_text(value: Any) -> str:
    text = str(value or "").lower().replace("_", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null", "nat"}:
        return ""
    return text


def _to_float(value: Any) -> float | None:
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(numeric_value) or math.isinf(numeric_value):
        return None
    return numeric_value


def _dedupe(values: list[str]) -> list[str]:
    result = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result
