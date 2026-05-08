from __future__ import annotations

import csv
import json
import math
import re
import shutil
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

IN_DIR = REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_generator_ready_slot_checked"
IN_RECIPES = IN_DIR / "recipes.csv"
IN_INGREDIENTS = IN_DIR / "recipe_ingredients.csv"
IN_NUTRITION = IN_DIR / "recipe_nutrition_cache.csv"

OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_generator_ready_slot_checked_time_enriched"
OUT_RECIPES = OUT_DIR / "recipes.csv"
OUT_INGREDIENTS = OUT_DIR / "recipe_ingredients.csv"
OUT_NUTRITION = OUT_DIR / "recipe_nutrition_cache.csv"
OUT_README = OUT_DIR / "README_v1_1_generator_ready_slot_checked_time_enriched.txt"

OUT_AUDIT = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_slot_checked_time_enrichment_audit.csv"
OUT_SUMMARY = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_slot_checked_time_enrichment_summary.txt"

TIME_ENRICHED_SCOPE_STATUS = "v1_1_generator_ready_slot_checked_time_enriched_draft"
TIME_ENRICHED_SOURCE_DATASET = "recipes_dataset_64k_dishes_v1_1_generator_ready_slot_checked_time_enriched_draft"

EXTRA_RECIPE_COLUMNS = [
    "active_time_estimated_min",
    "passive_time_estimated_min",
    "effective_time_min_for_scoring",
    "has_long_passive_time",
    "time_estimation_confidence",
    "time_estimation_method",
    "time_estimation_reasons",
]

AUDIT_COLUMNS = [
    "recipe_id",
    "display_name",
    "recipe_kind",
    "ingredient_count",
    "directions_step_count",
    "prep_time_min",
    "cook_time_min",
    "total_time_min",
    "active_time_estimated_min",
    "passive_time_estimated_min",
    "effective_time_min_for_scoring",
    "has_long_passive_time",
    "time_estimation_confidence",
    "time_estimation_method",
    "time_estimation_reasons",
    "passive_keywords_detected",
    "cooking_keywords_detected",
]

PASSIVE_KEYWORDS = [
    "marinate",
    "marinade",
    "chill",
    "refrigerate",
    "refrigerator",
    "overnight",
    "rest",
    "let stand",
    "freeze",
    "slow cooker",
    "crockpot",
]
COOKING_KEYWORDS = [
    "bake",
    "roast",
    "grill",
    "boil",
    "simmer",
    "fry",
    "saute",
    "air fryer",
    "pressure cook",
    "stew",
    "slow cook",
]
KIND_DEFAULTS = {
    "breakfast": (10.0, 15.0),
    "snack": (8.0, 5.0),
    "salad": (20.0, 5.0),
    "soup": (15.0, 45.0),
    "complete_main": (20.0, 40.0),
    "near_complete_main": (18.0, 35.0),
    "protein_component": (10.0, 20.0),
    "carb_side": (10.0, 20.0),
    "veg_side": (10.0, 15.0),
    "component": (10.0, 15.0),
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


def format_number(value: float | None, digits: int = 1) -> str:
    if value is None:
        return ""
    return f"{value:.{digits}f}".rstrip("0").rstrip(".")


def normalize_text(value: object) -> str:
    text = clean_text(value).lower().replace("_", " ")
    text = re.sub(r"[^a-z0-9./]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def keyword_hits(text: str, keywords: list[str]) -> list[str]:
    hits: list[str] = []
    for keyword in keywords:
        normalized = normalize_text(keyword)
        pattern = rf"(?<![a-z0-9]){re.escape(normalized)}(?![a-z0-9])"
        if re.search(pattern, text):
            hits.append(keyword)
    return hits


def recipe_steps(row: dict[str, str]) -> list[str]:
    directions = clean_text(row.get("directions_json"))
    if not directions:
        return []
    try:
        parsed = json.loads(directions)
    except json.JSONDecodeError:
        return [directions]
    if isinstance(parsed, list):
        return [clean_text(step) for step in parsed if clean_text(step)]
    return [directions]


def estimate_time(row: dict[str, str], ingredient_count: int) -> dict[str, object]:
    existing_prep = parse_float(row.get("prep_time_min"))
    existing_cook = parse_float(row.get("cook_time_min"))
    existing_total = parse_float(row.get("total_time_min"))
    steps = recipe_steps(row)
    step_count = int(parse_float(row.get("directions_step_count")) or len(steps) or 0)
    recipe_kind = clean_text(row.get("recipe_kind"))
    reasons: list[str] = []

    if existing_prep is not None and existing_cook is not None and existing_total is not None:
        passive_time = max(existing_total - existing_prep - existing_cook, 0.0)
        reasons.append("existing_time_fields_preserved")
        return _time_result(
            prep=existing_prep,
            cook=existing_cook,
            passive=passive_time,
            method="existing_recipe_time_fields",
            confidence="high",
            reasons=reasons,
            passive_hits=keyword_hits(_row_text(row, steps), PASSIVE_KEYWORDS),
            cooking_hits=keyword_hits(_row_text(row, steps), COOKING_KEYWORDS),
        )

    text = _row_text(row, steps)
    passive_hits = keyword_hits(text, PASSIVE_KEYWORDS)
    cooking_hits = keyword_hits(text, COOKING_KEYWORDS)
    base_prep, base_cook = KIND_DEFAULTS.get(recipe_kind, (15.0, 25.0))
    prep = _estimate_prep_time(base_prep, ingredient_count, step_count)
    cook = _estimate_cook_time(base_cook, steps, cooking_hits, recipe_kind)
    passive = _estimate_passive_time(steps, passive_hits)

    if existing_prep is not None:
        prep = existing_prep
        reasons.append("existing_prep_preserved")
    if existing_cook is not None:
        cook = existing_cook
        reasons.append("existing_cook_preserved")
    if existing_total is not None:
        total_without_passive = prep + cook
        passive = max(existing_total - total_without_passive, passive)
        reasons.append("existing_total_used_as_lower_bound")

    if passive_hits:
        reasons.append("passive_keywords_detected")
    if cooking_hits:
        reasons.append("cooking_keywords_detected")
    if not steps:
        reasons.append("directions_missing")
    if not _step_time_values(steps):
        reasons.append("no_explicit_step_times")

    confidence = _confidence(steps, cooking_hits, passive_hits, ingredient_count)
    method = "round11_direction_keyword_time_estimate"
    return _time_result(
        prep=prep,
        cook=cook,
        passive=passive,
        method=method,
        confidence=confidence,
        reasons=reasons,
        passive_hits=passive_hits,
        cooking_hits=cooking_hits,
    )


def _time_result(
    prep: float,
    cook: float,
    passive: float,
    method: str,
    confidence: str,
    reasons: list[str],
    passive_hits: list[str],
    cooking_hits: list[str],
) -> dict[str, object]:
    active = round(max(prep + cook, 0.0), 1)
    passive = round(max(passive, 0.0), 1)
    total = round(active + passive, 1)
    if passive > 0:
        reasons.append("passive_time_added")
    if passive >= 180:
        reasons.append("long_passive_time_detected")
    return {
        "prep_time_min": round(max(prep, 0.0), 1),
        "cook_time_min": round(max(cook, 0.0), 1),
        "total_time_min": total,
        "active_time_estimated_min": active,
        "passive_time_estimated_min": passive,
        "effective_time_min_for_scoring": active,
        "has_long_passive_time": passive >= 180,
        "time_estimation_confidence": confidence,
        "time_estimation_method": method,
        "time_estimation_reasons": sorted(set(reasons)),
        "passive_keywords_detected": passive_hits,
        "cooking_keywords_detected": cooking_hits,
    }


def _estimate_prep_time(base_prep: float, ingredient_count: int, step_count: int) -> float:
    ingredient_extra = max(ingredient_count - 5, 0) * 1.5
    step_extra = max(step_count - 4, 0) * 1.0
    return round(min(max(base_prep + ingredient_extra + step_extra, 5.0), 35.0), 1)


def _estimate_cook_time(
    base_cook: float,
    steps: list[str],
    cooking_hits: list[str],
    recipe_kind: str,
) -> float:
    timed_steps = []
    for step in steps:
        normalized = normalize_text(step)
        if keyword_hits(normalized, COOKING_KEYWORDS):
            timed_steps.extend(_step_time_values([step]))
    explicit_cook = sum(value for value in timed_steps if value <= 180)
    if explicit_cook > 0:
        return round(min(max(explicit_cook, base_cook * 0.75), 120.0), 1)
    if "slow cooker" in cooking_hits or "slow cook" in cooking_hits:
        return 25.0
    if recipe_kind == "snack" and not cooking_hits:
        return 5.0
    if recipe_kind == "salad" and not cooking_hits:
        return 5.0
    return round(base_cook, 1)


def _estimate_passive_time(steps: list[str], passive_hits: list[str]) -> float:
    passive = 0.0
    for step in steps:
        normalized = normalize_text(step)
        if not keyword_hits(normalized, PASSIVE_KEYWORDS):
            continue
        values = _step_time_values([step])
        if values:
            passive += max(values)
    text = normalize_text(" ".join(steps))
    if "overnight" in passive_hits:
        passive = max(passive, 720.0)
    if "few hours" in text:
        passive = max(passive, 180.0)
    if "slow cooker" in passive_hits or "crockpot" in passive_hits or "slow cook" in text:
        slow_values = [value for value in _step_time_values(steps) if value >= 120]
        passive = max(passive, max(slow_values) if slow_values else 240.0)
    elif passive_hits and passive == 0:
        passive = 30.0
    return round(passive, 1)


def _step_time_values(steps: list[str]) -> list[float]:
    values: list[float] = []
    for step in steps:
        text = normalize_text(step)
        values.extend(_range_times(text))
        values.extend(_simple_times(text))
    return values


def _range_times(text: str) -> list[float]:
    values: list[float] = []
    pattern = re.compile(r"(\d+(?:\.\d+)?)\s*(?:to|-)\s*(\d+(?:\.\d+)?)\s*(minutes?|hours?|hrs?)")
    for match in pattern.finditer(text):
        high = float(match.group(2))
        unit = match.group(3)
        values.append(_to_minutes(high, unit))
    return values


def _simple_times(text: str) -> list[float]:
    values: list[float] = []
    pattern = re.compile(r"(?<!to\s)(?<!-\s)(\d+(?:\.\d+)?)\s*(minutes?|hours?|hrs?)")
    for match in pattern.finditer(text):
        amount = float(match.group(1))
        unit = match.group(2)
        values.append(_to_minutes(amount, unit))
    return values


def _to_minutes(amount: float, unit: str) -> float:
    if unit.startswith("hour") or unit.startswith("hr"):
        return amount * 60.0
    return amount


def _confidence(
    steps: list[str],
    cooking_hits: list[str],
    passive_hits: list[str],
    ingredient_count: int,
) -> str:
    explicit_times = _step_time_values(steps)
    if explicit_times and cooking_hits:
        return "high"
    if steps and (cooking_hits or passive_hits or ingredient_count >= 3):
        return "medium"
    return "low"


def _row_text(row: dict[str, str], steps: list[str]) -> str:
    return normalize_text(
        " ".join(
            [
                clean_text(row.get("display_name")),
                clean_text(row.get("recipe_kind")),
                clean_text(row.get("recipe_category")),
                clean_text(row.get("recipe_subcategory")),
                " ".join(steps),
            ]
        )
    )


def ingredient_counts(rows: list[dict[str, str]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        recipe_id = clean_text(row.get("recipe_id"))
        if recipe_id:
            counts[recipe_id] += 1
    return counts


def append_qc_notes(existing: str, estimate: dict[str, object]) -> str:
    notes = [part.strip() for part in clean_text(existing).split(";") if part.strip()]
    notes.append("time_estimated_round11_v1_1")
    if float(estimate["passive_time_estimated_min"]) > 0:
        notes.append("passive_time_detected")
    if clean_text(estimate["time_estimation_confidence"]) == "low":
        notes.append("time_low_confidence")
    return "; ".join(dict.fromkeys(notes))


def build_rows(
    recipes: list[dict[str, str]],
    recipe_fieldnames: list[str],
    ingredient_count_by_recipe: Counter[str],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[str]]:
    output_rows: list[dict[str, object]] = []
    audit_rows: list[dict[str, object]] = []
    fieldnames = list(recipe_fieldnames)
    for column in EXTRA_RECIPE_COLUMNS:
        if column not in fieldnames:
            fieldnames.append(column)

    for row in recipes:
        recipe_id = clean_text(row.get("recipe_id"))
        ingredient_count = ingredient_count_by_recipe.get(recipe_id, 0)
        estimate = estimate_time(row, ingredient_count)
        enriched = dict(row)
        enriched["source_dataset"] = TIME_ENRICHED_SOURCE_DATASET
        enriched["scope_status"] = TIME_ENRICHED_SCOPE_STATUS
        enriched["prep_time_min"] = format_number(float(estimate["prep_time_min"]))
        enriched["cook_time_min"] = format_number(float(estimate["cook_time_min"]))
        enriched["total_time_min"] = format_number(float(estimate["total_time_min"]))
        enriched["active_time_estimated_min"] = format_number(float(estimate["active_time_estimated_min"]))
        enriched["passive_time_estimated_min"] = format_number(float(estimate["passive_time_estimated_min"]))
        enriched["effective_time_min_for_scoring"] = format_number(
            float(estimate["effective_time_min_for_scoring"])
        )
        enriched["has_long_passive_time"] = str(bool(estimate["has_long_passive_time"]))
        enriched["time_estimation_confidence"] = clean_text(estimate["time_estimation_confidence"])
        enriched["time_estimation_method"] = clean_text(estimate["time_estimation_method"])
        enriched["time_estimation_reasons"] = "|".join(estimate["time_estimation_reasons"])
        enriched["qc_notes"] = append_qc_notes(clean_text(row.get("qc_notes")), estimate)
        output_rows.append(enriched)
        audit_rows.append(
            {
                "recipe_id": recipe_id,
                "display_name": clean_text(row.get("display_name")),
                "recipe_kind": clean_text(row.get("recipe_kind")),
                "ingredient_count": ingredient_count,
                "directions_step_count": clean_text(row.get("directions_step_count")),
                "prep_time_min": enriched["prep_time_min"],
                "cook_time_min": enriched["cook_time_min"],
                "total_time_min": enriched["total_time_min"],
                "active_time_estimated_min": enriched["active_time_estimated_min"],
                "passive_time_estimated_min": enriched["passive_time_estimated_min"],
                "effective_time_min_for_scoring": enriched["effective_time_min_for_scoring"],
                "has_long_passive_time": enriched["has_long_passive_time"],
                "time_estimation_confidence": enriched["time_estimation_confidence"],
                "time_estimation_method": enriched["time_estimation_method"],
                "time_estimation_reasons": enriched["time_estimation_reasons"],
                "passive_keywords_detected": "|".join(estimate["passive_keywords_detected"]),
                "cooking_keywords_detected": "|".join(estimate["cooking_keywords_detected"]),
            }
        )
    return output_rows, audit_rows, fieldnames


def counter_text(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key}: {value}" for key, value in counter.most_common()]


def numeric_values(rows: list[dict[str, object]], column: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        parsed = parse_float(row.get(column))
        if parsed is not None:
            values.append(parsed)
    return values


def median(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    middle = len(values) // 2
    if len(values) % 2:
        return values[middle]
    return (values[middle - 1] + values[middle]) / 2


def build_summary(audit_rows: list[dict[str, object]]) -> str:
    confidence_counts = Counter(clean_text(row.get("time_estimation_confidence")) for row in audit_rows)
    passive_count = sum(1 for row in audit_rows if parse_float(row.get("passive_time_estimated_min")) not in {None, 0.0})
    long_passive_count = sum(1 for row in audit_rows if clean_text(row.get("has_long_passive_time")) == "True")
    kind_counts = Counter(clean_text(row.get("recipe_kind")) for row in audit_rows)
    lines = [
        "Recipes_DB v1.1 slot-checked time enrichment summary",
        "=" * 58,
        "",
        f"recipes_enriched: {len(audit_rows)}",
        "",
        "Time confidence counts:",
    ]
    lines.extend(counter_text(confidence_counts))
    lines.extend(
        [
            "",
            f"recipes_with_passive_time: {passive_count}",
            f"recipes_with_long_passive_time: {long_passive_count}",
            "",
            "Recipe kind counts:",
        ]
    )
    lines.extend(counter_text(kind_counts))
    lines.extend(
        [
            "",
            "Median time estimates:",
            f"- total_time_min: {format_number(median(numeric_values(audit_rows, 'total_time_min')))}",
            f"- effective_time_min_for_scoring: {format_number(median(numeric_values(audit_rows, 'effective_time_min_for_scoring')))}",
            f"- passive_time_estimated_min: {format_number(median(numeric_values(audit_rows, 'passive_time_estimated_min')))}",
            "",
            "Output folder:",
            f"- {OUT_DIR}",
            "",
            "Caveat:",
            "- round11 time values are heuristic draft estimates from directions/title/kind, not production truth.",
        ]
    )
    return "\n".join(lines) + "\n"


def write_readme(recipe_count: int) -> None:
    text = f"""Recipes_DB v1.1 generator-ready slot-checked time-enriched draft
================================================================

This folder is a draft/test materialization only. It does not replace data/recipesdb/current and it is not full v1.1 materialization.

Contents:
- recipes.csv
- recipe_ingredients.csv
- recipe_nutrition_cache.csv

Included recipes: {recipe_count}

Round11 changes:
- recipes.csv has heuristic time estimates for prep_time_min, cook_time_min and total_time_min.
- extra diagnostic columns are included for active/passive/effective time and confidence.
- recipe_ingredients.csv is copied unchanged from the slot-checked draft.
- recipe_nutrition_cache.csv is copied unchanged from the slot-checked draft.

Use:
python -m src.generator_v1_cli --profile profiles/member_profile_demo_v1.json --dataset_profile v1_1_generator_ready_slot_checked_time_enriched

Caveat:
These are conservative draft estimates meant for Generator v1 testing, not production time metadata.
"""
    OUT_README.parent.mkdir(parents=True, exist_ok=True)
    OUT_README.write_text(text, encoding="utf-8")


def main() -> None:
    recipes, recipe_fieldnames = read_csv(IN_RECIPES)
    ingredients, _ = read_csv(IN_INGREDIENTS)
    if not IN_NUTRITION.exists():
        raise FileNotFoundError(f"Lipseste fisierul asteptat: {IN_NUTRITION}")

    counts = ingredient_counts(ingredients)
    enriched_rows, audit_rows, recipe_fieldnames = build_rows(recipes, recipe_fieldnames, counts)

    write_csv(OUT_RECIPES, enriched_rows, recipe_fieldnames)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(IN_INGREDIENTS, OUT_INGREDIENTS)
    shutil.copyfile(IN_NUTRITION, OUT_NUTRITION)
    write_readme(len(enriched_rows))
    write_csv(OUT_AUDIT, audit_rows, AUDIT_COLUMNS)
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(build_summary(audit_rows), encoding="utf-8")

    confidence_counts = Counter(clean_text(row.get("time_estimation_confidence")) for row in audit_rows)
    passive_count = sum(1 for row in audit_rows if parse_float(row.get("passive_time_estimated_min")) not in {None, 0.0})
    print("Recipes_DB v1.1 slot-checked time fields enriched")
    print(f"recipes_enriched={len(enriched_rows)}")
    print(f"time_confidence_counts={dict(confidence_counts)}")
    print(f"recipes_with_passive_time={passive_count}")
    print(f"written_recipes={OUT_RECIPES}")
    print(f"written_audit={OUT_AUDIT}")
    print(f"written_summary={OUT_SUMMARY}")


if __name__ == "__main__":
    main()
