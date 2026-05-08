from __future__ import annotations

import csv
import json
import math
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

BASE_DIR = REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_generator_ready_slot_checked_time_enriched"
BASE_RECIPES = BASE_DIR / "recipes.csv"
BASE_INGREDIENTS = BASE_DIR / "recipe_ingredients.csv"
BASE_NUTRITION = BASE_DIR / "recipe_nutrition_cache.csv"
SNACK_READINESS = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_snack_readiness_audit.csv"

MANUAL_DIR = REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_manual_snacks"
MANUAL_RECIPES = MANUAL_DIR / "recipes.csv"
MANUAL_INGREDIENTS = MANUAL_DIR / "recipe_ingredients.csv"
MANUAL_NUTRITION = MANUAL_DIR / "recipe_nutrition_cache.csv"

OUT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_generator_ready_slot_checked_time_enriched_snack_curated"
OUT_RECIPES = OUT_DIR / "recipes.csv"
OUT_INGREDIENTS = OUT_DIR / "recipe_ingredients.csv"
OUT_NUTRITION = OUT_DIR / "recipe_nutrition_cache.csv"
OUT_README = OUT_DIR / "README_v1_1_generator_ready_slot_checked_time_enriched_snack_curated.txt"
OUT_SUMMARY = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_snack_curated_materialization_summary.txt"
OUT_AUDIT = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_snack_curated_materialization_audit.csv"
OUT_DECISIONS = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round12_existing_snack_decisions.csv"
OUT_DECISIONS_SUMMARY = REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round12_existing_snack_decisions_summary.txt"

SNACK_CURATED_SCOPE = "v1_1_generator_ready_slot_checked_time_enriched_snack_curated_draft"


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


def round_number(value: float) -> str:
    rounded = round(value, 4)
    text = f"{rounded:.4f}".rstrip("0").rstrip(".")
    return text or "0"


def union_columns(*column_lists: list[str]) -> list[str]:
    columns: list[str] = []
    for column_list in column_lists:
        for column in column_list:
            if column not in columns:
                columns.append(column)
    return columns


def normalize_rows(rows: list[dict[str, object]], fieldnames: list[str]) -> list[dict[str, object]]:
    normalized: list[dict[str, object]] = []
    for row in rows:
        normalized.append({field: row.get(field, "") for field in fieldnames})
    return normalized


def allowed_slots(row: dict[str, str]) -> list[str]:
    text = clean_text(row.get("allowed_slots_json"))
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, list):
        return []
    return [clean_text(value).lower() for value in parsed if clean_text(value)]


def index_by_recipe_id(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {clean_text(row.get("recipe_id")): row for row in rows if clean_text(row.get("recipe_id"))}


def append_note(existing: object, note: str) -> str:
    text = clean_text(existing)
    if not text:
        return note
    if note in text:
        return text
    return f"{text}; {note}"


def build_existing_snack_decisions(
    base_recipes: list[dict[str, str]],
    snack_readiness_rows: list[dict[str, str]],
) -> tuple[list[dict[str, object]], set[str], set[str]]:
    readiness_by_id = index_by_recipe_id(snack_readiness_rows)
    decisions: list[dict[str, object]] = []
    keep_ids: set[str] = set()
    exclude_ids: set[str] = set()

    for recipe in base_recipes:
        recipe_id = clean_text(recipe.get("recipe_id"))
        if "snack" not in allowed_slots(recipe):
            continue
        readiness = readiness_by_id.get(recipe_id, {})
        status = clean_text(readiness.get("snack_readiness_status")) or "missing_snack_audit"
        if status == "snack_ready":
            decision = "keep_existing_snack"
            reason = "snack_ready_in_round11_audit"
            keep_ids.add(recipe_id)
        else:
            decision = "exclude_existing_snack_from_round12"
            reason = f"round11_status:{status}"
            exclude_ids.add(recipe_id)
        decisions.append(
            {
                "recipe_id": recipe_id,
                "display_name": clean_text(recipe.get("display_name")),
                "snack_readiness_status": status,
                "snack_reason": clean_text(readiness.get("snack_reason")),
                "decision": decision,
                "decision_reason": reason,
                "allowed_slots_json": clean_text(recipe.get("allowed_slots_json")),
                "kcal_per_serving": clean_text(readiness.get("kcal_per_serving")),
                "protein_g_per_serving": clean_text(readiness.get("protein_g_per_serving")),
                "effective_time_min_for_scoring": clean_text(readiness.get("effective_time_min_for_scoring")),
            }
        )
    return decisions, keep_ids, exclude_ids


def materialize_rows(
    base_recipes: list[dict[str, str]],
    base_ingredients: list[dict[str, str]],
    base_nutrition: list[dict[str, str]],
    manual_recipes: list[dict[str, str]],
    manual_ingredients: list[dict[str, str]],
    manual_nutrition: list[dict[str, str]],
    old_snack_keep_ids: set[str],
    old_snack_exclude_ids: set[str],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    kept_base_ids: set[str] = set()
    audit: list[dict[str, object]] = []
    output_recipes: list[dict[str, object]] = []

    for recipe in base_recipes:
        recipe_id = clean_text(recipe.get("recipe_id"))
        is_old_snack_candidate = recipe_id in old_snack_keep_ids or recipe_id in old_snack_exclude_ids
        if recipe_id in old_snack_exclude_ids:
            audit.append(audit_row(recipe, "existing_snack_excluded", False, "snack_review_or_exclude_round11"))
            continue
        kept_base_ids.add(recipe_id)
        updated = dict(recipe)
        updated["scope_status"] = SNACK_CURATED_SCOPE
        updated["qc_notes"] = append_note(updated.get("qc_notes"), "snack_curated_round12_base_kept")
        source_label = "existing_snack_kept" if is_old_snack_candidate else "base_non_snack_kept"
        output_recipes.append(updated)
        audit.append(audit_row(updated, source_label, True, "kept_from_time_enriched_base"))

    existing_ids = {clean_text(row.get("recipe_id")) for row in output_recipes}
    manual_ids: set[str] = set()
    for recipe in manual_recipes:
        recipe_id = clean_text(recipe.get("recipe_id"))
        if recipe_id in existing_ids or recipe_id in manual_ids:
            raise ValueError(f"recipe_id duplicat in materializarea round12: {recipe_id}")
        manual_ids.add(recipe_id)
        updated = dict(recipe)
        updated["scope_status"] = SNACK_CURATED_SCOPE
        updated["qc_notes"] = append_note(updated.get("qc_notes"), "snack_curated_round12_manual_added")
        output_recipes.append(updated)
        audit.append(audit_row(updated, "manual_snack_added", True, "manual_curated_round12"))

    output_ids = kept_base_ids | manual_ids
    output_ingredients = [dict(row) for row in base_ingredients if clean_text(row.get("recipe_id")) in kept_base_ids]
    output_ingredients.extend(dict(row) for row in manual_ingredients if clean_text(row.get("recipe_id")) in manual_ids)
    output_nutrition = [dict(row) for row in base_nutrition if clean_text(row.get("recipe_id")) in kept_base_ids]
    output_nutrition.extend(dict(row) for row in manual_nutrition if clean_text(row.get("recipe_id")) in manual_ids)

    missing_nutrition = output_ids - {clean_text(row.get("recipe_id")) for row in output_nutrition}
    if missing_nutrition:
        raise ValueError(f"Retete fara nutrition cache in output: {sorted(missing_nutrition)[:5]}")

    return output_recipes, output_ingredients, output_nutrition, audit


def audit_row(recipe: dict[str, object], source: str, included: bool, reason: str) -> dict[str, object]:
    return {
        "recipe_id": clean_text(recipe.get("recipe_id")),
        "display_name": clean_text(recipe.get("display_name")),
        "round12_source": source,
        "included_in_snack_curated": str(included),
        "decision_reason": reason,
        "recipe_kind": clean_text(recipe.get("recipe_kind")),
        "allowed_slots_json": clean_text(recipe.get("allowed_slots_json")),
        "scope_status": clean_text(recipe.get("scope_status")),
        "qc_notes": clean_text(recipe.get("qc_notes")),
    }


def numeric_values(rows: list[dict[str, object]], column: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = parse_float(row.get(column))
        if value is not None:
            values.append(value)
    return values


def median(values: list[float]) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    middle = len(sorted_values) // 2
    if len(sorted_values) % 2:
        return sorted_values[middle]
    return (sorted_values[middle - 1] + sorted_values[middle]) / 2


def build_decision_summary(decisions: list[dict[str, object]]) -> str:
    status_counts = Counter(clean_text(row.get("snack_readiness_status")) for row in decisions)
    decision_counts = Counter(clean_text(row.get("decision")) for row in decisions)
    lines = [
        "Recipes_DB v1.1 round12 existing snack decisions",
        "=" * 50,
        "",
        f"existing_snack_candidates: {len(decisions)}",
        "",
        "Readiness counts:",
    ]
    lines.extend(counter_lines(status_counts))
    lines.extend(["", "Decision counts:"])
    lines.extend(counter_lines(decision_counts))
    lines.extend(["", "Decisions:"])
    for row in decisions:
        lines.append(
            "- "
            + clean_text(row.get("recipe_id"))
            + " | "
            + clean_text(row.get("display_name"))
            + " | "
            + clean_text(row.get("decision"))
            + " | "
            + clean_text(row.get("decision_reason"))
        )
    return "\n".join(lines) + "\n"


def counter_lines(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key}: {value}" for key, value in counter.most_common()]


def build_summary(
    output_recipes: list[dict[str, object]],
    output_ingredients: list[dict[str, object]],
    output_nutrition: list[dict[str, object]],
    decisions: list[dict[str, object]],
    manual_recipes: list[dict[str, str]],
    audit: list[dict[str, object]],
) -> str:
    recipe_kind_counts = Counter(clean_text(row.get("recipe_kind")) for row in output_recipes)
    cache_status_counts = Counter(clean_text(row.get("cache_status")) for row in output_nutrition)
    audit_counts = Counter(clean_text(row.get("round12_source")) for row in audit)
    slot_counts: Counter[str] = Counter()
    for row in output_recipes:
        for slot in allowed_slots({key: clean_text(value) for key, value in row.items()}):
            slot_counts[slot] += 1
    lines = [
        "Recipes_DB v1.1 round12 snack-curated materialization",
        "=" * 56,
        "",
        f"output_recipe_count: {len(output_recipes)}",
        f"output_ingredient_rows: {len(output_ingredients)}",
        f"output_nutrition_rows: {len(output_nutrition)}",
        f"manual_snacks_added: {len(manual_recipes)}",
        f"existing_snack_decisions: {len(decisions)}",
        "",
        "Audit source counts:",
    ]
    lines.extend(counter_lines(audit_counts))
    lines.extend(["", "Recipe kind counts:"])
    lines.extend(counter_lines(recipe_kind_counts))
    lines.extend(["", "Allowed slot counts by recipe:"])
    lines.extend(counter_lines(slot_counts))
    lines.extend(["", "Cache status counts:"])
    lines.extend(counter_lines(cache_status_counts))
    lines.extend(
        [
            "",
            "Median per-serving macros:",
            f"- kcal: {round_number(median(numeric_values(output_nutrition, 'energy_kcal_per_serving')))}",
            f"- protein_g: {round_number(median(numeric_values(output_nutrition, 'protein_g_per_serving')))}",
            f"- carbs_g: {round_number(median(numeric_values(output_nutrition, 'carbs_g_per_serving')))}",
            f"- fat_g: {round_number(median(numeric_values(output_nutrition, 'fat_g_per_serving')))}",
            "",
            "Notes:",
            "- starts from v1_1_generator_ready_slot_checked_time_enriched draft.",
            "- old snack_review/snack_exclude candidates are excluded from the new folder.",
            "- manual snacks are added from Food_DB v1.1 round9 exact matches only.",
            "- current Recipes_DB and Food_DB are unchanged.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_readme(output_count: int, manual_count: int, excluded_old_snacks: int) -> str:
    return "\n".join(
        [
            "Recipes_DB v1.1 slot-checked time-enriched snack-curated draft",
            "=" * 67,
            "",
            "This is draft/test materialization only for Generator v1 testing.",
            "It does not replace data/recipesdb/current and it is not full v1.1.",
            "",
            f"Output recipes: {output_count}",
            f"Manual snacks added: {manual_count}",
            f"Existing weak snack candidates excluded: {excluded_old_snacks}",
            "",
            "The dataset keeps the round11 slot/time checked recipes, removes weak old snack candidates,",
            "and supplements the snack slot with simple manual curated snacks.",
            "",
        ]
    )


def main() -> None:
    base_recipes, base_recipe_columns = read_csv(BASE_RECIPES)
    base_ingredients, base_ingredient_columns = read_csv(BASE_INGREDIENTS)
    base_nutrition, base_nutrition_columns = read_csv(BASE_NUTRITION)
    snack_readiness_rows, _ = read_csv(SNACK_READINESS)
    manual_recipes, manual_recipe_columns = read_csv(MANUAL_RECIPES)
    manual_ingredients, manual_ingredient_columns = read_csv(MANUAL_INGREDIENTS)
    manual_nutrition, manual_nutrition_columns = read_csv(MANUAL_NUTRITION)

    decisions, old_snack_keep_ids, old_snack_exclude_ids = build_existing_snack_decisions(
        base_recipes,
        snack_readiness_rows,
    )
    output_recipes, output_ingredients, output_nutrition, audit = materialize_rows(
        base_recipes,
        base_ingredients,
        base_nutrition,
        manual_recipes,
        manual_ingredients,
        manual_nutrition,
        old_snack_keep_ids,
        old_snack_exclude_ids,
    )

    recipe_columns = union_columns(base_recipe_columns, manual_recipe_columns)
    ingredient_columns = union_columns(base_ingredient_columns, manual_ingredient_columns)
    nutrition_columns = union_columns(base_nutrition_columns, manual_nutrition_columns)

    write_csv(OUT_RECIPES, normalize_rows(output_recipes, recipe_columns), recipe_columns)
    write_csv(OUT_INGREDIENTS, normalize_rows(output_ingredients, ingredient_columns), ingredient_columns)
    write_csv(OUT_NUTRITION, normalize_rows(output_nutrition, nutrition_columns), nutrition_columns)
    write_csv(
        OUT_DECISIONS,
        decisions,
        [
            "recipe_id",
            "display_name",
            "snack_readiness_status",
            "snack_reason",
            "decision",
            "decision_reason",
            "allowed_slots_json",
            "kcal_per_serving",
            "protein_g_per_serving",
            "effective_time_min_for_scoring",
        ],
    )
    write_csv(
        OUT_AUDIT,
        audit,
        [
            "recipe_id",
            "display_name",
            "round12_source",
            "included_in_snack_curated",
            "decision_reason",
            "recipe_kind",
            "allowed_slots_json",
            "scope_status",
            "qc_notes",
        ],
    )
    OUT_README.write_text(
        build_readme(len(output_recipes), len(manual_recipes), len(old_snack_exclude_ids)),
        encoding="utf-8",
    )
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(
        build_summary(output_recipes, output_ingredients, output_nutrition, decisions, manual_recipes, audit),
        encoding="utf-8",
    )
    OUT_DECISIONS_SUMMARY.write_text(build_decision_summary(decisions), encoding="utf-8")

    print("Recipes_DB v1.1 snack-curated subset written")
    print(f"output_recipe_count={len(output_recipes)}")
    print(f"manual_snacks_added={len(manual_recipes)}")
    print(f"existing_snacks_excluded={len(old_snack_exclude_ids)}")
    print(f"written_recipes={OUT_RECIPES}")
    print(f"written_summary={OUT_SUMMARY}")


if __name__ == "__main__":
    main()
