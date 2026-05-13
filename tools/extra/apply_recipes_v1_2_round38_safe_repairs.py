from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
RECIPES_DRAFT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft"
AUDIT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"
FOODDB_PATH = REPO_ROOT / "data" / "fooddb" / "draft" / "fooddb_v1_1_core_master_draft_round9.csv"

REVIEW_PATH = AUDIT_DIR / "recipes_v1_2_round38_repair_review.csv"
ROUND37_UNIT_ROWS_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_round37_plus100_ingredients_unit_rules.csv"
ROUND37_MATCHES_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_round37_plus100_food_matches.csv"

OUT_REPAIRED_INGREDIENTS = RECIPES_DRAFT_DIR / "recipes_v1_2_round38_repaired_ingredients.csv"
OUT_REPAIRED_MATCHES = RECIPES_DRAFT_DIR / "recipes_v1_2_round38_repaired_food_matches.csv"
OUT_REPAIRED_UNMAPPED = RECIPES_DRAFT_DIR / "recipes_v1_2_round38_repaired_unmapped.csv"
OUT_APPLIED = AUDIT_DIR / "recipes_v1_2_round38_repairs_applied.csv"
OUT_DEFERRED = AUDIT_DIR / "recipes_v1_2_round38_repairs_deferred.csv"
OUT_SUMMARY = AUDIT_DIR / "recipes_v1_2_round38_repairs_applied_summary.txt"

APPLIED_COLUMNS = [
    "repair_id",
    "recipe_id_candidate",
    "display_name",
    "blocking_ingredient",
    "repair_decision",
    "repair_type",
    "approved_food_id",
    "approved_quantity_grams_estimated",
    "before_mapping_status",
    "before_food_id",
    "before_quantity_grams_estimated",
    "after_mapping_status",
    "after_food_id",
    "after_quantity_grams_estimated",
    "applied",
    "application_notes",
]


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    RECIPES_DRAFT_DIR.mkdir(parents=True, exist_ok=True)
    review_rows = read_csv(REVIEW_PATH)
    unit_rows = read_csv(ROUND37_UNIT_ROWS_PATH)
    mapping_rows = read_csv(ROUND37_MATCHES_PATH)
    food_lookup = build_food_lookup(read_csv(FOODDB_PATH))

    approved_rows = [
        row for row in review_rows
        if row.get("apply_repair") == "true"
        and row.get("repair_decision") in {"approve_safe", "approve_with_review"}
    ]
    deferred_rows = [row for row in review_rows if row not in approved_rows]

    repaired_unit_rows, unit_applied_keys = apply_unit_repairs(unit_rows, approved_rows)
    repaired_mapping_rows, applied_audit_rows = apply_mapping_repairs(
        mapping_rows=mapping_rows,
        approved_rows=approved_rows,
        food_lookup=food_lookup,
        unit_applied_keys=unit_applied_keys,
    )
    repaired_unmapped = [
        row for row in repaired_mapping_rows
        if row.get("mapping_status") != "accepted_auto"
    ]

    write_csv(OUT_REPAIRED_INGREDIENTS, repaired_unit_rows, list(unit_rows[0].keys()) if unit_rows else [])
    write_csv(OUT_REPAIRED_MATCHES, repaired_mapping_rows, list(mapping_rows[0].keys()) if mapping_rows else [])
    write_csv(OUT_REPAIRED_UNMAPPED, repaired_unmapped, list(mapping_rows[0].keys()) if mapping_rows else [])
    write_csv(OUT_APPLIED, applied_audit_rows, APPLIED_COLUMNS)
    write_csv(OUT_DEFERRED, deferred_rows, list(review_rows[0].keys()) if review_rows else [])
    OUT_SUMMARY.write_text(
        build_summary(review_rows, approved_rows, deferred_rows, applied_audit_rows),
        encoding="utf-8",
    )

    print("Round38 safe repairs applied")
    print(f"- reviewed rows: {len(review_rows)}")
    print(f"- approved rows: {len(approved_rows)}")
    print(f"- applied rows: {len([row for row in applied_audit_rows if row['applied'] == 'true'])}")
    print(f"- deferred/rejected rows: {len(deferred_rows)}")


def apply_unit_repairs(
    unit_rows: list[dict[str, str]],
    approved_rows: list[dict[str, str]],
) -> tuple[list[dict[str, str]], set[tuple[str, str]]]:
    approved_by_key = approved_map(approved_rows)
    applied_keys: set[tuple[str, str]] = set()
    repaired = []
    for row in unit_rows:
        updated = dict(row)
        key = key_for_row(row)
        repair = approved_by_key.get(key)
        if repair:
            grams = clean_text(repair.get("approved_quantity_grams_estimated"))
            if grams:
                updated["quantity_grams_estimated"] = grams
                applied_keys.add(key)
        repaired.append(updated)
    return repaired, applied_keys


def apply_mapping_repairs(
    *,
    mapping_rows: list[dict[str, str]],
    approved_rows: list[dict[str, str]],
    food_lookup: dict[str, dict[str, str]],
    unit_applied_keys: set[tuple[str, str]],
) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    approved_by_key = approved_map(approved_rows)
    repaired = []
    audit_rows = []
    for row in mapping_rows:
        updated = dict(row)
        key = key_for_row(row)
        repair = approved_by_key.get(key)
        if not repair:
            repaired.append(updated)
            continue

        before_status = clean_text(row.get("mapping_status"))
        before_food_id = clean_text(row.get("mapped_food_id"))
        before_grams = clean_text(row.get("quantity_grams_estimated"))
        approved_food_id = clean_text(repair.get("approved_food_id"))
        approved_grams = clean_text(repair.get("approved_quantity_grams_estimated"))
        applied = False
        notes = []

        if approved_grams:
            updated["quantity_grams_estimated"] = approved_grams
            applied = True
            notes.append("grams_updated")
        if approved_food_id and approved_food_id in food_lookup:
            updated["mapped_food_id"] = approved_food_id
            updated["mapped_food_canonical_name"] = food_lookup[approved_food_id].get("canonical_name", "")
            updated["mapping_status"] = "accepted_auto"
            updated["mapping_confidence"] = "high" if repair.get("repair_decision") == "approve_safe" else "medium"
            updated["mapping_method"] = f"round38_repair_{repair.get('repair_type', '')}"
            updated["mapping_notes"] = "round38_safe_local_repair"
            updated["manual_decision_notes"] = append_note(
                clean_text(updated.get("manual_decision_notes")),
                repair.get("repair_id", ""),
            )
            applied = True
            notes.append("alias_updated")
        if key in unit_applied_keys and "grams_updated" not in notes:
            notes.append("unit_row_updated")

        audit_rows.append(
            {
                "repair_id": repair.get("repair_id", ""),
                "recipe_id_candidate": repair.get("recipe_id_candidate", ""),
                "display_name": repair.get("display_name", ""),
                "blocking_ingredient": repair.get("blocking_ingredient", ""),
                "repair_decision": repair.get("repair_decision", ""),
                "repair_type": repair.get("repair_type", ""),
                "approved_food_id": approved_food_id,
                "approved_quantity_grams_estimated": approved_grams,
                "before_mapping_status": before_status,
                "before_food_id": before_food_id,
                "before_quantity_grams_estimated": before_grams,
                "after_mapping_status": updated.get("mapping_status", ""),
                "after_food_id": updated.get("mapped_food_id", ""),
                "after_quantity_grams_estimated": updated.get("quantity_grams_estimated", ""),
                "applied": str(applied).lower(),
                "application_notes": ";".join(notes),
            }
        )
        repaired.append(updated)
    return repaired, audit_rows


def approved_map(rows: list[dict[str, str]]) -> dict[tuple[str, str], dict[str, str]]:
    result = {}
    for row in rows:
        result[key_for_row(row)] = row
    return result


def key_for_row(row: dict[str, str]) -> tuple[str, str]:
    ingredient = (
        row.get("ingredient_name_normalized")
        or row.get("blocking_ingredient")
        or row.get("ingredient_raw_text")
        or ""
    ).strip().lower()
    return row.get("recipe_id_candidate", ""), ingredient


def build_summary(
    review_rows: list[dict[str, str]],
    approved_rows: list[dict[str, str]],
    deferred_rows: list[dict[str, str]],
    applied_audit_rows: list[dict[str, str]],
) -> str:
    decision_counts = Counter(row.get("repair_decision", "") for row in review_rows)
    fix_counts = Counter(row.get("repair_type", "") for row in approved_rows)
    applied_count = sum(1 for row in applied_audit_rows if row.get("applied") == "true")
    lines = [
        "Recipes_DB v1.2 Round38 safe repairs applied summary",
        "",
        f"- reviewed repair rows: {len(review_rows)}",
        f"- approved repair rows: {len(approved_rows)}",
        f"- applied repair rows: {applied_count}",
        f"- deferred/rejected repair rows: {len(deferred_rows)}",
        "",
        "Review decisions",
        *format_counter(decision_counts),
        "",
        "Applied fix types",
        *format_counter(fix_counts),
        "",
        "Applied rows",
    ]
    for row in applied_audit_rows:
        if row.get("applied") != "true":
            continue
        lines.append(
            "- "
            f"{row['repair_id']} | {row['display_name']} | "
            f"{row['blocking_ingredient']} -> {row['after_food_id'] or row['repair_type']} "
            f"grams={row['after_quantity_grams_estimated']}"
        )
    lines.extend(
        [
            "",
            "Strict notes",
            "- Applied repairs only modify Round38 draft/audit outputs.",
            "- No Food_DB additions were created.",
            "- Deferred rows are written separately for manual source review.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_food_lookup(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    return {row.get("food_id", ""): row for row in rows if row.get("food_id")}


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def clean_text(value: object) -> str:
    return "" if value is None else str(value).strip()


def append_note(existing: str, note: str) -> str:
    parts = [part.strip() for part in existing.split(";") if part.strip()]
    if note and note not in parts:
        parts.append(note)
    return "; ".join(parts)


def format_counter(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key or 'blank'}: {value}" for key, value in counter.most_common()]


if __name__ == "__main__":
    main()
