from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SOURCE_DATASET_DIR = ROOT / "data" / "recipesdb" / "draft" / "v1_2_generator_ready_round42_dataset_expanded"
DEMO_DATASET_DIR = ROOT / "data" / "recipesdb" / "draft" / "v1_2_demo_candidate"
RECIPES_AUDIT_DIR = ROOT / "data" / "recipesdb" / "audit"
FOODDB_AUDIT_DIR = ROOT / "data" / "fooddb" / "audit"
FOODDB_DRAFT_DIR = ROOT / "data" / "fooddb" / "draft"
OUTPUTS_DIR = ROOT / "outputs"

ROUND43_SLOT_PATH = RECIPES_AUDIT_DIR / "recipes_v1_2_round43_inventory_by_slot.csv"
ROUND43_PROTEIN_PATH = RECIPES_AUDIT_DIR / "recipes_v1_2_round43_inventory_by_protein.csv"
ROUND43_RISKS_PATH = RECIPES_AUDIT_DIR / "recipes_v1_2_round43_demo_risks.csv"
ROUND43_SOURCE_CANDIDATES_PATH = FOODDB_AUDIT_DIR / "fooddb_v1_2_source_verification_batch2_candidates.csv"

FREEZE_SUMMARY_OUT = RECIPES_AUDIT_DIR / "recipes_v1_2_demo_candidate_freeze_summary.txt"
KNOWN_RISKS_OUT = RECIPES_AUDIT_DIR / "recipes_v1_2_demo_candidate_known_risks.csv"
INVENTORY_OUT = RECIPES_AUDIT_DIR / "recipes_v1_2_demo_candidate_inventory.csv"
SMOKE_SUMMARY_OUT = RECIPES_AUDIT_DIR / "generator_v1_v1_2_demo_candidate_smoke_summary.txt"

HANDOFF_OUT = FOODDB_AUDIT_DIR / "fooddb_v1_2_source_verification_batch2_handoff.csv"
HANDOFF_PROMPT_OUT = FOODDB_AUDIT_DIR / "fooddb_v1_2_source_verification_batch2_handoff_prompt.txt"
TEMPLATE_OUT = FOODDB_DRAFT_DIR / "fooddb_v1_2_manual_additions_verified_batch2_TEMPLATE.csv"

DATASET_PROFILE = "v1_2_demo_candidate"
SOURCE_PROFILE = "v1_2_generator_ready_round42_dataset_expanded"


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({name: row.get(name, "") for name in fieldnames})


def write_text(path: Path, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def safe_float(value: object) -> float | None:
    try:
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except (TypeError, ValueError):
        return None


def normalize_id(text: str) -> str:
    value = re.sub(r"[^a-z0-9]+", "_", (text or "").lower()).strip("_")
    return value or "unknown"


def infer_role(name: str) -> str:
    text = (name or "").lower()
    if any(term in text for term in ["beef", "cod", "lamb", "pork", "chicken", "salmon", "tuna", "steak"]):
        return "protein"
    if any(term in text for term in ["bean", "garbanzo", "lentil"]):
        return "carb_protein"
    if any(term in text for term in ["milk", "cheese", "cream"]):
        return "dairy"
    if any(term in text for term in ["oil"]):
        return "fat"
    return "other"


def infer_food_group(role: str) -> str:
    return {
        "protein": "meat_fish_eggs",
        "carb_protein": "legumes",
        "dairy": "dairy",
        "fat": "fats_oils",
    }.get(role, "other")


def copy_demo_dataset() -> None:
    DEMO_DATASET_DIR.mkdir(parents=True, exist_ok=True)
    for name in ["recipes.csv", "recipe_ingredients.csv", "recipe_nutrition_cache.csv"]:
        shutil.copy2(SOURCE_DATASET_DIR / name, DEMO_DATASET_DIR / name)


def count_rows(path: Path) -> int:
    return len(read_csv(path))


def cache_counts() -> Counter[str]:
    rows = read_csv(DEMO_DATASET_DIR / "recipe_nutrition_cache.csv")
    return Counter(row.get("cache_status", "missing") or "missing" for row in rows)


def write_readme(recipe_count: int) -> None:
    lines = [
        "Recipes_DB v1.2 demo candidate",
        "",
        "Status:",
        "- This is a draft/demo candidate, not production current.",
        f"- Source dataset profile: {SOURCE_PROFILE}.",
        f"- Recipe count: {recipe_count}.",
        "- Readiness status: demo_ready_with_risks.",
        "- Do not replace data/recipesdb/current yet.",
        "- Intended use: Generator v1 / Checkpoint 1-2 demo/testing.",
        "",
        "Known risks:",
        "- semantic family variety weak",
        "- oatmeal breakfast repetition",
        "- high-fat-share recipes",
        "- low-carb main recipes",
        "- some breakfast calorie outliers",
        "- turkey and legume coverage still low",
        "- no production QA",
        "",
        "Recommended generator config:",
        "- dataset_profile = v1_2_demo_candidate",
        "- selection_mode = balanced_day",
        "- portion_policy = target_aware",
        "- meal_realism_mode = practical",
        "- quality_gate = demo_safe",
        "- multi_day_mode = global_alternatives_3_day",
        "- multi_day_no_repeat_policy = hard",
        "- day_candidate_builder = direct_from_slots",
    ]
    write_text(DEMO_DATASET_DIR / "README_v1_2_demo_candidate.txt", lines)


def write_freeze_audits(recipe_count: int) -> None:
    slot_rows = read_csv(ROUND43_SLOT_PATH)
    protein_rows = read_csv(ROUND43_PROTEIN_PATH)
    risk_rows = read_csv(ROUND43_RISKS_PATH)
    caches = cache_counts()
    inventory_rows: list[dict[str, object]] = []
    for row in slot_rows:
        inventory_rows.append({
            "inventory_type": "slot",
            "name": row.get("slot", ""),
            "count": row.get("recipe_count", ""),
            "extra": f"strong={row.get('strong_generator_ready_estimate_count', '')}; median_kcal={row.get('median_kcal', '')}",
        })
    for row in protein_rows:
        inventory_rows.append({
            "inventory_type": "primary_protein",
            "name": row.get("primary_protein", ""),
            "count": row.get("recipe_count", ""),
            "extra": f"share={row.get('share_of_total', '')}",
        })
    for status, count in caches.most_common():
        inventory_rows.append({
            "inventory_type": "cache_status",
            "name": status,
            "count": count,
            "extra": "",
        })
    write_csv(INVENTORY_OUT, inventory_rows, ["inventory_type", "name", "count", "extra"])
    write_csv(KNOWN_RISKS_OUT, risk_rows, ["risk_type", "severity", "affected_count", "examples", "recommendation"])

    lines = [
        "Recipes_DB v1.2 demo candidate freeze summary",
        "",
        f"source_profile={SOURCE_PROFILE}",
        f"demo_profile={DATASET_PROFILE}",
        f"dataset_path={DEMO_DATASET_DIR.relative_to(ROOT)}",
        f"recipe_count={recipe_count}",
        "readiness_status=demo_ready_with_risks",
        "production_current_status=not_current_not_production",
        "",
        "Recommended generator config:",
        "- dataset_profile=v1_2_demo_candidate",
        "- selection_mode=balanced_day",
        "- portion_policy=target_aware",
        "- meal_realism_mode=practical",
        "- quality_gate=demo_safe",
        "- multi_day_mode=global_alternatives_3_day",
        "- multi_day_no_repeat_policy=hard",
        "- day_candidate_builder=direct_from_slots",
        "",
        "Why not production/current:",
        "- semantic family variety is weak in the current 3-day plan",
        "- oatmeal-like breakfast repeats semantically",
        "- source-derived data still needs QA for macro outliers and naming noise",
        "- Food_DB source verification batch2 is still pending",
        "",
        "Next recommended work:",
        "- run manual source verification batch2",
        "- review selected alias/unit repairs from the repair queue",
        "- decide separately whether family-level variety should become generator behavior",
    ]
    write_text(FREEZE_SUMMARY_OUT, lines)


def write_source_handoff() -> None:
    source_rows = read_csv(ROUND43_SOURCE_CANDIDATES_PATH)
    handoff_rows: list[dict[str, object]] = []
    for index, row in enumerate(source_rows, start=1):
        blocker = row.get("ingredient_or_blocker", "")
        role = infer_role(blocker)
        handoff_rows.append({
            "blocker_id": f"batch2_blocker_{index:03d}",
            "candidate_food_id": f"food_v1_2_batch2_candidate_{normalize_id(blocker)}",
            "canonical_name": normalize_id(blocker),
            "display_name": blocker,
            "ingredient_examples": blocker,
            "affected_recipe_count": row.get("affected_recipes_count", ""),
            "affected_recipe_names": row.get("affected_recipe_names", ""),
            "role": role,
            "priority": row.get("priority_score", ""),
            "source_needed": row.get("source_needed", "true"),
            "suggested_source_type": row.get("recommended_source_type", ""),
            "suggested_search_query": f"{blocker} nutrition per 100g raw cooked kcal protein carbs fat",
            "decision": "",
            "source_name": "",
            "source_url": "",
            "raw_or_cooked_state": "",
            "energy_kcal_100": "",
            "protein_g_100": "",
            "carbs_g_100": "",
            "fat_g_100": "",
            "confidence": "",
            "qc_notes": row.get("risk_notes", ""),
        })
    handoff_fields = [
        "blocker_id",
        "candidate_food_id",
        "canonical_name",
        "display_name",
        "ingredient_examples",
        "affected_recipe_count",
        "affected_recipe_names",
        "role",
        "priority",
        "source_needed",
        "suggested_source_type",
        "suggested_search_query",
        "decision",
        "source_name",
        "source_url",
        "raw_or_cooked_state",
        "energy_kcal_100",
        "protein_g_100",
        "carbs_g_100",
        "fat_g_100",
        "confidence",
        "qc_notes",
    ]
    write_csv(HANDOFF_OUT, handoff_rows, handoff_fields)

    template_rows = []
    for row in handoff_rows:
        role = row.get("role", "")
        template_rows.append({
            "candidate_food_id": row.get("candidate_food_id", ""),
            "canonical_name": row.get("canonical_name", ""),
            "display_name": row.get("display_name", ""),
            "food_group": infer_food_group(str(role)),
            "role": role,
            "raw_or_cooked_state": "",
            "energy_kcal_100": "",
            "protein_g_100": "",
            "carbs_g_100": "",
            "fat_g_100": "",
            "source_name": "",
            "source_url": "",
            "source_type": row.get("suggested_source_type", ""),
            "confidence": "",
            "qc_notes": row.get("qc_notes", ""),
            "decision": "",
        })
    template_fields = [
        "candidate_food_id",
        "canonical_name",
        "display_name",
        "food_group",
        "role",
        "raw_or_cooked_state",
        "energy_kcal_100",
        "protein_g_100",
        "carbs_g_100",
        "fat_g_100",
        "source_name",
        "source_url",
        "source_type",
        "confidence",
        "qc_notes",
        "decision",
    ]
    write_csv(TEMPLATE_OUT, template_rows, template_fields)

    prompt_lines = [
        "Food_DB v1.2 source verification batch2 handoff prompt",
        "",
        "Task:",
        "- Complete the CSV template for the provided Food_DB source verification candidates.",
        "- Return CSV-ready output using the exact template columns.",
        "",
        "Hard rules:",
        "- Do not invent nutrition values.",
        "- Use CIQUAL, USDA, official nutrition databases, or official manufacturer data where appropriate.",
        "- Do not confuse kJ with kcal.",
        "- Raw/cooked/processed state must be explicit.",
        "- For processed foods, manufacturer or official product nutrition may be used.",
        "- If no good source exists, set decision=keep_deferred.",
        "",
        "Decision values:",
        "- safe_to_add",
        "- needs_review",
        "- keep_deferred",
        "",
        "Required macro fields per 100g:",
        "- energy_kcal_100",
        "- protein_g_100",
        "- carbs_g_100",
        "- fat_g_100",
        "- source_name",
        "- source_url",
        "- raw_or_cooked_state",
        "",
        f"Input handoff CSV: {HANDOFF_OUT.relative_to(ROOT)}",
        f"Output template CSV: {TEMPLATE_OUT.relative_to(ROOT)}",
    ]
    write_text(HANDOFF_PROMPT_OUT, prompt_lines)


def read_json(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def summarize_one_day(plan: dict[str, object] | None) -> list[str]:
    if not plan:
        return ["one_day_status=not_run_or_missing_output"]
    validation = plan.get("validation", {})
    quality_status = plan.get("quality_gate_status") or plan.get("quality_gate", {}).get("quality_gate_status")
    totals = plan.get("day_totals") or plan.get("totals") or {}
    selected = plan.get("selected_meals") or plan.get("meals") or []
    return [
        "one_day_status=completed",
        f"one_day_validation={validation.get('validation_status', 'unknown') if isinstance(validation, dict) else 'unknown'}",
        f"one_day_quality_gate={quality_status or 'unknown'}",
        f"one_day_kcal={totals.get('energy_kcal') or totals.get('kcal') or totals.get('total_kcal') or 'unknown'}",
        f"one_day_selected_meals={len(selected) if isinstance(selected, list) else 'unknown'}",
    ]


def summarize_multi_day(plan: dict[str, object] | None) -> list[str]:
    if not plan:
        return ["three_day_status=not_run_or_missing_output"]
    summary = plan.get("multi_day_summary", {})
    return [
        "three_day_status=completed",
        f"three_day_valid_days={summary.get('valid_day_count', 'unknown') if isinstance(summary, dict) else 'unknown'}",
        f"three_day_accept_days={summary.get('accept_day_count', 'unknown') if isinstance(summary, dict) else 'unknown'}",
        f"three_day_review_days={summary.get('review_day_count', 'unknown') if isinstance(summary, dict) else 'unknown'}",
        f"three_day_repeated_recipes={summary.get('repeated_recipe_count', 'unknown') if isinstance(summary, dict) else 'unknown'}",
        f"three_day_unique_recipes={summary.get('unique_recipe_count', 'unknown') if isinstance(summary, dict) else 'unknown'}",
        f"three_day_multi_day_loss={summary.get('multi_day_loss', plan.get('multi_day_loss', 'unknown')) if isinstance(summary, dict) else plan.get('multi_day_loss', 'unknown')}",
    ]


def write_smoke_summary() -> None:
    one_day = read_json(OUTPUTS_DIR / "generator_v1_plan.json")
    multi_day = read_json(OUTPUTS_DIR / "generator_v1_multiday_plan.json")
    lines = [
        "Generator v1 v1.2 demo candidate smoke summary",
        "",
        f"dataset_profile={DATASET_PROFILE}",
        *summarize_one_day(one_day),
        "",
        *summarize_multi_day(multi_day),
    ]
    write_text(SMOKE_SUMMARY_OUT, lines)


def freeze_package() -> None:
    copy_demo_dataset()
    recipe_count = count_rows(DEMO_DATASET_DIR / "recipes.csv")
    write_readme(recipe_count)
    write_freeze_audits(recipe_count)
    write_source_handoff()
    write_smoke_summary()
    print(f"Round44 demo candidate package written: recipes={recipe_count}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Freeze Recipes_DB v1.2 demo candidate and handoff package.")
    parser.add_argument("--smoke-summary-only", action="store_true")
    args = parser.parse_args()
    if args.smoke_summary_only:
        write_smoke_summary()
        print("Round44 smoke summary updated.")
        return
    freeze_package()


if __name__ == "__main__":
    main()
