from __future__ import annotations

import csv
import json
import math
import statistics
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DATASET_DIR = ROOT / "data" / "recipesdb" / "draft" / "v1_2_generator_ready_round42_dataset_expanded"
AUDIT_DIR = ROOT / "data" / "recipesdb" / "audit"

RECIPES_PATH = DATASET_DIR / "recipes.csv"
INGREDIENTS_PATH = DATASET_DIR / "recipe_ingredients.csv"
NUTRITION_PATH = DATASET_DIR / "recipe_nutrition_cache.csv"
IMPACT_SUMMARY_PATH = AUDIT_DIR / "generator_v1_round42_dataset_expanded_impact_summary.txt"
DAYS_PATH = AUDIT_DIR / "generator_v1_round42_dataset_expanded_days.csv"
MEALS_PATH = AUDIT_DIR / "generator_v1_round42_dataset_expanded_meals.csv"

SUMMARY_OUT = AUDIT_DIR / "recipes_v1_2_round43_demo_readiness_summary.txt"
SLOT_OUT = AUDIT_DIR / "recipes_v1_2_round43_inventory_by_slot.csv"
PROTEIN_OUT = AUDIT_DIR / "recipes_v1_2_round43_inventory_by_protein.csv"
MACRO_OUT = AUDIT_DIR / "recipes_v1_2_round43_macro_quality.csv"
RISKS_OUT = AUDIT_DIR / "recipes_v1_2_round43_demo_risks.csv"
FAMILY_OUT = AUDIT_DIR / "recipes_v1_2_round43_family_variety_audit.csv"
FAMILY_SUMMARY_OUT = AUDIT_DIR / "recipes_v1_2_round43_family_variety_summary.txt"
FREEZE_OUT = AUDIT_DIR / "recipes_v1_2_round43_freeze_recommendation.txt"


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


def fnum(value: object, default: float | None = None) -> float | None:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    try:
        result = float(text)
    except ValueError:
        return default
    if math.isnan(result):
        return default
    return result


def truthy(value: object) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def median(values: list[float]) -> str:
    clean = [v for v in values if v is not None]
    if not clean:
        return ""
    return f"{statistics.median(clean):.3f}"


def parse_slots(raw: str) -> list[str]:
    text = (raw or "").strip()
    if not text:
        return ["unknown"]
    try:
        value = json.loads(text)
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()] or ["unknown"]
    except json.JSONDecodeError:
        pass
    return [part.strip().strip('"').strip("'") for part in text.replace("[", "").replace("]", "").split(",") if part.strip()] or ["unknown"]


def normalize(text: str) -> str:
    return " ".join((text or "").lower().replace("_", " ").replace("-", " ").split())


def ingredient_texts_by_recipe(ingredients: list[dict[str, str]]) -> dict[str, str]:
    grouped: dict[str, list[str]] = defaultdict(list)
    for row in ingredients:
        recipe_id = row.get("recipe_id", "")
        grouped[recipe_id].append(row.get("ingredient_name_normalized") or row.get("ingredient_name_parsed") or row.get("ingredient_raw_text") or "")
    return {recipe_id: " ".join(values) for recipe_id, values in grouped.items()}


def source_type(recipe: dict[str, str]) -> str:
    recipe_id = recipe.get("recipe_id", "")
    qc = normalize(recipe.get("qc_notes", "") + " " + recipe.get("scope_status", ""))
    if "round42_dataset" in recipe_id:
        return "dataset_curated_round42"
    if "round41_manual" in recipe_id or "manual_curated_round41" in qc:
        return "manual_curated_round41"
    if "manual_batch1" in qc or "batch1" in recipe_id:
        return "fooddb_manual_batch1_recovered"
    if "repaired" in qc or "round38" in recipe_id or "round36" in recipe_id:
        return "repaired"
    if "round37" in recipe_id:
        return "dataset_curated_round37"
    if "round28" in recipe_id or "round30" in recipe_id or "plus10" in qc or "plus30" in qc:
        return "targeted_expansion"
    if recipe_id.startswith("recipes_v1_1_candidate"):
        return "v1_1_base"
    return "unknown"


def primary_protein(recipe: dict[str, str], ingredient_text: str) -> str:
    text = normalize(" ".join([
        recipe.get("display_name", ""),
        recipe.get("recipe_family_name", ""),
        recipe.get("recipe_kind", ""),
        ingredient_text,
    ]))
    checks = [
        ("chicken", ["chicken"]),
        ("turkey", ["turkey"]),
        ("fish", ["salmon", "tuna", "cod", "fish", "shrimp", "seafood", "crab"]),
        ("beef", ["beef", "steak", "sirloin", "round steak"]),
        ("pork", ["pork", "ham", "bacon", "sausage"]),
        ("lamb", ["lamb"]),
        ("egg", ["egg", "omelet", "frittata"]),
        ("legume", ["lentil", "bean", "chickpea", "garbanzo", "split pea"]),
        ("dairy", ["yogurt", "cheese", "milk"]),
        ("vegetarian", ["vegetarian", "veggie", "tofu"]),
    ]
    for label, needles in checks:
        if any(needle in text for needle in needles):
            return label
    return "unknown"


def family_tags(recipe: dict[str, str], ingredient_text: str) -> list[str]:
    text = normalize(" ".join([
        recipe.get("display_name", ""),
        recipe.get("recipe_family_name", ""),
        recipe.get("recipe_kind", ""),
        recipe.get("recipe_category", ""),
        recipe.get("recipe_subcategory", ""),
        ingredient_text,
    ]))
    tags: list[str] = []
    checks = [
        ("fried_rice", ["fried rice"]),
        ("chicken_rice", ["chicken rice", "chicken and rice", "arroz con pollo"]),
        ("fish_potato", ["fish potato", "salmon potato", "cod potato"]),
        ("stuffed_peppers", ["stuffed pepper"]),
        ("oatmeal", ["oatmeal", "oats", "muesli"]),
        ("waffles", ["waffle"]),
        ("pancakes", ["pancake", "crepe", "crêpe", "blintz", "dutch baby"]),
        ("frittata", ["frittata"]),
        ("eggs", ["egg", "omelet"]),
        ("yogurt", ["yogurt"]),
        ("risotto", ["risotto"]),
        ("pasta", ["pasta", "spaghetti", "linguine", "fettuccine", "alfredo", "lasagna", "gnocchi", "macaroni", "noodle"]),
        ("rice", ["rice"]),
        ("potatoes", ["potato"]),
        ("beans_lentils", ["bean", "lentil", "chickpea", "garbanzo"]),
        ("soup", ["soup", "chili", "chowder", "minestrone", "stew"]),
        ("casserole", ["casserole", "bake", "baked"]),
        ("stir_fry", ["stir fry", "stir-fry", "saute"]),
        ("sandwich_toast", ["sandwich", "toast", "bread", "torta"]),
        ("salad", ["salad"]),
        ("curry", ["curry", "dahl"]),
        ("tacos_wraps", ["taco", "wrap", "tortilla", "quesadilla", "enchilada"]),
    ]
    for tag, needles in checks:
        if any(needle in text for needle in needles):
            tags.append(tag)
    return tags or ["other"]


def primary_family(tags: list[str]) -> str:
    order = [
        "fried_rice", "chicken_rice", "fish_potato", "stuffed_peppers", "oatmeal", "waffles",
        "pancakes", "frittata", "eggs", "yogurt", "risotto", "pasta", "rice", "potatoes",
        "beans_lentils", "soup", "casserole", "stir_fry", "sandwich_toast", "salad", "curry",
        "tacos_wraps", "other",
    ]
    for tag in order:
        if tag in tags:
            return tag
    return tags[0] if tags else "other"


def is_strong_for_slots(slots: list[str], kcal: float | None, protein: float | None, carbs: float | None, macro_ratio: float | None) -> bool:
    if kcal is None or protein is None or carbs is None:
        return False
    if "breakfast" in slots:
        return kcal >= 250 and protein >= 10 and carbs >= 35 and kcal <= 800 and (macro_ratio or 0) >= 0.45
    if "snack" in slots and len(slots) == 1:
        return 80 <= kcal <= 350
    if "lunch" in slots or "dinner" in slots:
        return kcal >= 450 and protein >= 18 and carbs >= 35 and (macro_ratio or 0) >= 0.60
    return False


def quality_flags(recipe: dict[str, str], nutrition: dict[str, str], slots: list[str]) -> list[str]:
    kcal = fnum(nutrition.get("energy_kcal_per_serving"))
    protein = fnum(nutrition.get("protein_g_per_serving"))
    carbs = fnum(nutrition.get("carbs_g_per_serving"))
    fat = fnum(nutrition.get("fat_g_per_serving"))
    macro_ratio = fnum(nutrition.get("macro_relevant_mapped_weight_ratio"))
    flags: list[str] = []
    if "breakfast" in slots and kcal is not None and kcal > 850:
        flags.append("breakfast_over_850_kcal")
    if slots == ["snack"] and kcal is not None and kcal > 400:
        flags.append("snack_over_400_kcal")
    if ("lunch" in slots or "dinner" in slots) and kcal is not None and kcal < 250:
        flags.append("main_under_250_kcal")
    if ("lunch" in slots or "dinner" in slots) and protein is not None and protein < 10:
        flags.append("main_protein_under_10g")
    if ("lunch" in slots or "dinner" in slots) and carbs is not None and carbs < 10:
        flags.append("main_carbs_under_10g")
    if kcal and fat is not None and (fat * 9 / kcal) > 0.55:
        flags.append("very_high_fat_share")
    if macro_ratio is not None and macro_ratio < 0.50 and ("lunch" in slots or "dinner" in slots):
        flags.append("main_macro_relevant_ratio_under_0_50")
    effective_time = fnum(recipe.get("effective_time_min_for_scoring"))
    total_time = fnum(recipe.get("total_time_min"))
    if effective_time is None or effective_time <= 0:
        flags.append("missing_effective_time")
    if total_time is None or total_time <= 0:
        flags.append("missing_total_time")
    name = normalize(recipe.get("display_name", ""))
    suspicious_terms = [
        "cookie", "cookies", "candy", "ice cream", "cocktail", "drink", "punch",
        "dog food", "cat food", "pet food", "frosting", "dessert", "brownie",
        "cupcake", "milkshake",
    ]
    if any(term in name for term in suspicious_terms):
        flags.append("suspicious_name_content")
    return flags


def load_round42_metrics() -> dict[str, str]:
    metrics: dict[str, str] = {}
    if not IMPACT_SUMMARY_PATH.exists():
        return metrics
    for raw_line in IMPACT_SUMMARY_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if "=" in line and line.startswith(("selected_", "generator_", "strong_", "manual_")):
            key, value = line.split("=", 1)
            metrics[key.strip()] = value.strip()
        if line.startswith("- round42_dataset_expanded:"):
            metrics["round42_summary_line"] = line
        if line.startswith("- round41_manual_curated:"):
            metrics["round41_summary_line"] = line
    return metrics


def main() -> None:
    recipes = read_csv(RECIPES_PATH)
    nutrition_rows = read_csv(NUTRITION_PATH)
    ingredients = read_csv(INGREDIENTS_PATH)
    meals = read_csv(MEALS_PATH)
    nutrition_by_id = {row.get("recipe_id", ""): row for row in nutrition_rows}
    ingredient_text_by_id = ingredient_texts_by_recipe(ingredients)

    enriched: list[dict[str, object]] = []
    slot_counter: Counter[str] = Counter()
    kind_counter: Counter[str] = Counter()
    protein_counter: Counter[str] = Counter()
    source_counter: Counter[str] = Counter()
    cache_counter: Counter[str] = Counter()
    family_by_slot: dict[str, Counter[str]] = defaultdict(Counter)
    macro_rows: list[dict[str, object]] = []
    family_rows: list[dict[str, object]] = []

    for recipe in recipes:
        recipe_id = recipe.get("recipe_id", "")
        nutrition = nutrition_by_id.get(recipe_id, {})
        slots = parse_slots(recipe.get("allowed_slots_json", ""))
        ingredient_text = ingredient_text_by_id.get(recipe_id, "")
        protein = primary_protein(recipe, ingredient_text)
        tags = family_tags(recipe, ingredient_text)
        primary_tag = primary_family(tags)
        src_type = source_type(recipe)
        kcal = fnum(nutrition.get("energy_kcal_per_serving"))
        protein_g = fnum(nutrition.get("protein_g_per_serving"))
        carbs_g = fnum(nutrition.get("carbs_g_per_serving"))
        fat_g = fnum(nutrition.get("fat_g_per_serving"))
        macro_ratio = fnum(nutrition.get("macro_relevant_mapped_weight_ratio"))
        mapped_ratio = fnum(nutrition.get("mapped_weight_ratio"))
        strong = is_strong_for_slots(slots, kcal, protein_g, carbs_g, macro_ratio)
        flags = quality_flags(recipe, nutrition, slots)

        kind_counter[recipe.get("recipe_kind", "unknown") or "unknown"] += 1
        protein_counter[protein] += 1
        source_counter[src_type] += 1
        cache_counter[nutrition.get("cache_status", "missing") or "missing"] += 1
        for slot in slots:
            slot_counter[slot] += 1
            family_by_slot[slot][primary_tag] += 1

        enriched.append({
            "recipe_id": recipe_id,
            "display_name": recipe.get("display_name", ""),
            "slots": slots,
            "recipe_kind": recipe.get("recipe_kind", ""),
            "primary_protein": protein,
            "source_type": src_type,
            "primary_family": primary_tag,
            "family_tags": ";".join(tags),
            "kcal": kcal,
            "protein_g": protein_g,
            "carbs_g": carbs_g,
            "fat_g": fat_g,
            "macro_ratio": macro_ratio,
            "mapped_ratio": mapped_ratio,
            "cache_status": nutrition.get("cache_status", ""),
            "strong_generator_ready_estimate": strong,
            "quality_flags": flags,
        })

        macro_rows.append({
            "recipe_id": recipe_id,
            "display_name": recipe.get("display_name", ""),
            "allowed_slots_json": recipe.get("allowed_slots_json", ""),
            "recipe_kind": recipe.get("recipe_kind", ""),
            "source_type": src_type,
            "primary_protein": protein,
            "primary_family": primary_tag,
            "energy_kcal_per_serving": "" if kcal is None else f"{kcal:.3f}",
            "protein_g_per_serving": "" if protein_g is None else f"{protein_g:.3f}",
            "carbs_g_per_serving": "" if carbs_g is None else f"{carbs_g:.3f}",
            "fat_g_per_serving": "" if fat_g is None else f"{fat_g:.3f}",
            "mapped_weight_ratio": "" if mapped_ratio is None else f"{mapped_ratio:.4f}",
            "macro_relevant_mapped_weight_ratio": "" if macro_ratio is None else f"{macro_ratio:.4f}",
            "cache_status": nutrition.get("cache_status", ""),
            "strong_generator_ready_estimate": str(strong).lower(),
            "quality_flags": ";".join(flags),
        })
        family_rows.append({
            "row_scope": "dataset",
            "day_index": "",
            "slot": ";".join(slots),
            "recipe_id": recipe_id,
            "display_name": recipe.get("display_name", ""),
            "recipe_kind": recipe.get("recipe_kind", ""),
            "primary_family": primary_tag,
            "family_tags": ";".join(tags),
            "source_type": src_type,
            "selected_in_latest_plan": "false",
        })

    latest_plan = [row for row in meals if row.get("scenario") == "round42_dataset_expanded"]
    plan_recipe_ids = [row.get("recipe_id", "") for row in latest_plan]
    exact_repeats = sorted([recipe_id for recipe_id, count in Counter(plan_recipe_ids).items() if count > 1])
    plan_family_by_slot: dict[str, Counter[str]] = defaultdict(Counter)
    plan_family_all_main: Counter[str] = Counter()
    plan_rows: list[dict[str, object]] = []
    for meal in latest_plan:
        recipe_id = meal.get("recipe_id", "")
        recipe = next((row for row in recipes if row.get("recipe_id") == recipe_id), {})
        ingredient_text = ingredient_text_by_id.get(recipe_id, "")
        tags = family_tags(recipe or {"display_name": meal.get("display_name", "")}, ingredient_text)
        family = primary_family(tags)
        slot = meal.get("slot", "")
        plan_family_by_slot[slot][family] += 1
        if slot in {"lunch", "dinner"}:
            plan_family_all_main[family] += 1
        plan_rows.append({
            "row_scope": "latest_plan",
            "day_index": meal.get("day_index", ""),
            "slot": slot,
            "recipe_id": recipe_id,
            "display_name": meal.get("display_name", ""),
            "recipe_kind": recipe.get("recipe_kind", ""),
            "primary_family": family,
            "family_tags": ";".join(tags),
            "source_type": source_type(recipe) if recipe else "",
            "selected_in_latest_plan": "true",
        })
    family_rows.extend(plan_rows)

    repeated_breakfast_family_count = sum(count - 1 for count in plan_family_by_slot.get("breakfast", Counter()).values() if count > 1)
    repeated_main_family_count = sum(count - 1 for count in plan_family_all_main.values() if count > 1)
    repeated_family_by_slot = []
    for slot, counter in sorted(plan_family_by_slot.items()):
        for fam, count in sorted(counter.items()):
            if count > 1:
                repeated_family_by_slot.append(f"{slot}:{fam}={count}")
    if exact_repeats or repeated_breakfast_family_count >= 2 or repeated_main_family_count >= 3:
        semantic_status = "weak"
    elif repeated_family_by_slot or repeated_main_family_count:
        semantic_status = "review"
    else:
        semantic_status = "good"

    slot_rows: list[dict[str, object]] = []
    for slot in sorted(slot_counter):
        slot_recipes = [row for row in enriched if slot in row["slots"]]
        slot_rows.append({
            "slot": slot,
            "recipe_count": len(slot_recipes),
            "strong_generator_ready_estimate_count": sum(1 for row in slot_recipes if row["strong_generator_ready_estimate"]),
            "median_kcal": median([row["kcal"] for row in slot_recipes if row["kcal"] is not None]),
            "median_protein_g": median([row["protein_g"] for row in slot_recipes if row["protein_g"] is not None]),
            "median_carbs_g": median([row["carbs_g"] for row in slot_recipes if row["carbs_g"] is not None]),
            "median_fat_g": median([row["fat_g"] for row in slot_recipes if row["fat_g"] is not None]),
            "median_macro_relevant_mapped_weight_ratio": median([row["macro_ratio"] for row in slot_recipes if row["macro_ratio"] is not None]),
            "dominant_families": ";".join(f"{fam}:{count}" for fam, count in family_by_slot[slot].most_common(8)),
        })

    protein_rows = [{
        "primary_protein": protein,
        "recipe_count": count,
        "share_of_total": f"{count / max(len(recipes), 1):.3f}",
    } for protein, count in sorted(protein_counter.items(), key=lambda item: (-item[1], item[0]))]

    risk_rows: list[dict[str, object]] = []
    flag_counter: Counter[str] = Counter()
    flag_examples: dict[str, list[str]] = defaultdict(list)
    for row in enriched:
        for flag in row["quality_flags"]:
            flag_counter[flag] += 1
            if len(flag_examples[flag]) < 8:
                flag_examples[flag].append(str(row["display_name"]))
    for flag, count in sorted(flag_counter.items(), key=lambda item: (-item[1], item[0])):
        severity = "high" if flag in {"suspicious_name_content", "missing_effective_time"} and count else "medium"
        if flag in {"main_under_250_kcal", "main_protein_under_10g", "main_carbs_under_10g"} and count >= 5:
            severity = "high"
        risk_rows.append({
            "risk_type": flag,
            "severity": severity,
            "affected_count": count,
            "examples": "; ".join(flag_examples[flag]),
            "recommendation": "review_before_current_materialization",
        })
    if semantic_status != "good":
        risk_rows.append({
            "risk_type": "semantic_family_variety_latest_plan",
            "severity": "medium" if semantic_status == "review" else "high",
            "affected_count": repeated_breakfast_family_count + repeated_main_family_count,
            "examples": "; ".join(repeated_family_by_slot),
            "recommendation": "audit_family_level_variety_before_week_level_demo",
        })

    metrics = load_round42_metrics()
    target_met = len(recipes) >= 250
    valid_multiday = "valid=3" in metrics.get("round42_summary_line", "") and "accept=3" in metrics.get("round42_summary_line", "") and "repeated=0" in metrics.get("round42_summary_line", "")
    high_risks = sum(1 for row in risk_rows if row["severity"] == "high")
    demo_status = "demo_ready" if target_met and valid_multiday and high_risks == 0 and semantic_status == "good" else "demo_ready_with_risks"
    if not target_met or not valid_multiday:
        demo_status = "not_demo_ready"

    write_csv(SLOT_OUT, slot_rows, [
        "slot", "recipe_count", "strong_generator_ready_estimate_count", "median_kcal",
        "median_protein_g", "median_carbs_g", "median_fat_g",
        "median_macro_relevant_mapped_weight_ratio", "dominant_families",
    ])
    write_csv(PROTEIN_OUT, protein_rows, ["primary_protein", "recipe_count", "share_of_total"])
    write_csv(MACRO_OUT, macro_rows, [
        "recipe_id", "display_name", "allowed_slots_json", "recipe_kind", "source_type",
        "primary_protein", "primary_family", "energy_kcal_per_serving",
        "protein_g_per_serving", "carbs_g_per_serving", "fat_g_per_serving",
        "mapped_weight_ratio", "macro_relevant_mapped_weight_ratio", "cache_status",
        "strong_generator_ready_estimate", "quality_flags",
    ])
    write_csv(RISKS_OUT, risk_rows, ["risk_type", "severity", "affected_count", "examples", "recommendation"])
    write_csv(FAMILY_OUT, family_rows, [
        "row_scope", "day_index", "slot", "recipe_id", "display_name", "recipe_kind",
        "primary_family", "family_tags", "source_type", "selected_in_latest_plan",
    ])

    family_summary_lines = [
        "Recipes_DB v1.2 Round43 family variety summary",
        "",
        f"latest_plan_exact_repeats={len(exact_repeats)}",
        f"latest_plan_repeated_recipe_ids={';'.join(exact_repeats) if exact_repeats else 'none'}",
        f"repeated_family_by_slot={';'.join(repeated_family_by_slot) if repeated_family_by_slot else 'none'}",
        f"repeated_breakfast_family_count={repeated_breakfast_family_count}",
        f"repeated_lunch_dinner_family_count={repeated_main_family_count}",
        f"semantic_variety_status={semantic_status}",
        "",
        "Dataset dominant families by slot:",
    ]
    for slot in sorted(family_by_slot):
        family_summary_lines.append(f"- {slot}: " + "; ".join(f"{fam}={count}" for fam, count in family_by_slot[slot].most_common(10)))
    family_summary_lines.extend([
        "",
        "Interpretation:",
        "- Exact no-repeat is good in the current 3-day plan.",
        "- Breakfast semantic variety needs review if oatmeal/oat-like breakfasts dominate consecutive days.",
        "- Do not implement a family constraint yet; keep this as audit evidence.",
    ])
    write_text(FAMILY_SUMMARY_OUT, family_summary_lines)

    summary_lines = [
        "Recipes_DB v1.2 Round43 demo readiness summary",
        "",
        f"baseline_dataset={DATASET_DIR.relative_to(ROOT)}",
        f"total_recipes={len(recipes)}",
        f"target_250_generator_ready_met={str(target_met).lower()}",
        f"demo_readiness_status={demo_status}",
        f"strong_generator_ready_estimate={sum(1 for row in enriched if row['strong_generator_ready_estimate'])}",
        "",
        "Slot inventory:",
    ]
    for row in slot_rows:
        summary_lines.append(f"- {row['slot']}: recipes={row['recipe_count']}, strong_estimate={row['strong_generator_ready_estimate_count']}, med_kcal={row['median_kcal']}, med_P/C/F={row['median_protein_g']}/{row['median_carbs_g']}/{row['median_fat_g']}")
    summary_lines.extend([
        "",
        "Recipe kind counts:",
        *[f"- {kind}: {count}" for kind, count in kind_counter.most_common()],
        "",
        "Primary protein counts:",
        *[f"- {protein}: {count}" for protein, count in protein_counter.most_common()],
        "",
        "Source type counts:",
        *[f"- {source}: {count}" for source, count in source_counter.most_common()],
        "",
        "Cache status counts:",
        *[f"- {status}: {count}" for status, count in cache_counter.most_common()],
        "",
        "Round42 multi-day:",
        f"- {metrics.get('round42_summary_line', 'missing')}",
        "",
        "Main risks:",
    ])
    if risk_rows:
        for row in risk_rows[:20]:
            summary_lines.append(f"- {row['severity']}: {row['risk_type']} count={row['affected_count']} examples={row['examples']}")
    else:
        summary_lines.append("- none")
    summary_lines.extend([
        "",
        "Strict assessment:",
        "- The dataset meets the approximate 250 generator-ready target and supports a valid accept no-repeat 3-day demo.",
        "- It should still be treated as demo_ready_with_risks because source-derived recipes have noisy naming/mapping history and semantic family variety is not yet enforced.",
    ])
    write_text(SUMMARY_OUT, summary_lines)

    freeze_lines = [
        "Recipes_DB v1.2 Round43 freeze recommendation",
        "",
        f"dataset={DATASET_DIR.relative_to(ROOT)}",
        f"recipe_count={len(recipes)}",
        f"demo_readiness_status={demo_status}",
        f"semantic_variety_status={semantic_status}",
        "",
        "Recommendation:",
        "- Freeze this dataset as a v1.2 demo candidate, not as production/current.",
        "- Do not run another broad expansion batch before repairing and reviewing the current queue.",
        "- Run source verification batch2 for blockers that recover multiple useful recipes.",
        "- Keep family-level variety as an audit/UX risk; implement a constraint only after a separate design decision.",
        "",
        "Known risks before any production/current materialization:",
        "- Some source-derived recipes still have noisy titles or mojibake from the upstream dataset.",
        "- Breakfast semantic variety in the latest plan is weak because oatmeal/oat-like families repeat.",
        "- Round42 failed recipes show mapping and macro coverage gaps that should be prioritized instead of ignored.",
        "- Persistent docs/architecture should be updated only after explicit approval.",
        "",
        "Next action choice:",
        "- Recommended combination: A freeze demo candidate + B source verification batch2 + later C family-level variety design.",
    ]
    write_text(FREEZE_OUT, freeze_lines)
    print(f"Round43 readiness audit complete: recipes={len(recipes)} status={demo_status} semantic={semantic_status}")


if __name__ == "__main__":
    main()
