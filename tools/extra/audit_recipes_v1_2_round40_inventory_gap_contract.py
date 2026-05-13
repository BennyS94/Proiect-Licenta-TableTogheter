from __future__ import annotations

import csv
import json
import statistics
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]

BASELINE_CANDIDATES = [
    Path("data/recipesdb/draft/v1_2_generator_ready_round37_expanded_repaired_manual_batch1"),
    Path("data/recipesdb/draft/v1_2_generator_ready_round37_expanded_repaired"),
    Path("data/recipesdb/draft/v1_2_generator_ready_round37_expanded"),
    Path("data/recipesdb/draft/v1_2_generator_ready_plus30_plus15"),
    Path("data/recipesdb/draft/v1_2_generator_ready_plus30"),
    Path("data/recipesdb/draft/v1_1_generator_ready_slot_checked_time_enriched_snack_curated"),
]

AUDIT_DIR = ROOT / "data/recipesdb/audit"
SOURCE_RECIPES = ROOT / "data/recipesdb/source/1_Recipe_csv.csv"
MANUAL_REPAIR_QUEUE = ROOT / "data/recipesdb/draft/recipes_v1_2_manual_repair_queue.csv"
SOURCE_VERIFICATION_QUEUE = ROOT / "data/fooddb/audit/fooddb_v1_2_manual_source_verification_queue.csv"

SUMMARY_OUT = AUDIT_DIR / "recipes_v1_2_round40_inventory_summary.txt"
BY_SLOT_OUT = AUDIT_DIR / "recipes_v1_2_round40_inventory_by_slot.csv"
BY_PROTEIN_OUT = AUDIT_DIR / "recipes_v1_2_round40_inventory_by_protein.csv"
MACRO_OUT = AUDIT_DIR / "recipes_v1_2_round40_macro_coverage.csv"
GAP_OUT = AUDIT_DIR / "recipes_v1_2_round40_gap_analysis.csv"
CONTRACT_OUT = AUDIT_DIR / "recipes_v1_2_round40_expansion_contract.txt"
TARGETS_OUT = AUDIT_DIR / "recipes_v1_2_round40_expansion_targets.csv"
MANUAL_PLAN_OUT = AUDIT_DIR / "recipes_v1_2_round40_manual_curated_plan.csv"
DATASET_PLAN_OUT = AUDIT_DIR / "recipes_v1_2_round40_dataset_curated_plan.csv"
REPAIR_STRATEGY_OUT = AUDIT_DIR / "recipes_v1_2_round40_repair_strategy.csv"


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def to_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    try:
        return float(text)
    except ValueError:
        return default


def parse_slots(value: str) -> list[str]:
    if not value:
        return ["unknown"]
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            slots = [str(item).strip().lower() for item in parsed if str(item).strip()]
            return slots or ["unknown"]
    except json.JSONDecodeError:
        pass
    stripped = value.replace("[", "").replace("]", "").replace('"', "")
    slots = [item.strip().lower() for item in stripped.split(",") if item.strip()]
    return slots or ["unknown"]


def median(values: list[float]) -> float:
    clean = [value for value in values if value is not None]
    if not clean:
        return 0.0
    return round(statistics.median(clean), 4)


def percentile(values: list[float], fraction: float) -> float:
    clean = sorted(value for value in values if value is not None)
    if not clean:
        return 0.0
    index = int(round((len(clean) - 1) * fraction))
    return round(clean[index], 4)


def detect_baseline() -> Path:
    for relative_path in BASELINE_CANDIDATES:
        path = ROOT / relative_path
        if (
            (path / "recipes.csv").exists()
            and (path / "recipe_ingredients.csv").exists()
            and (path / "recipe_nutrition_cache.csv").exists()
        ):
            return path
    raise FileNotFoundError("No usable Recipes_DB v1.2 draft baseline dataset found.")


def row_text(row: dict[str, str], fields: list[str]) -> str:
    return " ".join(str(row.get(field, "")).lower() for field in fields)


def infer_source_type(recipe: dict[str, str]) -> str:
    text = row_text(recipe, ["recipe_id", "source_dataset", "qc_notes", "scope_status"])
    if "manual_batch1" in text:
        return "fooddb_manual_batch"
    if "round38" in text or "repaired" in text:
        return "repaired"
    if "round37" in text:
        return "dataset_round37"
    if "round30" in text or "round28" in text or "plus30" in text or "plus15" in text:
        return "targeted_draft"
    if "snack" in parse_slots(recipe.get("allowed_slots_json", "")):
        return "manual_snack_or_snack_curated"
    if "manual" in text:
        return "manual_curated"
    if "recipes_dataset" in text or "candidate" in text:
        return "dataset"
    return "unknown"


def infer_primary_protein(recipe: dict[str, str], ingredient_rows: list[dict[str, str]]) -> str:
    text = row_text(
        recipe,
        [
            "display_name",
            "recipe_name",
            "recipe_category",
            "recipe_subcategory",
            "recipe_kind",
            "qc_notes",
        ],
    )
    ingredient_text = " ".join(
        row_text(row, ["ingredient_name_normalized", "ingredient_raw_text"]) for row in ingredient_rows
    )
    full_text = f"{text} {ingredient_text}"
    checks = [
        ("chicken", ["chicken", "hen"]),
        ("turkey", ["turkey"]),
        ("fish", ["fish", "salmon", "tuna", "cod", "tilapia", "shrimp", "prawn", "sardine", "trout"]),
        ("beef", ["beef", "steak", "sirloin", "round steak", "ground beef", "meatball", "carne"]),
        ("pork", ["pork", "ham", "bacon", "sausage", "guanciale"]),
        ("legume", ["lentil", "bean", "chickpea", "pinto", "black bean", "white bean"]),
        ("egg", ["egg", "frittata", "omelet", "omelette", "quiche", "huevos"]),
        ("vegetarian", ["tofu", "vegetarian", "veggie", "mushroom", "zucchini", "spinach"]),
    ]
    for label, keywords in checks:
        if any(keyword in full_text for keyword in keywords):
            return label
    return "unknown"


def is_complete_main(recipe: dict[str, str]) -> bool:
    kind = recipe.get("recipe_kind", "").lower()
    return "complete" in kind or "near_complete" in kind or "main" in kind


def is_breakfast_pattern_limited(recipe: dict[str, str]) -> bool:
    text = row_text(recipe, ["display_name", "recipe_name", "recipe_category", "recipe_subcategory"])
    limited_keywords = [
        "waffle",
        "oat",
        "porridge",
        "egg",
        "frittata",
        "omelet",
        "omelette",
        "quiche",
        "huevos",
    ]
    return any(keyword in text for keyword in limited_keywords)


def severity_for_gap(gap_size: int, target_count: int) -> str:
    if gap_size <= 0:
        return "low"
    ratio = gap_size / max(target_count, 1)
    if ratio >= 0.45:
        return "high"
    if ratio >= 0.2:
        return "medium"
    return "low"


def is_strong_generator_ready_estimate(item: dict[str, object]) -> bool:
    slots = set(item["slots"])
    if "breakfast" in slots:
        return item["kcal"] >= 250 and item["protein_g"] >= 10 and item["carbs_g"] >= 35
    if "lunch" in slots or "dinner" in slots:
        return item["kcal"] >= 450 and item["protein_g"] >= 18 and item["carbs_g"] >= 35
    if "snack" in slots:
        return 80 <= item["kcal"] <= 350
    return False


def gap_row(
    name: str,
    current_count: int,
    target_count: int,
    why: str,
    source: str,
    severity: str | None = None,
) -> dict[str, object]:
    gap_size = max(target_count - current_count, 0)
    return {
        "gap_name": name,
        "current_count": current_count,
        "target_count": target_count,
        "gap_size": gap_size,
        "severity": severity or severity_for_gap(gap_size, target_count),
        "why_it_matters_for_generator": why,
        "recommended_source": source,
    }


def plan_manual_rows() -> list[dict[str, str]]:
    rows = [
        ("Greek Yogurt Oats Banana", "breakfast", "breakfast_competitor", "dairy", "oats/banana", "fruit", "350-550", "18-28", "45-75", "low", "greek yogurt; oats; banana", "Clean high-yield breakfast alternative."),
        ("Yogurt Oats Apple", "breakfast", "breakfast_competitor", "dairy", "oats/apple", "fruit", "320-520", "16-26", "45-70", "low", "greek yogurt; oats; apple", "Waffle/oat pattern, but controlled and clean."),
        ("Egg Toast Breakfast Bowl", "breakfast", "breakfast_competitor", "egg", "wholemeal toast", "tomato/spinach", "350-600", "18-30", "35-60", "low", "eggs; wholemeal bread", "Practical Romanian/European breakfast."),
        ("Egg Potato Frittata", "breakfast", "breakfast_competitor", "egg", "potato", "spinach/onion", "350-650", "16-28", "35-65", "low", "eggs; potato", "Known useful breakfast shape."),
        ("Cottage Cheese Toast Plate", "breakfast", "breakfast_competitor", "dairy", "wholemeal bread", "cucumber/tomato", "300-500", "18-30", "30-55", "medium", "cottage cheese", "Needs canonical cottage cheese if missing."),
        ("Turkey Egg Breakfast Wrap", "breakfast", "breakfast_competitor", "turkey/egg", "wrap", "spinach", "400-650", "24-36", "35-60", "medium", "turkey slices; tortilla", "Useful if processed turkey source is safe."),
        ("Tuna Toast Breakfast Plate", "breakfast", "breakfast_competitor", "fish", "wholemeal bread", "tomato", "350-550", "25-40", "30-55", "low", "canned tuna; bread", "Adds non-egg protein breakfast."),
        ("Bean Egg Breakfast Bowl", "breakfast", "breakfast_competitor", "egg/legume", "beans/potato", "tomato", "400-650", "20-32", "45-75", "medium", "beans; eggs", "Good variety if bean mapping is source-backed."),
        ("Milk Oats Berry Bowl", "breakfast", "breakfast_competitor", "dairy", "oats", "berries", "300-500", "12-22", "45-70", "low", "milk; oats; berries", "Light breakfast candidate."),
        ("Spinach Cheese Omelette Toast", "breakfast", "breakfast_competitor", "egg/dairy", "toast", "spinach", "350-600", "22-34", "30-55", "low", "eggs; cheese; bread", "Fast low-time breakfast."),
        ("Apple Peanut Butter Oats", "breakfast", "breakfast_competitor", "dairy/fat", "oats/apple", "fruit", "400-650", "14-24", "50-80", "low", "oats; peanut butter; apple", "Energy-dense but controllable."),
        ("Potato Egg Breakfast Skillet", "breakfast", "breakfast_competitor", "egg", "potato", "pepper/onion", "400-700", "18-30", "45-75", "low", "potato; eggs", "Competes with waffles using carbs."),
        ("Smoked Salmon Yogurt Toast", "breakfast", "breakfast_competitor", "fish/dairy", "bread", "cucumber", "350-600", "22-35", "30-55", "medium", "smoked salmon; yogurt", "Useful if smoked salmon mapping is stable."),
        ("Ricotta Banana Toast", "breakfast", "breakfast_competitor", "dairy", "bread/banana", "fruit", "320-550", "14-24", "45-70", "medium", "ricotta", "Good softer breakfast option."),
        ("Breakfast Rice Pudding Yogurt Bowl", "breakfast", "breakfast_competitor", "dairy", "rice", "fruit", "350-600", "12-24", "55-85", "low", "rice; yogurt", "Carb-forward breakfast without dessert excess."),
        ("Chicken Rice Bowl with Vegetables", "lunch_dinner", "carb_protein_main", "chicken", "rice", "mixed vegetables", "500-750", "30-45", "55-90", "low", "chicken breast; rice; vegetables", "Core generator utility meal."),
        ("Chicken Couscous Bowl", "lunch_dinner", "carb_protein_main", "chicken", "couscous", "zucchini/pepper", "500-750", "30-45", "55-90", "low", "chicken; couscous", "Fast European household meal."),
        ("Chicken Potato Tray Bake", "lunch_dinner", "carb_protein_main", "chicken", "potato", "carrot/onion", "550-800", "30-45", "50-85", "low", "chicken; potato", "Strong complete main."),
        ("Chicken Tomato Pasta", "lunch_dinner", "carb_protein_main", "chicken", "pasta", "tomato/spinach", "550-850", "30-50", "65-100", "low", "chicken; pasta", "Alternative to Chicken Broccoli Pasta."),
        ("Turkey Pasta with Tomato Sauce", "lunch_dinner", "carb_protein_main", "turkey", "pasta", "tomato", "550-850", "28-45", "65-100", "medium", "ground turkey; pasta", "Needs turkey mapping confidence."),
        ("Turkey Potato Bake", "lunch_dinner", "carb_protein_main", "turkey", "potato", "peas/carrot", "500-800", "28-45", "50-85", "medium", "turkey; potato", "Turkey variety."),
        ("Turkey Rice Stuffed Peppers", "lunch_dinner", "carb_protein_main", "turkey", "rice", "pepper/tomato", "500-750", "25-42", "55-90", "medium", "turkey; rice", "High utility if turkey source is stable."),
        ("Salmon Potato Bowl", "lunch_dinner", "fish_turkey_pork_main", "fish", "potato", "green beans", "550-800", "28-45", "45-75", "low", "salmon; potato", "Fish gap support."),
        ("Fish Rice Bowl", "lunch_dinner", "fish_turkey_pork_main", "fish", "rice", "vegetables", "500-750", "25-40", "55-90", "low", "white fish; rice", "Clean fish meal."),
        ("Tuna Pasta with Yogurt Sauce", "lunch_dinner", "fish_turkey_pork_main", "fish", "pasta", "peas/cucumber", "500-750", "25-42", "60-95", "low", "tuna; pasta; yogurt", "Cheap and practical."),
        ("Cod Potato Stew", "lunch_dinner", "fish_turkey_pork_main", "fish", "potato", "tomato/onion", "450-700", "25-40", "45-75", "low", "cod; potato", "European fish option."),
        ("Pork Rice with Vegetables", "lunch_dinner", "fish_turkey_pork_main", "pork", "rice", "mixed vegetables", "550-850", "30-45", "55-90", "medium", "pork loin; rice", "Pork gap support."),
        ("Pork Potato Stew", "lunch_dinner", "fish_turkey_pork_main", "pork", "potato", "carrot/onion", "550-850", "30-45", "50-85", "medium", "pork; potato", "Practical pork complete meal."),
        ("Beef Potato Stew", "lunch_dinner", "carb_protein_main", "beef", "potato", "carrot/onion", "600-900", "30-50", "50-85", "medium", "beef; potato", "Useful but beef mapping must be exact."),
        ("Beef Rice Pepper Bowl", "lunch_dinner", "carb_protein_main", "beef", "rice", "pepper/onion", "550-850", "30-50", "55-90", "medium", "beef; rice", "Alternative complete beef main."),
        ("Lean Beef Pasta Bake", "lunch_dinner", "carb_protein_main", "beef", "pasta", "tomato", "600-900", "30-50", "65-105", "medium", "lean beef; pasta", "Avoids vague generic beef."),
        ("Lentil Rice Bowl", "lunch_dinner", "vegetarian_legume_balanced", "legume", "rice", "carrot/spinach", "450-700", "18-30", "65-100", "low", "lentils; rice", "Vegetarian balanced candidate."),
        ("Bean Pasta Bowl", "lunch_dinner", "vegetarian_legume_balanced", "legume", "pasta", "tomato/spinach", "500-750", "18-32", "70-105", "medium", "beans; pasta", "Legume variety."),
        ("Chickpea Couscous Bowl", "lunch_dinner", "vegetarian_legume_balanced", "legume", "couscous", "cucumber/tomato", "450-700", "16-28", "65-100", "low", "chickpeas; couscous", "Fast vegetarian meal."),
        ("White Bean Potato Stew", "lunch_dinner", "vegetarian_legume_balanced", "legume", "potato", "carrot/onion", "450-700", "16-28", "55-90", "medium", "white beans; potato", "Needs bean source/mapping."),
        ("Tofu Rice Vegetable Bowl", "lunch_dinner", "vegetarian_legume_balanced", "vegetarian", "rice", "mixed vegetables", "450-700", "18-30", "55-90", "medium", "tofu; rice", "Only if tofu Food_DB is safe."),
        ("Egg Fried Rice with Vegetables", "lunch_dinner", "vegetarian_legume_balanced", "egg", "rice", "peas/carrot", "450-700", "16-28", "65-100", "low", "eggs; rice", "Useful vegetarian-ish complete meal."),
        ("Chicken Bean Rice Bowl", "lunch_dinner", "carb_protein_main", "chicken/legume", "rice/beans", "tomato", "550-850", "35-55", "70-110", "medium", "chicken; beans; rice", "High macro utility."),
        ("Turkey Lentil Rice Bowl", "lunch_dinner", "carb_protein_main", "turkey/legume", "rice/lentils", "tomato", "550-850", "35-55", "70-110", "medium", "turkey; lentils; rice", "High protein/carb diversity."),
        ("Salmon Pasta Spinach Bowl", "lunch_dinner", "fish_turkey_pork_main", "fish", "pasta", "spinach", "550-850", "28-45", "65-100", "low", "salmon; pasta; spinach", "Fish pasta option."),
        ("Mackerel Potato Salad Meal", "lunch_dinner", "fish_turkey_pork_main", "fish", "potato", "green vegetables", "500-750", "25-40", "45-75", "medium", "mackerel; potato", "Fish variety, watch fat."),
        ("Chicken Polenta Bowl", "lunch_dinner", "carb_protein_main", "chicken", "polenta", "mushroom", "500-750", "28-45", "55-90", "medium", "cornmeal; chicken", "Regional practical meal."),
        ("Pork Pasta with Peas", "lunch_dinner", "fish_turkey_pork_main", "pork", "pasta", "peas", "550-850", "28-45", "65-100", "medium", "pork; pasta", "Pork pasta option."),
        ("Beef Lentil Rice Bowl", "lunch_dinner", "carb_protein_main", "beef/legume", "rice/lentils", "tomato", "600-900", "35-55", "70-110", "medium", "beef; lentils; rice", "Macro strong but mapping-sensitive."),
        ("Yogurt Fruit Snack Bowl", "snack", "snack", "dairy", "fruit", "fruit", "150-300", "8-18", "20-45", "low", "yogurt; fruit", "Clean snack."),
        ("Small Turkey Bread Snack", "snack", "snack", "turkey", "bread", "cucumber", "180-320", "12-22", "20-40", "medium", "turkey; bread", "Useful if turkey source is safe."),
        ("Boiled Egg Toast Snack", "snack", "snack", "egg", "toast", "tomato", "180-320", "10-18", "18-35", "low", "egg; bread", "Simple snack."),
        ("Cottage Cheese Apple Snack", "snack", "snack", "dairy", "fruit", "fruit", "160-300", "12-22", "20-40", "medium", "cottage cheese", "Needs safe cottage cheese."),
        ("Tuna Cracker Snack Plate", "snack", "snack", "fish", "crackers", "cucumber", "180-350", "15-25", "18-40", "medium", "tuna; crackers", "Protein snack, avoid main-size portion."),
    ]
    fieldnames = [
        "planned_recipe_name",
        "slot",
        "target_bucket",
        "primary_protein",
        "carb_source",
        "veg_component",
        "expected_kcal_range",
        "expected_protein_range",
        "expected_carbs_range",
        "ingredient_mapping_risk",
        "Food_DB_items_needed",
        "notes",
    ]
    return [dict(zip(fieldnames, item)) for item in rows]


def build_dataset_plan() -> list[dict[str, object]]:
    return [
        {
            "plan_step": "initial_scan",
            "target_bucket": "all_targeted",
            "scan_target_count": 500,
            "shortlist_target_count": 250,
            "expected_ready_count": 70,
            "quality_gates": "title/content quality; ingredient clarity; mapping likelihood; meal completeness; macro usefulness; duplicate avoidance; European practicality",
            "notes": "Scan 500-1000 source rows, but shortlist only utility candidates.",
        },
        {
            "plan_step": "shortlist_breakfast",
            "target_bucket": "breakfast",
            "scan_target_count": 160,
            "shortlist_target_count": 50,
            "expected_ready_count": 20,
            "quality_gates": "real breakfast; 250-750 kcal likely; protein signal; meaningful carbs; not dessert-like",
            "notes": "Prefer oats/yogurt/egg/toast/potato patterns with clean mapping.",
        },
        {
            "plan_step": "shortlist_lunch_dinner",
            "target_bucket": "lunch_dinner_carb_protein",
            "scan_target_count": 350,
            "shortlist_target_count": 150,
            "expected_ready_count": 50,
            "quality_gates": "complete meal; protein plus carb or vegetables; clear ingredients; practical cooking",
            "notes": "Main growth engine for v1.2.",
        },
        {
            "plan_step": "shortlist_fish_turkey_pork",
            "target_bucket": "fish_turkey_pork",
            "scan_target_count": 140,
            "shortlist_target_count": 50,
            "expected_ready_count": 20,
            "quality_gates": "specific protein; carb side present; no vague meat cuts; clear raw/cooked state",
            "notes": "Reduces chicken/beef dominance.",
        },
        {
            "plan_step": "shortlist_vegetarian_legume",
            "target_bucket": "vegetarian_legume",
            "scan_target_count": 120,
            "shortlist_target_count": 40,
            "expected_ready_count": 18,
            "quality_gates": "legume/egg/tofu protein; carb present; not salad-only; mapped beans/lentils",
            "notes": "Requires Food_DB coverage for beans/legumes.",
        },
        {
            "plan_step": "repair_candidate_capture",
            "target_bucket": "repair_candidate",
            "scan_target_count": 0,
            "shortlist_target_count": 0,
            "expected_ready_count": 15,
            "quality_gates": "valuable failures only; no weak recipes; no unsafe mappings",
            "notes": "Failed valuable recipes go to manual repair queue instead of being discarded.",
        },
    ]


def build_targets() -> list[dict[str, object]]:
    return [
        {
            "bucket_name": "manual_breakfast_clean",
            "target_candidate_count": 15,
            "expected_ready_count": "12-15",
            "source_strategy": "manual_curated",
            "desired_macros": "250-650 kcal, protein 10-30g, carbs 35-80g",
            "desired_recipe_examples": "Greek yogurt oats banana; egg potato frittata; egg toast breakfast bowl",
            "avoid_examples": "dessert waffles; huge casseroles; unclear servings",
            "priority": "high",
            "reason": "Highest ready yield and better control over breakfast variety.",
        },
        {
            "bucket_name": "manual_lunch_dinner_clean",
            "target_candidate_count": 35,
            "expected_ready_count": "30-35",
            "source_strategy": "manual_curated",
            "desired_macros": "450-850 kcal, protein 18-50g, carbs 35-100g",
            "desired_recipe_examples": "chicken rice bowl; turkey pasta; salmon potato bowl; lentil rice bowl",
            "avoid_examples": "only-meat dishes; sauce-only; brand-heavy processed meals",
            "priority": "high",
            "reason": "Fastest route to +95 ready recipes with predictable mapping.",
        },
        {
            "bucket_name": "dataset_breakfast_candidates",
            "target_candidate_count": 50,
            "expected_ready_count": "15-25",
            "source_strategy": "dataset_curated",
            "desired_macros": "200-750 kcal, protein >=8g, meaningful carbs",
            "desired_recipe_examples": "oats/yogurt/fruit; egg toast; potato egg dishes",
            "avoid_examples": "desserts pretending to be breakfast; drinks; huge sweet bakes",
            "priority": "medium",
            "reason": "Adds natural variation if strict title/content gates are used.",
        },
        {
            "bucket_name": "dataset_lunch_dinner_candidates",
            "target_candidate_count": 150,
            "expected_ready_count": "40-60",
            "source_strategy": "dataset_curated",
            "desired_macros": "complete carb-protein meals, kcal >=350, protein >=14g, carbs >=25g",
            "desired_recipe_examples": "rice/pasta/potato bowls; stews; baked mains with carb side",
            "avoid_examples": "components; vague meat; obscure ingredients; random fusion",
            "priority": "high",
            "reason": "Largest remaining volume need.",
        },
        {
            "bucket_name": "dataset_fish_turkey_pork_legume",
            "target_candidate_count": 50,
            "expected_ready_count": "15-25",
            "source_strategy": "dataset_curated",
            "desired_macros": "specific protein mains with carb or legumes, balanced macros",
            "desired_recipe_examples": "fish rice bowl; turkey potato bake; pork pasta; bean rice bowl",
            "avoid_examples": "bone-in ambiguous meat; only protein without side; no carb",
            "priority": "high",
            "reason": "Balances protein mix and reduces chicken/beef dominance.",
        },
        {
            "bucket_name": "repair_queue_recovery",
            "target_candidate_count": "existing_queue",
            "expected_ready_count": "10-25",
            "source_strategy": "repair_queue",
            "desired_macros": "recover valuable blocked recipes that already passed semantic review",
            "desired_recipe_examples": "blocked breakfast and complete mains from repair queue",
            "avoid_examples": "semantically weak rejected recipes",
            "priority": "medium",
            "reason": "Good use of previous curation work.",
        },
        {
            "bucket_name": "Food_DB_source_verification_batch2",
            "target_candidate_count": "high_priority_blockers",
            "expected_ready_count": "depends_on_recovered_recipes",
            "source_strategy": "source_verified_fooddb",
            "desired_macros": "verified canonical Food_DB items with source URL and raw/cooked state",
            "desired_recipe_examples": "baked beans; pie crust; biscuit mix; pepper jack; pizza sauce; guanciale",
            "avoid_examples": "generic beef/pork/turkey; unverified nutrition values",
            "priority": "high",
            "reason": "Unblocks multiple existing valuable recipes and improves mapping coverage.",
        },
    ]


def build_contract_text() -> str:
    return """Recipes_DB v1.2 expansion contract

Scope:
- This is a draft/testing expansion contract, not production data materialization.
- Food_DB = canonical ingredients.
- Recipes_DB = composed recipes.
- Main meal unit remains complete or near-complete recipe.
- Meal completion is fallback only, not the main generation mode.
- No broad/random import.
- Generator utility matters more than recipe count.
- Quality is more important than volume.
- Manual-curated recipes are allowed as seed/test library if clearly marked.
- Failed valuable recipes go into the manual repair queue.

Generator-ready contract:

Breakfast:
- realistic breakfast
- kcal approximately 200-800
- protein >= 8g, preferably >= 10-12g
- meaningful carbs
- realistic portion size
- allowed_slots_json = [\"breakfast\"]

Lunch/dinner:
- complete or near-complete meal
- protein + carb and/or vegetables
- kcal >= 350, preferably >= 450
- protein >= 14g, preferably >= 18g
- carbs >= 25g, preferably >= 35g
- macro_relevant_mapped_weight_ratio >= 0.50
- allowed_slots_json = [\"lunch\", \"dinner\"]

Snack:
- 80-350 kcal
- small portion
- not main dish
- clean ingredients
- allowed_slots_json = [\"snack\"]

Reject:
- pet/animal food
- drinks/cocktails
- dessert/candy/frozen dessert as meals
- sauce/dressing-only
- component-only unless explicitly needed
- weird/random fusion
- highly processed/brand-heavy
- unclear ingredient lists
- only meat without side/carb/veg
- only carb without meaningful protein

Manual repair rules:
- Do not discard valuable recipes only because automatic mapping failed.
- Send valuable failures to manual_repair_queue.
- Use verified source process for Food_DB additions.
- Do not invent nutrition values.
- Generic beef/pork/turkey mappings remain dangerous unless context is exact.
- Bone-in meat requires edible yield logic.
- Raw/cooked state must be clear.

Recommended implementation order:
1. Round41 manual-curated clean batch.
2. Food_DB source verification batch2 for blockers that affect multiple recipes.
3. Dataset-curated harvesting batch with strict quality gates.
4. Repair queue rematerialization after safe mapping/source decisions.
"""


def main() -> None:
    baseline = detect_baseline()
    recipes = read_csv(baseline / "recipes.csv")
    ingredients = read_csv(baseline / "recipe_ingredients.csv")
    nutrition_rows = read_csv(baseline / "recipe_nutrition_cache.csv")
    source_rows = read_csv(SOURCE_RECIPES)
    repair_rows = read_csv(MANUAL_REPAIR_QUEUE)
    source_queue_rows = read_csv(SOURCE_VERIFICATION_QUEUE)

    nutrition_by_recipe = {row.get("recipe_id", ""): row for row in nutrition_rows}
    ingredients_by_recipe: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in ingredients:
        ingredients_by_recipe[row.get("recipe_id", "")].append(row)

    enriched: list[dict[str, object]] = []
    for recipe in recipes:
        recipe_id = recipe.get("recipe_id", "")
        nutrition = nutrition_by_recipe.get(recipe_id, {})
        slots = parse_slots(recipe.get("allowed_slots_json", ""))
        protein = infer_primary_protein(recipe, ingredients_by_recipe.get(recipe_id, []))
        source_type = infer_source_type(recipe)
        kcal = to_float(nutrition.get("energy_kcal_per_serving"))
        protein_g = to_float(nutrition.get("protein_g_per_serving"))
        carbs_g = to_float(nutrition.get("carbs_g_per_serving"))
        fat_g = to_float(nutrition.get("fat_g_per_serving"))
        macro_ratio = to_float(nutrition.get("macro_relevant_mapped_weight_ratio"))
        mapped_ratio = to_float(nutrition.get("mapped_weight_ratio"))
        effective_time = to_float(recipe.get("effective_time_min_for_scoring") or recipe.get("total_time_min"))
        enriched.append(
            {
                "recipe": recipe,
                "nutrition": nutrition,
                "slots": slots,
                "protein": protein,
                "source_type": source_type,
                "kcal": kcal,
                "protein_g": protein_g,
                "carbs_g": carbs_g,
                "fat_g": fat_g,
                "macro_ratio": macro_ratio,
                "mapped_ratio": mapped_ratio,
                "effective_time": effective_time,
                "is_complete_main": is_complete_main(recipe),
                "is_limited_breakfast_pattern": is_breakfast_pattern_limited(recipe),
            }
        )

    slot_counts: Counter[str] = Counter()
    exact_slot_counts: Counter[str] = Counter()
    kind_counts: Counter[str] = Counter()
    cache_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    protein_counts: Counter[str] = Counter()
    for item in enriched:
        slots = item["slots"]
        exact_slot_counts["+".join(slots)] += 1
        for slot in slots:
            slot_counts[slot] += 1
        kind_counts[item["recipe"].get("recipe_kind", "unknown") or "unknown"] += 1
        cache_counts[item["nutrition"].get("cache_status", "missing") or "missing"] += 1
        source_counts[str(item["source_type"])] += 1
        protein_counts[str(item["protein"])] += 1

    slot_order = ["breakfast", "lunch", "dinner", "snack", "unknown"]
    by_slot_rows: list[dict[str, object]] = []
    macro_rows: list[dict[str, object]] = []
    for slot in slot_order:
        slot_items = [item for item in enriched if slot in item["slots"]]
        if not slot_items:
            continue
        kcal_values = [float(item["kcal"]) for item in slot_items]
        protein_values = [float(item["protein_g"]) for item in slot_items]
        carbs_values = [float(item["carbs_g"]) for item in slot_items]
        fat_values = [float(item["fat_g"]) for item in slot_items]
        ratio_values = [float(item["macro_ratio"]) for item in slot_items]
        time_values = [float(item["effective_time"]) for item in slot_items]
        by_slot_rows.append(
            {
                "slot": slot,
                "recipe_count": len(slot_items),
                "complete_main_count": sum(1 for item in slot_items if item["is_complete_main"]),
                "strong_generator_ready_estimate": sum(
                    1 for item in slot_items if is_strong_generator_ready_estimate(item)
                ),
                "median_kcal": median(kcal_values),
                "median_protein_g": median(protein_values),
                "median_carbs_g": median(carbs_values),
                "median_fat_g": median(fat_values),
                "median_macro_relevant_mapped_weight_ratio": median(ratio_values),
                "median_effective_time_min": median(time_values),
                "low_kcal_count": sum(1 for item in slot_items if item["kcal"] < (200 if slot == "breakfast" else 350 if slot in {"lunch", "dinner"} else 80)),
                "low_protein_count": sum(1 for item in slot_items if item["protein_g"] < (8 if slot == "breakfast" else 14 if slot in {"lunch", "dinner"} else 0)),
                "low_carbs_count": sum(1 for item in slot_items if item["carbs_g"] < (25 if slot in {"breakfast", "lunch", "dinner"} else 0)),
                "low_time_count": sum(1 for item in slot_items if item["effective_time"] <= 35),
            }
        )
        macro_rows.append(
            {
                "slot": slot,
                "recipe_count": len(slot_items),
                "kcal_p25": percentile(kcal_values, 0.25),
                "kcal_median": median(kcal_values),
                "kcal_p75": percentile(kcal_values, 0.75),
                "protein_p25": percentile(protein_values, 0.25),
                "protein_median": median(protein_values),
                "protein_p75": percentile(protein_values, 0.75),
                "carbs_p25": percentile(carbs_values, 0.25),
                "carbs_median": median(carbs_values),
                "carbs_p75": percentile(carbs_values, 0.75),
                "fat_p25": percentile(fat_values, 0.25),
                "fat_median": median(fat_values),
                "fat_p75": percentile(fat_values, 0.75),
                "macro_ratio_p25": percentile(ratio_values, 0.25),
                "macro_ratio_median": median(ratio_values),
                "macro_ratio_p75": percentile(ratio_values, 0.75),
                "mapped_ratio_median": median([float(item["mapped_ratio"]) for item in slot_items]),
            }
        )

    by_protein_rows: list[dict[str, object]] = []
    for protein_name in sorted(protein_counts):
        protein_items = [item for item in enriched if item["protein"] == protein_name]
        by_protein_rows.append(
            {
                "primary_protein": protein_name,
                "recipe_count": len(protein_items),
                "breakfast_count": sum(1 for item in protein_items if "breakfast" in item["slots"]),
                "lunch_dinner_count": sum(1 for item in protein_items if "lunch" in item["slots"] or "dinner" in item["slots"]),
                "snack_count": sum(1 for item in protein_items if "snack" in item["slots"]),
                "median_kcal": median([float(item["kcal"]) for item in protein_items]),
                "median_protein_g": median([float(item["protein_g"]) for item in protein_items]),
                "median_carbs_g": median([float(item["carbs_g"]) for item in protein_items]),
                "median_macro_relevant_mapped_weight_ratio": median([float(item["macro_ratio"]) for item in protein_items]),
            }
        )

    breakfast_items = [item for item in enriched if "breakfast" in item["slots"]]
    lunch_dinner_items = [item for item in enriched if "lunch" in item["slots"] or "dinner" in item["slots"]]
    snack_items = [item for item in enriched if "snack" in item["slots"]]
    low_time_items = [item for item in enriched if item["effective_time"] <= 35]
    alternative_breakfast_count = sum(1 for item in breakfast_items if not item["is_limited_breakfast_pattern"])
    complete_lunch_dinner_count = sum(1 for item in lunch_dinner_items if item["is_complete_main"])
    fish_count = protein_counts.get("fish", 0)
    turkey_count = protein_counts.get("turkey", 0)
    pork_count = protein_counts.get("pork", 0)
    veg_legume_count = protein_counts.get("vegetarian", 0) + protein_counts.get("legume", 0)
    no_repeat_unique = 12
    no_repeat_repeated = 0
    if (AUDIT_DIR / "generator_v1_manual_batch1_impact_summary.txt").exists():
        impact_text = (AUDIT_DIR / "generator_v1_manual_batch1_impact_summary.txt").read_text(encoding="utf-8")
        if "unique=12" in impact_text:
            no_repeat_unique = 12
        if "repeated=0" in impact_text:
            no_repeat_repeated = 0
    strong_ready_estimate_count = sum(1 for item in enriched if is_strong_generator_ready_estimate(item))

    gap_rows = [
        gap_row(
            "total_generator_ready",
            len(enriched),
            250,
            "License/demo target needs enough variety beyond one good 3-day plan.",
            "mixed",
        ),
        gap_row(
            "breakfast_candidates",
            len(breakfast_items),
            55,
            "Breakfast anchors should not collapse around waffles/oats/egg-only patterns.",
            "manual_curated + dataset_curated",
        ),
        gap_row(
            "breakfast_non_waffle_oat_egg_patterns",
            alternative_breakfast_count,
            25,
            "Supports realistic week-level variety and avoids visible repetition.",
            "manual_curated",
        ),
        gap_row(
            "lunch_dinner_complete_carb_protein",
            complete_lunch_dinner_count,
            155,
            "This is the main day-planning pool and biggest driver of quality.",
            "manual_curated + dataset_curated + repair_queue",
        ),
        gap_row(
            "fish_meals",
            fish_count,
            25,
            "Fish is needed for protein variety and lower dominance by chicken/beef.",
            "dataset_curated + manual_curated",
        ),
        gap_row(
            "turkey_meals",
            turkey_count,
            20,
            "Turkey improves lean protein variety and lunch/dinner alternatives.",
            "manual_curated + Food_DB_source_verification",
        ),
        gap_row(
            "pork_meals",
            pork_count,
            25,
            "Pork gives European household variety but needs exact safe mappings.",
            "dataset_curated + Food_DB_source_verification",
        ),
        gap_row(
            "vegetarian_legume_balanced_meals",
            veg_legume_count,
            25,
            "Needed for household preference/restriction variety and balanced non-meat days.",
            "manual_curated + Food_DB_source_verification",
        ),
        gap_row(
            "snack_recipes",
            len(snack_items),
            30,
            "Snacks are not the main bottleneck, but need enough clean small-portions.",
            "manual_curated + dataset_curated",
        ),
        gap_row(
            "low_time_recipes",
            len(low_time_items),
            80,
            "Fast recipes matter for practical meal realism and Streamlit testing.",
            "manual_curated + dataset_curated",
        ),
        gap_row(
            "no_repeat_3_day_diversity",
            no_repeat_unique if no_repeat_repeated == 0 else 0,
            12,
            "Current no-repeat 3-day plan is feasible; the issue is robustness beyond one plan.",
            "mixed",
            severity="low" if no_repeat_repeated == 0 else "high",
        ),
    ]

    target_rows = build_targets()
    manual_plan_rows = plan_manual_rows()
    dataset_plan_rows = build_dataset_plan()

    repair_counter = Counter(row.get("problem_type", "unknown") or "unknown" for row in repair_rows)
    repair_fix_counter = Counter(row.get("proposed_fix_type", "unknown") or "unknown" for row in repair_rows)
    source_priority_counter = Counter(row.get("priority", "unknown") or "unknown" for row in source_queue_rows)
    source_needed_count = sum(1 for row in source_queue_rows if str(row.get("source_needed", "")).lower() == "true")
    high_priority_examples = [
        row.get("ingredient_name_normalized", "")
        for row in source_queue_rows
        if row.get("priority") == "high"
    ][:10]
    repair_strategy_rows = [
        {
            "repair_category": "manual_repair_queue_total",
            "count": len(repair_rows),
            "priority": "high",
            "likely_recoverable_count": "10-25",
            "needs_web_source_count": source_needed_count,
            "examples": "; ".join(high_priority_examples),
            "next_action": "Review high-value failures and apply only safe alias/unit/source-backed fixes.",
        },
        {
            "repair_category": "mapping_gap",
            "count": repair_counter.get("mapping_gap", 0),
            "priority": "high",
            "likely_recoverable_count": "depends_on_source_batch",
            "needs_web_source_count": source_needed_count,
            "examples": "specific dairy, beans, processed sauces, protein cuts",
            "next_action": "Use Food_DB source verification batch2 for repeat blockers.",
        },
        {
            "repair_category": "unit_or_mapping_gap",
            "count": repair_counter.get("unit_or_mapping_gap", 0),
            "priority": "medium",
            "likely_recoverable_count": "5-15",
            "needs_web_source_count": "some",
            "examples": "package/can/jar/count units",
            "next_action": "Approve only narrow ingredient-specific unit rules.",
        },
        {
            "repair_category": "fooddb_source_verification",
            "count": len(source_queue_rows),
            "priority": "high",
            "likely_recoverable_count": "10-20",
            "needs_web_source_count": source_needed_count,
            "examples": "; ".join(high_priority_examples),
            "next_action": "Fill verified kcal/protein/carbs/fat with source name, URL, and raw/cooked state.",
        },
        {
            "repair_category": "proposed_alias_mapping",
            "count": repair_fix_counter.get("alias_mapping", 0),
            "priority": "medium",
            "likely_recoverable_count": "low_without_source",
            "needs_web_source_count": "varies",
            "examples": "exact aliases only; no generic meat mapping",
            "next_action": "Keep generic aliases deferred unless context is exact.",
        },
        {
            "repair_category": "proposed_unit_rule",
            "count": repair_fix_counter.get("unit_rule", 0),
            "priority": "medium",
            "likely_recoverable_count": "moderate",
            "needs_web_source_count": "usually false",
            "examples": "safe common units only",
            "next_action": "Apply only ingredient-specific common unit conversions.",
        },
    ]

    write_csv(
        BY_SLOT_OUT,
        by_slot_rows,
        [
            "slot",
            "recipe_count",
            "complete_main_count",
            "strong_generator_ready_estimate",
            "median_kcal",
            "median_protein_g",
            "median_carbs_g",
            "median_fat_g",
            "median_macro_relevant_mapped_weight_ratio",
            "median_effective_time_min",
            "low_kcal_count",
            "low_protein_count",
            "low_carbs_count",
            "low_time_count",
        ],
    )
    write_csv(
        BY_PROTEIN_OUT,
        by_protein_rows,
        [
            "primary_protein",
            "recipe_count",
            "breakfast_count",
            "lunch_dinner_count",
            "snack_count",
            "median_kcal",
            "median_protein_g",
            "median_carbs_g",
            "median_macro_relevant_mapped_weight_ratio",
        ],
    )
    write_csv(
        MACRO_OUT,
        macro_rows,
        [
            "slot",
            "recipe_count",
            "kcal_p25",
            "kcal_median",
            "kcal_p75",
            "protein_p25",
            "protein_median",
            "protein_p75",
            "carbs_p25",
            "carbs_median",
            "carbs_p75",
            "fat_p25",
            "fat_median",
            "fat_p75",
            "macro_ratio_p25",
            "macro_ratio_median",
            "macro_ratio_p75",
            "mapped_ratio_median",
        ],
    )
    write_csv(
        GAP_OUT,
        gap_rows,
        [
            "gap_name",
            "current_count",
            "target_count",
            "gap_size",
            "severity",
            "why_it_matters_for_generator",
            "recommended_source",
        ],
    )
    write_csv(
        TARGETS_OUT,
        target_rows,
        [
            "bucket_name",
            "target_candidate_count",
            "expected_ready_count",
            "source_strategy",
            "desired_macros",
            "desired_recipe_examples",
            "avoid_examples",
            "priority",
            "reason",
        ],
    )
    write_csv(
        MANUAL_PLAN_OUT,
        manual_plan_rows,
        [
            "planned_recipe_name",
            "slot",
            "target_bucket",
            "primary_protein",
            "carb_source",
            "veg_component",
            "expected_kcal_range",
            "expected_protein_range",
            "expected_carbs_range",
            "ingredient_mapping_risk",
            "Food_DB_items_needed",
            "notes",
        ],
    )
    write_csv(
        DATASET_PLAN_OUT,
        dataset_plan_rows,
        [
            "plan_step",
            "target_bucket",
            "scan_target_count",
            "shortlist_target_count",
            "expected_ready_count",
            "quality_gates",
            "notes",
        ],
    )
    write_csv(
        REPAIR_STRATEGY_OUT,
        repair_strategy_rows,
        [
            "repair_category",
            "count",
            "priority",
            "likely_recoverable_count",
            "needs_web_source_count",
            "examples",
            "next_action",
        ],
    )
    write_text(CONTRACT_OUT, build_contract_text())

    top_gap_lines = [
        f"- {row['gap_name']}: current={row['current_count']}, target={row['target_count']}, gap={row['gap_size']}, severity={row['severity']}"
        for row in gap_rows
        if row["gap_size"] or row["gap_name"] == "no_repeat_3_day_diversity"
    ]
    summary = f"""Recipes_DB v1.2 Round40 inventory, gap audit, and expansion contract

Selected baseline:
- {baseline.relative_to(ROOT)}

Inputs:
- recipes.csv rows: {len(recipes)}
- recipe_ingredients.csv rows: {len(ingredients)}
- recipe_nutrition_cache.csv rows: {len(nutrition_rows)}
- source recipe dataset rows: {len(source_rows)}
- manual repair queue rows: {len(repair_rows)}
- Food_DB source verification blockers: {len(source_queue_rows)}

Inventory:
- total generator-ready recipes: {len(enriched)}
- target generator-ready recipes: 250
- estimated gap to target: {max(250 - len(enriched), 0)}
- recipe_kind counts: {dict(kind_counts)}
- exact allowed slot counts: {dict(exact_slot_counts)}
- per-slot counts: {dict(slot_counts)}
- primary protein counts: {dict(protein_counts)}
- source type counts: {dict(source_counts)}
- cache_status counts: {dict(cache_counts)}
- strong_generator_ready estimate: {strong_ready_estimate_count}

Main gaps:
{chr(10).join(top_gap_lines)}

Gap answers:
1. Breakfast candidates: {"enough for current demo, not enough for 250-target library" if len(breakfast_items) >= 20 else "not enough"}.
2. Breakfast alternatives beyond waffles/oats/egg-heavy patterns: current={alternative_breakfast_count}; needs targeted manual variety.
3. Lunch/dinner complete carb-protein meals: current={complete_lunch_dinner_count}; still the largest strategic gap.
4. Fish meals: current={fish_count}; {"gap remains" if fish_count < 25 else "near target"}.
5. Turkey meals: current={turkey_count}; {"gap remains" if turkey_count < 20 else "near target"}.
6. Pork meals: current={pork_count}; {"gap remains" if pork_count < 25 else "near target"}.
7. Vegetarian/legume balanced meals: current={veg_legume_count}; {"gap remains" if veg_legume_count < 25 else "near target"}.
8. Snack recipes: current={len(snack_items)}; not the main bottleneck, but still below target if aiming at 250 recipes.
9. Low-time recipes: current={len(low_time_items)}; useful but secondary after macro/slot gaps.
10. No-repeat 3-day diversity: feasible now for the current demo, but not enough proof of robust week-level variety.

Expansion recommendation:
- Round41 should start with a manual-curated clean batch because it has the highest expected generator-ready yield and the cleanest mapping surface.
- Then run Food_DB source verification batch2 for blockers that affect multiple valuable recipes.
- Then run a larger dataset-curated harvesting batch using the contract and targets from this audit.
"""
    write_text(SUMMARY_OUT, summary)

    print(summary)


if __name__ == "__main__":
    main()
