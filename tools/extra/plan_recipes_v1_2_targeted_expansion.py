from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
RECIPES_DRAFT_DIR = REPO_ROOT / "data" / "recipesdb" / "draft"
AUDIT_DIR = REPO_ROOT / "data" / "recipesdb" / "audit"

CURRENT_DATASET_DIR = RECIPES_DRAFT_DIR / "v1_2_generator_ready_plus30_plus15"
CURRENT_RECIPES_PATH = CURRENT_DATASET_DIR / "recipes.csv"
CURRENT_NUTRITION_PATH = CURRENT_DATASET_DIR / "recipe_nutrition_cache.csv"
REPAIR_QUEUE_PATH = RECIPES_DRAFT_DIR / "recipes_v1_2_manual_repair_queue.csv"
ROUND34_SUMMARY_PATH = AUDIT_DIR / "generator_v1_round34_direct_builder_performance_summary.txt"

PLAN_PATH = AUDIT_DIR / "recipes_v1_2_targeted_expansion_plan.txt"
TARGETS_PATH = AUDIT_DIR / "recipes_v1_2_targeted_expansion_targets.csv"
CURRENT_STATE_PATH = AUDIT_DIR / "recipes_v1_2_expansion_current_state_summary.txt"

TARGET_COLUMNS = [
    "bucket_name",
    "target_candidate_count",
    "expected_ready_count",
    "reason",
    "desired_macro_profile",
    "desired_examples",
    "avoid_examples",
    "likely_fooddb_gaps",
    "likely_unit_rules_needed",
    "priority",
]


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    recipes = read_csv(CURRENT_RECIPES_PATH)
    nutrition = read_csv(CURRENT_NUTRITION_PATH)
    repair_queue = read_csv(REPAIR_QUEUE_PATH)
    targets = expansion_targets()

    write_targets(targets)
    write_plan(targets, recipes, repair_queue)
    write_current_state(recipes, nutrition, repair_queue)

    print("Recipes_DB v1.2 targeted expansion plan written")
    print(f"- current generator-ready recipes: {len(recipes)}")
    print(f"- repair queue rows: {len(repair_queue)}")
    print(f"- target candidate count: {sum(int(row['target_candidate_count']) for row in targets)}")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_targets(rows: list[dict[str, str]]) -> None:
    with TARGETS_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=TARGET_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def expansion_targets() -> list[dict[str, str]]:
    return [
        {
            "bucket_name": "breakfast_competitors",
            "target_candidate_count": "30",
            "expected_ready_count": "15-20",
            "reason": "Breakfast variety is still strategically important because Waffles was a past anchor and breakfast alternatives need comparable carbs plus useful protein.",
            "desired_macro_profile": "300-700 kcal; protein >= 10-12g; meaningful carbs; not dessert-heavy; practical portion.",
            "desired_examples": "oats with dairy and fruit; egg with potato or toast; frittata or omelette with carb side; non-dessert pancakes or waffles; yogurt-oat breakfast bowls.",
            "avoid_examples": "desserts pretending to be breakfast; drink-only recipes; huge casseroles; snack-only items; 900+ kcal portions.",
            "likely_fooddb_gaps": "hash browns; cooked ham; breakfast cheeses; tortillas; yogurt variants; cooked oats variants.",
            "likely_unit_rules_needed": "cups shredded cheese; slices bread or cheese; small potatoes; cups cooked oats; count eggs.",
            "priority": "high",
        },
        {
            "bucket_name": "lunch_dinner_complete_carb_protein",
            "target_candidate_count": "50",
            "expected_ready_count": "25-35",
            "reason": "Multi-day quality depends on complete mains that can compete with Chicken and Broccoli Pasta, Thai Basil Chicken, Stuffed Cabbage, and Veggie Burgers.",
            "desired_macro_profile": "500-900 kcal; protein >= 25g; carbs >= 50g; carb plus protein plus veg when possible.",
            "desired_examples": "chicken rice bowls; chicken pasta; turkey pasta or rice; beef potato meals; fish with rice or potatoes; bean or lentil rice complete meals.",
            "avoid_examples": "only meat; only carb; sauces; dressings; component-only sides; very processed casseroles; obscure fusion recipes.",
            "likely_fooddb_gaps": "specific pasta shapes; cooked ham; smoked salmon; tuna variants; pork cuts; tortillas; canned beans; cooked rice states.",
            "likely_unit_rules_needed": "cups cooked rice or pasta; cans beans or tuna; fillets; chops; ounces cooked meat; packages noodles.",
            "priority": "high",
        },
        {
            "bucket_name": "fish_turkey_pork_specific_mains",
            "target_candidate_count": "10",
            "expected_ready_count": "5-8",
            "reason": "Protein diversity is thin; fish, turkey, and pork complete meals reduce repeated chicken-heavy anchors.",
            "desired_macro_profile": "450-850 kcal; protein >= 25g; carbs >= 35-50g; veg or carb side included.",
            "desired_examples": "salmon pasta; tuna rice bowls; turkey ziti or turkey rice; pork chops with potatoes; fish with potatoes and vegetables.",
            "avoid_examples": "fish-only fillets without carb; pork-only plates; smoked/brand-heavy processed meals; recipes with unclear serving sizes.",
            "likely_fooddb_gaps": "smoked salmon; pork chops; turkey mince variants; fish fillet forms; canned tuna drained weights.",
            "likely_unit_rules_needed": "fillet count; chop count; ounces cooked meat; cans drained fish; cups cooked pasta.",
            "priority": "high",
        },
        {
            "bucket_name": "vegetarian_legume_balanced_mains",
            "target_candidate_count": "10",
            "expected_ready_count": "5-7",
            "reason": "Vegetarian and legume mains can reduce reliance on Veggie Burgers if they are complete and protein-balanced.",
            "desired_macro_profile": "450-800 kcal; protein >= 18-25g; carbs >= 45g; beans, lentils, eggs, dairy, or tofu-like protein if Food_DB coverage exists.",
            "desired_examples": "lentil rice bowls; bean stuffed peppers; bean pasta; egg and potato mains; legume stews with rice.",
            "avoid_examples": "plain salads; vegetable sides; low-protein pasta; soup-only meals without substantial protein or carb.",
            "likely_fooddb_gaps": "lentil cooked weights; bean can drained weights; specific cheeses; tofu only if canonical Food_DB item exists.",
            "likely_unit_rules_needed": "cans beans; cups cooked lentils; cups cooked rice; portions cooked pasta.",
            "priority": "medium_high",
        },
    ]


def write_plan(targets: list[dict[str, str]], recipes: list[dict[str, str]], repair_queue: list[dict[str, str]]) -> None:
    total_candidates = sum(int(row["target_candidate_count"]) for row in targets)
    ready_expectation = "50-70"
    repair_counts = Counter(row.get("target_bucket", "") for row in repair_queue)

    lines = [
        "Recipes_DB v1.2 targeted expansion plan",
        "",
        "Strategic position",
        "- Generator v1 should not be made more complex before the data improves.",
        "- The next serious workstream is targeted Recipes_DB expansion plus manual repair.",
        "- Valuable recipes that fail automatic mapping should move through repair, not be discarded immediately.",
        "- Food_DB remains canonical ingredients only; Recipes_DB remains composed recipes.",
        "",
        "Recommended expansion target",
        f"- Candidate intake: +{total_candidates} targeted candidates.",
        f"- Expected generator-ready after repair: {ready_expectation}.",
        "- Candidate source should be targeted, not broad/random import.",
        "- Manual-curated recipes are allowed when source data is weak, but nutrition must come from mapped Food_DB ingredients.",
        "",
        "Target buckets",
    ]
    for row in targets:
        lines.extend(
            [
                f"- {row['bucket_name']}: {row['target_candidate_count']} candidates, expected ready {row['expected_ready_count']}.",
                f"  reason: {row['reason']}",
                f"  desired: {row['desired_examples']}",
                f"  avoid: {row['avoid_examples']}",
            ]
        )

    lines.extend(
        [
            "",
            "Current repair queue signal",
            *format_counter(repair_counts),
            "",
            "Operating rules",
            "- Do not add production rows directly.",
            "- Do not force unsafe Food_DB mappings.",
            "- Add alias/unit/Food_DB fixes through approved manual repair rows.",
            "- Reject semantically weak recipes even if they parse cleanly.",
            "- Prefer fewer high-quality generator-ready recipes over a larger weak draft.",
            "",
            "Next recommended step",
            "- Review high-priority repair queue rows first.",
            "- Approve safe alias and unit-rule fixes in a controlled repair round.",
            "- In parallel, select the next +100 targeted candidates using the bucket split above.",
        ]
    )
    PLAN_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_current_state(
    recipes: list[dict[str, str]],
    nutrition: list[dict[str, str]],
    repair_queue: list[dict[str, str]],
) -> None:
    round34_text = ROUND34_SUMMARY_PATH.read_text(encoding="utf-8") if ROUND34_SUMMARY_PATH.exists() else ""
    repair_problem_counts = Counter(row.get("problem_type", "") for row in repair_queue)
    repair_fix_counts = Counter(row.get("proposed_fix_type", "") for row in repair_queue)

    lines = [
        "Recipes_DB v1.2 expansion current state",
        "",
        "- current best dataset profile: v1_2_generator_ready_plus30_plus15",
        f"- current recipe count: {len(recipes)}",
        f"- current nutrition cache rows: {len(nutrition)}",
        f"- manual repair queue rows: {len(repair_queue)}",
        "",
        "Round32/Round34 multi-day status",
        "- Round32 proved 3-day no-repeat is feasible with a broader candidate pool.",
        "- Round34 replaced repeated balanced selector calls with direct_from_slots for test speed.",
        "- Best practical setting from Round34: direct_from_slots shortlist=12.",
        extract_round34_direct_shortlist_line(round34_text),
        extract_round34_line(round34_text, "Selected plan"),
        "",
        "Remaining issues",
        "- Dataset diversity is still the main product-quality risk.",
        "- Dominant anchors can reappear when the candidate set is small or source recipes are thin.",
        "- Useful failed recipes need manual alias, unit, serving, or Food_DB repair.",
        "- Runtime improved enough for testing, but it is not instant.",
        "",
        "Manual repair queue problem counts",
        *format_counter(repair_problem_counts),
        "",
        "Manual repair proposed fix counts",
        *format_counter(repair_fix_counts),
        "",
        "Recommended next action",
        "- Use manual repair queue plus targeted expansion.",
        "- Do not invest in more selector complexity until Recipes_DB/Food_DB coverage improves.",
    ]
    CURRENT_STATE_PATH.write_text("\n".join(line for line in lines if line is not None) + "\n", encoding="utf-8")


def extract_round34_line(text: str, marker: str) -> str | None:
    if not text:
        return None
    for line in text.splitlines():
        if marker.lower() in line.lower():
            return f"- Round34 {line.strip().lstrip('- ').strip()}"
    return None


def extract_round34_direct_shortlist_line(text: str) -> str | None:
    if not text:
        return None
    for line in text.splitlines():
        if "direct_from_slots_shortlist_12" in line:
            return f"- Round34 {line.strip().lstrip('- ').strip()}"
    return None


def format_counter(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key or 'blank'}: {value}" for key, value in counter.most_common()]


if __name__ == "__main__":
    main()
