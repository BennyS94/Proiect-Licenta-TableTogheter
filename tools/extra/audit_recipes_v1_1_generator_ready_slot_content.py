from __future__ import annotations

import csv
import json
import re
from collections import Counter
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]

RECIPES_PATH = REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_generator_ready" / "recipes.csv"
NUTRITION_PATH = (
    REPO_ROOT / "data" / "recipesdb" / "draft" / "v1_1_generator_ready" / "recipe_nutrition_cache.csv"
)
READINESS_PATH = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_round10_generator_readiness.csv"
)
CURATED_PATH = REPO_ROOT / "data" / "recipesdb" / "draft" / "recipes_v1_1_curated_200.csv"

OUT_AUDIT = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_generator_ready_slot_content_audit.csv"
)
OUT_SUMMARY = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_generator_ready_slot_content_summary.txt"
)
OUT_EXCLUSIONS = (
    REPO_ROOT / "data" / "recipesdb" / "audit" / "recipes_v1_1_generator_ready_content_exclusions.csv"
)

AUDIT_COLUMNS = [
    "recipe_id",
    "display_name",
    "recipe_kind",
    "recipe_category",
    "recipe_subcategory",
    "primary_protein",
    "content_quality_status",
    "content_exclusion_reason",
    "allowed_slots_json",
    "slot_policy_reason",
    "is_pet_food",
    "is_dessert_like",
    "is_drink",
    "is_breakfast_only",
    "is_snack_only",
    "is_main_meal",
    "is_component_or_side",
    "slot_policy_confidence",
]

PET_PATTERNS = [
    "dog biscuit",
    "dog biscuits",
    "dog treat",
    "dog treats",
    "cat food",
    "pet food",
    "pet treat",
]
DRINK_PATTERNS = [
    "cocktail",
    "smoothie",
    "drink",
    "beverage",
    "lemonade",
    "punch",
    "shake",
]
DESSERT_PATTERNS = [
    "cake",
    "cupcake",
    "cookie",
    "cookies",
    "candy",
    "dessert",
    "brownie",
    "pie",
    "pudding",
    "ice cream",
    "sorbet",
    "fudge",
]
BREAKFAST_PATTERNS = [
    "breakfast",
    "waffle",
    "waffles",
    "pancake",
    "pancakes",
    "omelet",
    "omelet",
    "omelette",
    "frittata",
    "oatmeal",
    "oats",
    "yogurt bowl",
    "french toast",
    "hash browns",
]
SNACK_PATTERNS = [
    "hummus",
    "deviled egg",
    "deviled eggs",
    "chickpeas",
    "puffed butter beans",
    "snack",
]
MAIN_PATTERNS = [
    "stuffed pepper",
    "stuffed peppers",
    "stuffed cabbage",
    "stew",
    "stir fry",
    "casserole",
    "lasagna",
    "rice",
    "rice bowl",
    "meatloaf",
    "chili",
    "curry",
    "gnocchi",
    "ravioli",
    "burger",
    "sandwich",
    "fried rice",
    "noodle",
    "noodles",
    "linguini",
    "short ribs",
    "skewers",
    "soup",
    "salad",
]
COMPONENT_KINDS = {"protein_component", "carb_side", "veg_side", "component"}
MAIN_KINDS = {"complete_main", "near_complete_main", "soup", "salad"}


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


def normalize_text(value: object) -> str:
    text = clean_text(value).lower().replace("_", " ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def contains_phrase(text: str, phrase: str) -> bool:
    normalized_phrase = normalize_text(phrase)
    if not normalized_phrase:
        return False
    pattern = rf"(?<![a-z0-9]){re.escape(normalized_phrase)}(?![a-z0-9])"
    return re.search(pattern, text) is not None


def has_any(text: str, phrases: list[str]) -> bool:
    return any(contains_phrase(text, phrase) for phrase in phrases)


def index_by_recipe_id(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    output: dict[str, dict[str, str]] = {}
    for row in rows:
        recipe_id = clean_text(row.get("recipe_id") or row.get("recipe_id_candidate"))
        if recipe_id:
            output[recipe_id] = row
    return output


def classify_recipe(
    recipe: dict[str, str],
    readiness_by_id: dict[str, dict[str, str]],
    curated_by_id: dict[str, dict[str, str]],
) -> dict[str, object]:
    recipe_id = clean_text(recipe.get("recipe_id"))
    readiness = readiness_by_id.get(recipe_id, {})
    curated = curated_by_id.get(recipe_id, {})
    display_name = clean_text(recipe.get("display_name"))
    recipe_kind = clean_text(recipe.get("recipe_kind") or curated.get("recipe_kind_guess"))
    category = clean_text(recipe.get("recipe_category") or curated.get("source_category"))
    subcategory = clean_text(recipe.get("recipe_subcategory") or curated.get("source_subcategory"))
    primary_protein = clean_text(readiness.get("primary_protein") or curated.get("primary_protein"))
    search_text = normalize_text(
        " ".join(
            [
                display_name,
                recipe_kind,
                category,
                subcategory,
                clean_text(recipe.get("qc_notes")),
            ]
        )
    )

    is_pet_food = has_any(search_text, PET_PATTERNS)
    is_drink = has_any(search_text, DRINK_PATTERNS) or "drink" in normalize_text(category)
    is_breakfast_only = recipe_kind == "breakfast" or has_any(search_text, BREAKFAST_PATTERNS)
    is_snack_only = recipe_kind == "snack" or has_any(search_text, SNACK_PATTERNS)
    is_dessert_like = has_any(search_text, DESSERT_PATTERNS)
    is_component_or_side = recipe_kind in COMPONENT_KINDS
    is_main_meal = recipe_kind in MAIN_KINDS or has_any(search_text, MAIN_PATTERNS)
    is_appetizer_conflict = (
        recipe_kind != "snack"
        and not is_pet_food
        and ("appetizer" in normalize_text(category) or "appetizer" in normalize_text(subcategory))
    )

    status = "keep"
    exclusion_reason = ""
    allowed_slots: list[str] = []
    slot_reason = ""
    confidence = "high"

    if is_pet_food:
        status = "exclude"
        exclusion_reason = "pet_or_animal_food"
        slot_reason = "pet_food_not_for_generator_meals"
    elif is_drink:
        status = "exclude"
        exclusion_reason = "drink_or_cocktail"
        slot_reason = "drinks_are_out_of_scope_for_meal_slots"
    elif is_dessert_like and not (is_breakfast_only or is_snack_only):
        status = "exclude"
        exclusion_reason = "dessert_not_current_generator_meal"
        slot_reason = "dessert_like_recipe_not_useful_for_current_generator"
    elif is_appetizer_conflict:
        status = "review"
        exclusion_reason = "appetizer_or_snack_category_conflicts_with_main_kind"
        slot_reason = "category_suggests_snack_but_kind_suggests_main"
        confidence = "medium"
    elif is_component_or_side:
        status = "review"
        exclusion_reason = "component_or_side_not_full_meal"
        slot_reason = "components_and_sides_not_selectable_as_full_meals"
        confidence = "medium"
    elif is_breakfast_only:
        allowed_slots = ["breakfast"]
        slot_reason = "breakfast_kind_or_breakfast_title"
    elif is_snack_only:
        allowed_slots = ["snack"]
        slot_reason = "snack_kind_or_snack_title"
    elif recipe_kind in {"complete_main", "near_complete_main", "soup", "salad"}:
        allowed_slots = ["lunch", "dinner"]
        slot_reason = f"{recipe_kind}_restricted_to_lunch_dinner"
    else:
        status = "review"
        exclusion_reason = "unclear_slot_policy"
        slot_reason = "recipe_kind_not_supported_by_slot_policy"
        confidence = "low"

    if status != "keep":
        allowed_slots = []

    return {
        "recipe_id": recipe_id,
        "display_name": display_name,
        "recipe_kind": recipe_kind,
        "recipe_category": category,
        "recipe_subcategory": subcategory,
        "primary_protein": primary_protein,
        "content_quality_status": status,
        "content_exclusion_reason": exclusion_reason,
        "allowed_slots_json": json.dumps(allowed_slots),
        "slot_policy_reason": slot_reason,
        "is_pet_food": str(is_pet_food),
        "is_dessert_like": str(is_dessert_like),
        "is_drink": str(is_drink),
        "is_breakfast_only": str(is_breakfast_only),
        "is_snack_only": str(is_snack_only),
        "is_main_meal": str(is_main_meal),
        "is_component_or_side": str(is_component_or_side),
        "slot_policy_confidence": confidence,
    }


def counter_text(counter: Counter[str]) -> list[str]:
    if not counter:
        return ["- none"]
    return [f"- {key}: {value}" for key, value in counter.most_common()]


def build_summary(rows: list[dict[str, object]]) -> str:
    status_counts = Counter(clean_text(row.get("content_quality_status")) for row in rows)
    slot_counts: Counter[str] = Counter()
    empty_slots: list[dict[str, object]] = []
    exclusions = [
        row
        for row in rows
        if clean_text(row.get("content_quality_status")) in {"review", "exclude"}
    ]
    for row in rows:
        allowed_slots = json.loads(clean_text(row.get("allowed_slots_json")) or "[]")
        if not allowed_slots:
            empty_slots.append(row)
        for slot in allowed_slots:
            slot_counts[str(slot)] += 1

    safe_for_testing = status_counts.get("exclude", 0) == 0 and status_counts.get("review", 0) == 0
    lines = [
        "Recipes_DB v1.1 generator-ready slot/content audit",
        "=" * 58,
        "",
        f"total_recipes_audited: {len(rows)}",
        "",
        "Content quality counts:",
    ]
    lines.extend(counter_text(status_counts))
    lines.extend(["", "Allowed slot counts:"])
    lines.extend(counter_text(slot_counts))
    lines.extend(
        [
            "",
            f"recipes_with_empty_allowed_slots: {len(empty_slots)}",
            "",
            "Excluded/review titles and reasons:",
        ]
    )
    if exclusions:
        for row in exclusions:
            lines.append(
                "- "
                + clean_text(row.get("recipe_id"))
                + " | "
                + clean_text(row.get("display_name"))
                + " | "
                + clean_text(row.get("content_quality_status"))
                + " | "
                + clean_text(row.get("content_exclusion_reason"))
            )
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "Generator v1 testing assessment:",
            f"- raw_subset_safe_without_slot_checked_materialization: {str(safe_for_testing).lower()}",
            "- slot_checked_subset_safe_for_testing: true, if only keep rows are materialized and allowed_slots_json is enforced",
        ]
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    recipes, _ = read_csv(RECIPES_PATH)
    _, _ = read_csv(NUTRITION_PATH)
    readiness, _ = read_csv(READINESS_PATH)
    curated, _ = read_csv(CURATED_PATH)

    readiness_by_id = index_by_recipe_id(readiness)
    curated_by_id = index_by_recipe_id(curated)
    audit_rows = [classify_recipe(row, readiness_by_id, curated_by_id) for row in recipes]
    exclusion_rows = [
        row
        for row in audit_rows
        if clean_text(row.get("content_quality_status")) in {"review", "exclude"}
    ]

    write_csv(OUT_AUDIT, audit_rows, AUDIT_COLUMNS)
    write_csv(OUT_EXCLUSIONS, exclusion_rows, AUDIT_COLUMNS)
    OUT_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    OUT_SUMMARY.write_text(build_summary(audit_rows), encoding="utf-8")

    status_counts = Counter(clean_text(row.get("content_quality_status")) for row in audit_rows)
    print("Recipes_DB v1.1 generator-ready slot/content audit written")
    print(f"total_recipes_audited={len(audit_rows)}")
    print(f"keep={status_counts.get('keep', 0)}")
    print(f"review={status_counts.get('review', 0)}")
    print(f"exclude={status_counts.get('exclude', 0)}")
    print(f"written_audit={OUT_AUDIT}")
    print(f"written_summary={OUT_SUMMARY}")
    print(f"written_exclusions={OUT_EXCLUSIONS}")


if __name__ == "__main__":
    main()
