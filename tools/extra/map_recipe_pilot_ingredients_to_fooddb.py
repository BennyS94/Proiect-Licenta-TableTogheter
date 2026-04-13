import argparse
import csv
import re
import unicodedata
from collections import Counter
from pathlib import Path


DEFAULT_INGREDIENTS = Path("data/recipesdb/draft/recipes_pilot_ingredients_parsed.csv")
DEFAULT_FOODDB = Path("data/fooddb/draft/fooddb_v1_core_master_draft.csv")
DEFAULT_MATCHES_OUT = Path("data/recipesdb/draft/recipe_ingredient_food_matches_draft.csv")
DEFAULT_UNMAPPED_OUT = Path("data/recipesdb/draft/recipe_ingredient_food_unmapped.csv")
DEFAULT_REVIEW_OUT = Path("data/recipesdb/audit/recipe_ingredient_mapping_review.csv")
DEFAULT_SUMMARY_OUT = Path("data/recipesdb/audit/recipe_ingredient_mapping_summary.txt")

SAFE_ALIAS_RULES = {
    "ground black pepper": "Black pepper, powder",
    "freshly ground black pepper": "Black pepper, powder",
    "garlic powder": "Garlic, powder, dried",
    "ground cinnamon": "Cinnamon, powder",
    "dried basil": "Basil, dried",
    "dried oregano": "Oregano, dried",
    "dried thyme": "Thyme, dried",
    "dried parsley": "Parsley, dried",
    "balsamic vinegar": "Vinegar, balsamic",
}

LIGHT_NORMALIZATION_RULES = {
    "minced garlic": "garlic",
    "chopped onion": "onion",
    "chopped fresh parsley": "fresh parsley",
}

SELECTED_FAMILY_PROMOTIONS = {
    "soy sauce": "Soy sauce, prepacked",
    "black pepper": "Black pepper, powder",
    "red onion": "Red onion, raw",
    "yellow onion": "Yellow onion, raw",
    "lemon zest": "Lemon zest, raw",
    "poppy seeds": "Poppy, seed",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Construieste primul mapping draft intre ingredientele parse si Food_DB."
    )
    parser.add_argument("--ingredients", default=str(DEFAULT_INGREDIENTS))
    parser.add_argument("--fooddb", default=str(DEFAULT_FOODDB))
    parser.add_argument("--out-matches", default=str(DEFAULT_MATCHES_OUT))
    parser.add_argument("--out-unmapped", default=str(DEFAULT_UNMAPPED_OUT))
    parser.add_argument("--out-review", default=str(DEFAULT_REVIEW_OUT))
    parser.add_argument("--out-summary", default=str(DEFAULT_SUMMARY_OUT))
    return parser.parse_args()


def read_csv_rows(path):
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def clean_text(value):
    return str(value or "").strip()


def exact_key(value):
    return clean_text(value).casefold()


def normalize_match_text(value):
    text = clean_text(value).casefold()
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = text.replace("&", " and ")
    text = text.replace("_", " ")
    text = text.replace("-", " ")
    text = text.replace("/", " ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def singularize_normalized_text(value):
    text = normalize_match_text(value)
    if not text:
        return ""
    words = text.split()
    if not words:
        return ""

    last = words[-1]
    if len(last) <= 3:
        return text
    if last.endswith("ies") and len(last) > 4:
        words[-1] = last[:-3] + "y"
    elif last.endswith("es") and len(last) > 4:
        words[-1] = last[:-2]
    elif last.endswith("s") and not last.endswith("ss"):
        words[-1] = last[:-1]
    return " ".join(word for word in words if word)


def dedupe_food_rows(rows):
    unique_rows = []
    seen_ids = set()
    for row in rows:
        food_id = clean_text(row.get("food_id"))
        if food_id and food_id not in seen_ids:
            unique_rows.append(row)
            seen_ids.add(food_id)
    return unique_rows


def build_index(food_rows, column_name, key_fn):
    index = {}
    for row in food_rows:
        value = clean_text(row.get(column_name))
        if not value:
            continue
        key = key_fn(value)
        if not key:
            continue
        index.setdefault(key, []).append(row)
    return index


def build_food_indexes(food_rows):
    return {
        "canonical_exact": build_index(food_rows, "canonical_name", exact_key),
        "display_exact": build_index(food_rows, "display_name", exact_key),
        "family_exact": build_index(food_rows, "food_family_name", exact_key),
        "canonical_normalized": build_index(food_rows, "canonical_name", normalize_match_text),
        "display_normalized": build_index(food_rows, "display_name", normalize_match_text),
        "family_normalized": build_index(food_rows, "food_family_name", normalize_match_text),
    }


def get_unique_display_target(food_indexes, display_name):
    candidate_rows = dedupe_food_rows(food_indexes["display_exact"].get(exact_key(display_name), []))
    if len(candidate_rows) == 1:
        return candidate_rows[0]
    return None


def prepare_candidate_for_matching(candidate_text):
    working_candidate = clean_text(candidate_text)
    candidate_notes = []

    normalized_working = normalize_match_text(working_candidate)
    normalization_target = LIGHT_NORMALIZATION_RULES.get(normalized_working, "")
    if normalization_target:
        working_candidate = normalization_target
        candidate_notes.append(
            f"light_normalization_rule:{normalized_working}->{normalize_match_text(normalization_target)}"
        )
        normalized_working = normalize_match_text(working_candidate)

    alias_target_display = SAFE_ALIAS_RULES.get(normalized_working, "")
    family_promotion_target = SELECTED_FAMILY_PROMOTIONS.get(normalized_working, "")

    return {
        "candidate_for_matching": working_candidate,
        "normalized_candidate": normalized_working,
        "candidate_notes": candidate_notes,
        "alias_target_display": alias_target_display,
        "family_promotion_target": family_promotion_target,
    }


def make_empty_match_result():
    return {
        "matched_food_id": "",
        "matched_canonical_name": "",
        "matched_display_name": "",
        "matched_food_family_name": "",
        "match_method": "no_match",
        "match_confidence": "low",
        "mapping_status": "unmapped",
        "mapping_notes": "",
        "candidate_count": "0",
        "candidate_food_ids_preview": "",
    }


def build_match_result(food_row, match_method, match_confidence, mapping_status, notes, candidate_count):
    candidate_ids = clean_text(food_row.get("food_id"))
    return {
        "matched_food_id": clean_text(food_row.get("food_id")),
        "matched_canonical_name": clean_text(food_row.get("canonical_name")),
        "matched_display_name": clean_text(food_row.get("display_name")),
        "matched_food_family_name": clean_text(food_row.get("food_family_name")),
        "match_method": match_method,
        "match_confidence": match_confidence,
        "mapping_status": mapping_status,
        "mapping_notes": "; ".join(dict.fromkeys(note for note in notes if note)),
        "candidate_count": str(candidate_count),
        "candidate_food_ids_preview": candidate_ids,
    }


def build_review_without_selection(match_method, match_confidence, notes, candidate_rows):
    preview = ",".join(clean_text(row.get("food_id")) for row in candidate_rows[:5])
    return {
        "matched_food_id": "",
        "matched_canonical_name": "",
        "matched_display_name": "",
        "matched_food_family_name": "",
        "match_method": match_method,
        "match_confidence": match_confidence,
        "mapping_status": "review_needed",
        "mapping_notes": "; ".join(dict.fromkeys(note for note in notes if note)),
        "candidate_count": str(len(candidate_rows)),
        "candidate_food_ids_preview": preview,
    }


def classify_mapping_result(parse_status, match_result, default_no_match_status):
    if parse_status == "review_needed":
        match_result["mapping_status"] = "review_needed"
        notes = [note.strip() for note in match_result["mapping_notes"].split(";") if note.strip()]
        if "parse_status_review_gate" not in notes:
            notes.append("parse_status_review_gate")
        match_result["mapping_notes"] = "; ".join(notes)
        return match_result

    if match_result["match_method"] == "no_match":
        match_result["mapping_status"] = default_no_match_status
        return match_result

    return match_result


def attempt_match(ingredient_row, food_indexes):
    parse_status = clean_text(ingredient_row.get("parse_status"))
    candidate_text = clean_text(ingredient_row.get("food_name_candidate"))

    no_match_default = "review_needed" if parse_status == "review_needed" else "unmapped"
    base_result = make_empty_match_result()

    if not candidate_text:
        base_result["mapping_status"] = no_match_default
        base_result["mapping_notes"] = "missing_food_name_candidate"
        return base_result

    prepared_candidate = prepare_candidate_for_matching(candidate_text)
    candidate_for_matching = prepared_candidate["candidate_for_matching"]
    candidate_notes = prepared_candidate["candidate_notes"]
    exact_candidate = exact_key(candidate_for_matching)
    normalized_candidate = prepared_candidate["normalized_candidate"]
    singular_family_candidate = singularize_normalized_text(candidate_for_matching)

    alias_target_row = None
    if prepared_candidate["alias_target_display"]:
        alias_target_row = get_unique_display_target(food_indexes, prepared_candidate["alias_target_display"])
        if alias_target_row:
            alias_result = build_match_result(
                food_row=alias_target_row,
                match_method="normalized_name_match",
                match_confidence="high",
                mapping_status="accepted_auto",
                notes=candidate_notes + [f"safe_alias_rule:{normalized_candidate}->{normalize_match_text(prepared_candidate['alias_target_display'])}"],
                candidate_count=1,
            )
            return classify_mapping_result(parse_status, alias_result, no_match_default)

    promotion_target_row = None
    if prepared_candidate["family_promotion_target"]:
        promotion_target_row = get_unique_display_target(food_indexes, prepared_candidate["family_promotion_target"])
        if promotion_target_row:
            promotion_method = "exact_family_name"
            if normalized_candidate != normalize_match_text(clean_text(promotion_target_row.get("food_family_name"))):
                promotion_method = "family_fallback"
            promotion_result = build_match_result(
                food_row=promotion_target_row,
                match_method=promotion_method,
                match_confidence="medium",
                mapping_status="accepted_auto",
                notes=candidate_notes + [f"selected_family_promotion_rule:{normalized_candidate}->{normalize_match_text(prepared_candidate['family_promotion_target'])}"],
                candidate_count=1,
            )
            return classify_mapping_result(parse_status, promotion_result, no_match_default)

    match_attempts = [
        (
            "exact_canonical_name",
            "high",
            True,
            food_indexes["canonical_exact"].get(exact_candidate, []),
            "exact canonical_name match",
        ),
        (
            "exact_display_name",
            "high",
            True,
            food_indexes["display_exact"].get(exact_candidate, []),
            "exact display_name match",
        ),
        (
            "exact_family_name",
            "medium",
            False,
            food_indexes["family_exact"].get(exact_candidate, []),
            "exact food_family_name match",
        ),
        (
            "normalized_name_match",
            "medium",
            True,
            food_indexes["canonical_normalized"].get(normalized_candidate, []),
            "normalized canonical_name match",
        ),
        (
            "normalized_name_match",
            "medium",
            True,
            food_indexes["display_normalized"].get(normalized_candidate, []),
            "normalized display_name match",
        ),
        (
            "family_fallback",
            "low",
            False,
            food_indexes["family_normalized"].get(normalized_candidate, []),
            "normalized food_family_name fallback",
        ),
        (
            "family_fallback",
            "low",
            False,
            food_indexes["family_normalized"].get(singular_family_candidate, []),
            "singularized food_family_name fallback",
        ),
    ]

    for match_method, confidence, auto_allowed, candidate_rows, note in match_attempts:
        unique_rows = dedupe_food_rows(candidate_rows)
        if not unique_rows:
            continue

        if len(unique_rows) == 1:
            notes = candidate_notes + [note]
            status = "accepted_auto" if auto_allowed else "review_needed"
            result = build_match_result(
                food_row=unique_rows[0],
                match_method=match_method,
                match_confidence=confidence,
                mapping_status=status,
                notes=notes,
                candidate_count=len(unique_rows),
            )
            return classify_mapping_result(parse_status, result, no_match_default)

        notes = candidate_notes + [f"{note}; ambiguous_candidate_set"]
        return classify_mapping_result(
            parse_status,
            build_review_without_selection(
                match_method=match_method,
                match_confidence=confidence,
                notes=notes,
                candidate_rows=unique_rows,
            ),
            no_match_default,
        )

    base_result["mapping_status"] = no_match_default
    base_result["mapping_notes"] = "; ".join(candidate_notes + ["no_exact_or_normalized_match"])
    return base_result


def main():
    args = parse_args()

    ingredients_path = Path(args.ingredients)
    fooddb_path = Path(args.fooddb)
    matches_out = Path(args.out_matches)
    unmapped_out = Path(args.out_unmapped)
    review_out = Path(args.out_review)
    summary_out = Path(args.out_summary)

    ingredient_rows = read_csv_rows(ingredients_path)
    food_rows = read_csv_rows(fooddb_path)
    food_indexes = build_food_indexes(food_rows)

    matches_rows = []
    review_rows = []
    unmapped_rows = []

    mapping_status_counts = Counter()
    match_method_counts = Counter()
    mapping_note_counts = Counter()

    fieldnames = list(ingredient_rows[0].keys()) + [
        "matched_food_id",
        "matched_canonical_name",
        "matched_display_name",
        "matched_food_family_name",
        "match_method",
        "match_confidence",
        "mapping_status",
        "mapping_notes",
        "candidate_count",
        "candidate_food_ids_preview",
    ]

    for ingredient_row in ingredient_rows:
        match_result = attempt_match(ingredient_row, food_indexes)
        output_row = dict(ingredient_row)
        output_row.update(match_result)
        matches_rows.append(output_row)

        mapping_status = output_row["mapping_status"]
        mapping_status_counts[mapping_status] += 1
        match_method_counts[output_row["match_method"]] += 1

        notes = [note.strip() for note in output_row["mapping_notes"].split(";") if note.strip()]
        for note in notes:
            mapping_note_counts[note] += 1

        if mapping_status == "review_needed":
            review_rows.append(output_row)
        elif mapping_status == "unmapped":
            unmapped_rows.append(output_row)

    write_csv(matches_out, matches_rows, fieldnames)
    write_csv(unmapped_out, unmapped_rows, fieldnames)
    write_csv(review_out, review_rows, fieldnames)

    summary_lines = [
        "Recipe ingredient to Food_DB mapping summary",
        "",
        f"Total ingredient rows considered: {len(matches_rows)}",
        "",
        "Mapping status counts:",
    ]

    for status, count in mapping_status_counts.most_common():
        summary_lines.append(f"- {status}: {count}")

    summary_lines.extend(
        [
            "",
            "Match method counts:",
        ]
    )

    for method, count in match_method_counts.most_common():
        summary_lines.append(f"- {method}: {count}")

    summary_lines.extend(
        [
            "",
            "Top mapping notes:",
        ]
    )

    for note, count in mapping_note_counts.most_common(20):
        summary_lines.append(f"- {note}: {count}")

    summary_out.parent.mkdir(parents=True, exist_ok=True)
    summary_out.write_text("\n".join(summary_lines) + "\n", encoding="utf-8")

    print(f"Total ingredient rows considered: {len(matches_rows)}")
    print("Mapping status counts:")
    for status, count in mapping_status_counts.most_common():
        print(f" - {status}: {count}")
    print("Match method counts:")
    for method, count in match_method_counts.most_common():
        print(f" - {method}: {count}")
    print(f"Written: {matches_out}")
    print(f"Written: {unmapped_out}")
    print(f"Written: {review_out}")
    print(f"Written: {summary_out}")


if __name__ == "__main__":
    main()
