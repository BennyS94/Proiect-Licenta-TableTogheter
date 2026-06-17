from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any

from src.generator_v1.feedback_adapter import build_household_preference_context


DIETARY_PREFERENCE_DEFAULTS = {
    "vegetarian": False,
    "vegan": False,
    "gluten_free": False,
    "no_beef": False,
    "no_pork": False,
    "no_chicken": False,
    "no_fish": False,
    "no_dairy": False,
}

FOOD_PREFERENCE_RATINGS = {"like", "dislike", "avoid"}

HEALTH_AND_DIET_DEFAULTS = {
    "dietary_patterns": {
        "keto": False,
        "paleo": False,
        "mediterranean": False,
    },
    "health_modes": {
        "diabetes_aware": False,
        "hypertension_friendly": False,
        "heart_friendly": False,
    },
}


def save_generated_plan(
    conn: sqlite3.Connection,
    plan_id: str,
    household_id: str,
    member_profile_id: str | None,
    generation_type: str,
    dataset_profile: str,
    days: int,
    request_json: dict[str, Any],
    response_json: dict[str, Any],
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO generated_plans (
            plan_id,
            household_id,
            member_profile_id,
            generation_type,
            dataset_profile,
            days,
            request_json,
            response_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            plan_id,
            household_id,
            member_profile_id,
            generation_type,
            dataset_profile,
            int(days or 1),
            _json_dumps(request_json),
            _json_dumps(response_json),
            _utc_now_iso(),
        ),
    )


def save_plan_days(
    conn: sqlite3.Connection,
    plan_id: str,
    response_json: dict[str, Any],
) -> None:
    conn.execute("DELETE FROM generated_plan_days WHERE plan_id = ?", (plan_id,))
    for day in _daily_plan_rows(response_json):
        totals = day.get("totals") if isinstance(day.get("totals"), dict) else {}
        conn.execute(
            """
            INSERT INTO generated_plan_days (
                plan_day_id,
                plan_id,
                day_index,
                kcal_total,
                protein_total,
                carbs_total,
                fat_total,
                validation_status,
                quality_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _new_id("plan_day"),
                plan_id,
                int(_to_float(day.get("day_index")) or 1),
                _to_float(totals.get("kcal")),
                _to_float(totals.get("protein_g")),
                _to_float(totals.get("carbs_g")),
                _to_float(totals.get("fat_g")),
                _clean_text(day.get("validation_status")),
                _clean_text(day.get("quality_status")),
            ),
        )


def save_plan_meals(
    conn: sqlite3.Connection,
    plan_id: str,
    response_json: dict[str, Any],
) -> None:
    conn.execute("DELETE FROM generated_plan_meals WHERE plan_id = ?", (plan_id,))
    for day in _daily_plan_rows(response_json):
        day_index = int(_to_float(day.get("day_index")) or 1)
        meals = day.get("selected_meals", [])
        if not isinstance(meals, list):
            continue
        for meal in meals:
            if not isinstance(meal, dict):
                continue
            recipe_id = _clean_text(meal.get("recipe_id"))
            display_name = _clean_text(meal.get("display_name")) or recipe_id
            slot = _clean_text(meal.get("slot"))
            if not recipe_id or not slot:
                continue
            conn.execute(
                """
                INSERT INTO generated_plan_meals (
                    plan_meal_id,
                    plan_id,
                    day_index,
                    slot,
                    recipe_id,
                    display_name,
                    portion_multiplier,
                    kcal,
                    protein_g,
                    carbs_g,
                    fat_g,
                    meal_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _new_id("plan_meal"),
                    plan_id,
                    day_index,
                    slot,
                    recipe_id,
                    display_name,
                    _to_float(meal.get("portion_multiplier")),
                    _to_float(meal.get("kcal")),
                    _to_float(meal.get("protein_g")),
                    _to_float(meal.get("carbs_g")),
                    _to_float(meal.get("fat_g")),
                    _json_dumps(meal),
                ),
            )


def save_grocery_list(
    conn: sqlite3.Connection,
    plan_id: str,
    household_id: str,
    grocery_json: dict[str, Any] | None,
) -> str | None:
    if not grocery_json:
        return None

    conn.execute("DELETE FROM grocery_list_items WHERE grocery_list_id IN (SELECT grocery_list_id FROM grocery_lists WHERE plan_id = ?)", (plan_id,))
    conn.execute("DELETE FROM grocery_lists WHERE plan_id = ?", (plan_id,))

    summary = grocery_json.get("summary") if isinstance(grocery_json.get("summary"), dict) else {}
    grocery_list_id = _clean_text(grocery_json.get("grocery_list_id")) or _new_id("grocery")
    conn.execute(
        """
        INSERT INTO grocery_lists (
            grocery_list_id,
            plan_id,
            household_id,
            total_estimated_cost,
            currency,
            grocery_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            grocery_list_id,
            plan_id,
            household_id,
            _to_float(grocery_json.get("total_estimated_cost") or summary.get("estimated_total_cost")),
            _clean_text(grocery_json.get("currency") or summary.get("estimated_total_currency")),
            _json_dumps(grocery_json),
            _utc_now_iso(),
        ),
    )

    items = grocery_json.get("items", [])
    if not isinstance(items, list):
        items = []
    for item in items:
        if not isinstance(item, dict):
            continue
        display_name = _clean_text(item.get("display_name")) or _clean_text(
            item.get("canonical_name")
        )
        if not display_name:
            continue
        conn.execute(
            """
            INSERT INTO grocery_list_items (
                grocery_item_id,
                grocery_list_id,
                display_name,
                category,
                needed_grams,
                purchase_display,
                estimated_cost,
                currency,
                item_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                _new_id("grocery_item"),
                grocery_list_id,
                display_name,
                _clean_text(item.get("category") or item.get("grocery_category")),
                _to_float(
                    item.get("needed_grams")
                    or item.get("display_grams_numeric")
                    or item.get("total_grams")
                ),
                _clean_text(item.get("purchase_display")),
                _to_float(item.get("estimated_cost")),
                _clean_text(item.get("currency") or grocery_json.get("currency")),
                _json_dumps(item),
            ),
        )
    return grocery_list_id


def get_generated_plan(
    conn: sqlite3.Connection,
    plan_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT response_json
        FROM generated_plans
        WHERE plan_id = ?
        """,
        (plan_id,),
    ).fetchone()
    if row is None:
        return None
    return json.loads(row[0])


def get_generated_plan_record(
    conn: sqlite3.Connection,
    plan_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            plan_id,
            household_id,
            member_profile_id,
            generation_type,
            dataset_profile,
            days,
            request_json,
            response_json,
            created_at
        FROM generated_plans
        WHERE plan_id = ?
        """,
        (plan_id,),
    ).fetchone()
    if row is None:
        return None
    return {
        "plan_id": row[0],
        "household_id": row[1],
        "member_profile_id": row[2],
        "generation_type": row[3],
        "dataset_profile": row[4],
        "days": row[5],
        "request_json": json.loads(row[6]),
        "response_json": json.loads(row[7]),
        "created_at": row[8],
    }


def get_grocery_list_by_plan_id(
    conn: sqlite3.Connection,
    plan_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT grocery_json
        FROM grocery_lists
        WHERE plan_id = ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (plan_id,),
    ).fetchone()
    if row is None:
        return None
    return json.loads(row[0])


def save_member_profile(
    conn: sqlite3.Connection,
    profile_dict: dict[str, Any],
) -> dict[str, Any]:
    now = _utc_now_iso()
    member_profile_id = _clean_text(profile_dict.get("member_profile_id")) or _new_id(
        "member_profile"
    )
    existing = conn.execute(
        "SELECT created_at FROM member_profiles WHERE member_profile_id = ?",
        (member_profile_id,),
    ).fetchone()
    created_at = existing[0] if existing else now
    saved = {
        "member_profile_id": member_profile_id,
        "household_id": _clean_text(profile_dict.get("household_id")),
        "display_name": _clean_text(
            profile_dict.get("display_name") or profile_dict.get("profile_name")
        ),
        "age": int(profile_dict.get("age") or 0),
        "sex": _clean_text(profile_dict.get("sex")),
        "weight_kg": float(profile_dict.get("weight_kg") or 0.0),
        "height_cm": float(profile_dict.get("height_cm") or 0.0),
        "activity_level": _clean_text(profile_dict.get("activity_level")),
        "goal": _clean_text(profile_dict.get("goal")),
        "goal_speed": _clean_text(profile_dict.get("goal_speed")),
        "training": dict(profile_dict.get("training") or {}),
        "meal_config": dict(profile_dict.get("meal_config") or {}),
        "dietary_preferences": _normalized_dietary_preferences(
            profile_dict.get("dietary_preferences")
        ),
        "food_preferences": _normalized_food_preferences(
            profile_dict.get("food_preferences")
        ),
        "health_and_diet_preferences": _normalized_health_and_diet_preferences(
            profile_dict.get("health_and_diet_preferences")
        ),
        "bf_profile": _clean_text(profile_dict.get("bf_profile")) or "normal",
        "is_active": bool(profile_dict.get("is_active", True)),
        "created_at": created_at,
        "updated_at": now,
    }
    conn.execute(
        """
        INSERT OR REPLACE INTO member_profiles (
            member_profile_id,
            household_id,
            display_name,
            age,
            sex,
            weight_kg,
            height_cm,
            activity_level,
            goal,
            goal_speed,
            training_json,
            meal_config_json,
            dietary_preferences_json,
            food_preferences_json,
            health_and_diet_preferences_json,
            is_active,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            saved["member_profile_id"],
            saved["household_id"],
            saved["display_name"],
            saved["age"],
            saved["sex"],
            saved["weight_kg"],
            saved["height_cm"],
            saved["activity_level"],
            saved["goal"],
            saved["goal_speed"],
            _json_dumps(saved["training"]),
            _json_dumps(saved["meal_config"]),
            _json_dumps(saved["dietary_preferences"]),
            _json_dumps(saved["food_preferences"]),
            _json_dumps(saved["health_and_diet_preferences"]),
            1 if saved["is_active"] else 0,
            saved["created_at"],
            saved["updated_at"],
        ),
    )
    return saved


def list_member_profiles(
    conn: sqlite3.Connection,
    household_id: str | None = None,
    active_only: bool = True,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if _clean_text(household_id):
        clauses.append("household_id = ?")
        params.append(_clean_text(household_id))
    if active_only:
        clauses.append("is_active = 1")
    where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    rows = conn.execute(
        f"""
        SELECT
            member_profile_id,
            household_id,
            display_name,
            age,
            sex,
            weight_kg,
            height_cm,
            activity_level,
            goal,
            goal_speed,
            training_json,
            meal_config_json,
            dietary_preferences_json,
            food_preferences_json,
            health_and_diet_preferences_json,
            is_active,
            created_at,
            updated_at
        FROM member_profiles
        {where_sql}
        ORDER BY created_at ASC, display_name ASC
        """,
        params,
    ).fetchall()
    return [_profile_row_to_dict(row) for row in rows]


def get_member_profile(
    conn: sqlite3.Connection,
    member_profile_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            member_profile_id,
            household_id,
            display_name,
            age,
            sex,
            weight_kg,
            height_cm,
            activity_level,
            goal,
            goal_speed,
            training_json,
            meal_config_json,
            dietary_preferences_json,
            food_preferences_json,
            health_and_diet_preferences_json,
            is_active,
            created_at,
            updated_at
        FROM member_profiles
        WHERE member_profile_id = ?
        """,
        (_clean_text(member_profile_id),),
    ).fetchone()
    if row is None:
        return None
    return _profile_row_to_dict(row)


def deactivate_member_profile(
    conn: sqlite3.Connection,
    member_profile_id: str,
) -> dict[str, Any] | None:
    profile_id = _clean_text(member_profile_id)
    if not profile_id:
        return None

    existing = get_member_profile(conn, profile_id)
    if existing is None:
        return None

    conn.execute(
        """
        UPDATE member_profiles
        SET is_active = 0,
            updated_at = ?
        WHERE member_profile_id = ?
        """,
        (
            _utc_now_iso(),
            profile_id,
        ),
    )
    return get_member_profile(conn, profile_id)


def get_member_profile_for_generation(
    conn: sqlite3.Connection,
    member_profile_id: str,
) -> dict[str, Any] | None:
    profile = get_member_profile(conn, member_profile_id)
    if profile is None or not profile.get("is_active", True):
        return None
    return _profile_for_generation(profile)


def list_active_member_profiles_for_household(
    conn: sqlite3.Connection,
    household_id: str,
) -> list[dict[str, Any]]:
    return list_member_profiles(
        conn,
        household_id=household_id,
        active_only=True,
    )


def build_household_profile_from_db(
    conn: sqlite3.Connection,
    household_id: str,
    selected_member_ids: list[str] | None = None,
) -> dict[str, Any] | None:
    household_id_clean = _clean_text(household_id)
    if not household_id_clean:
        return None

    selected_ids = {_clean_text(item) for item in selected_member_ids or [] if _clean_text(item)}
    profiles = list_active_member_profiles_for_household(conn, household_id_clean)
    members = []
    for profile in profiles:
        member_id = _clean_text(profile.get("member_profile_id"))
        if selected_ids and member_id not in selected_ids:
            continue
        members.append(_member_row_for_household(profile))

    if not members:
        return None

    return {
        "household_id": household_id_clean,
        "household_name": household_id_clean,
        "active_member_ids": [member["member_id"] for member in members],
        "members": members,
        "household_preferences": {
            "shared_meal_preference": True,
            "max_unique_dinners_per_day": 1,
            "cook_once_share_meal": True,
        },
        "meal_config": {
            "meals_per_day": 3,
            "include_snacks": True,
            "day_structure": "3_meals_plus_snack",
        },
        "planning_config": {
            "shared_meals": ["lunch", "dinner"],
            "breakfast_mode": "flexible",
            "snack_mode": "individual",
            "portion_allocation_mode": "proportional_to_energy_target",
            "portion_multiplier_min": 0.4,
            "portion_multiplier_max": 1.8,
        },
        "notes": {
            "source": "sqlite_member_profiles",
            "scope": "backend_m5_mvp",
        },
    }


def save_feedback_event(
    conn: sqlite3.Connection,
    event_dict: dict[str, Any],
) -> dict[str, Any]:
    now = _utc_now_iso()
    event = {
        "event_id": _clean_text(event_dict.get("event_id")) or _new_id("feedback_event"),
        "household_id": _clean_text(event_dict.get("household_id")),
        "member_profile_id": _clean_text(event_dict.get("member_profile_id")),
        "recipe_id": _clean_text(event_dict.get("recipe_id")),
        "plan_id": _clean_text(event_dict.get("plan_id")),
        "slot": _clean_text(event_dict.get("slot")),
        "feedback_type": _clean_text(event_dict.get("feedback_type")),
        "notes": _clean_text(event_dict.get("notes")),
        "source": _clean_text(event_dict.get("source")) or "api",
        "created_at": _clean_text(event_dict.get("created_at")) or now,
    }
    conn.execute(
        """
        INSERT OR REPLACE INTO feedback_events (
            event_id,
            household_id,
            member_profile_id,
            recipe_id,
            plan_id,
            slot,
            feedback_type,
            notes,
            source,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event["event_id"],
            event["household_id"],
            event["member_profile_id"],
            event["recipe_id"],
            event["plan_id"],
            event["slot"],
            event["feedback_type"],
            event["notes"],
            event["source"],
            event["created_at"],
        ),
    )
    return event


def list_feedback_events(
    conn: sqlite3.Connection,
    household_id: str | None = None,
    member_profile_id: str | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if _clean_text(household_id):
        clauses.append("household_id = ?")
        params.append(_clean_text(household_id))
    if _clean_text(member_profile_id):
        clauses.append("(member_profile_id = ? OR member_profile_id = '')")
        params.append(_clean_text(member_profile_id))
    where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    rows = conn.execute(
        f"""
        SELECT
            event_id,
            household_id,
            member_profile_id,
            recipe_id,
            plan_id,
            slot,
            feedback_type,
            notes,
            source,
            created_at
        FROM feedback_events
        {where_sql}
        ORDER BY created_at ASC
        """,
        params,
    ).fetchall()
    return [_feedback_row_to_dict(row) for row in rows]


def delete_feedback_events(
    conn: sqlite3.Connection,
    household_id: str | None = None,
    member_profile_id: str | None = None,
    event_id: str | None = None,
    recipe_id: str | None = None,
    plan_id: str | None = None,
    slot: str | None = None,
    feedback_type: str | None = None,
) -> int:
    clauses: list[str] = []
    params: list[Any] = []
    if _clean_text(event_id):
        clauses.append("event_id = ?")
        params.append(_clean_text(event_id))
    if _clean_text(household_id):
        clauses.append("household_id = ?")
        params.append(_clean_text(household_id))
    if _clean_text(member_profile_id):
        clauses.append("member_profile_id = ?")
        params.append(_clean_text(member_profile_id))
    if _clean_text(recipe_id):
        clauses.append("recipe_id = ?")
        params.append(_clean_text(recipe_id))
    if _clean_text(plan_id):
        clauses.append("plan_id = ?")
        params.append(_clean_text(plan_id))
    if _clean_text(slot):
        clauses.append("slot = ?")
        params.append(_clean_text(slot))
    if _clean_text(feedback_type):
        clauses.append("feedback_type = ?")
        params.append(_clean_text(feedback_type))
    where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    cursor = conn.execute(f"DELETE FROM feedback_events{where_sql}", params)
    return int(cursor.rowcount or 0)


def build_feedback_context_from_db(
    conn: sqlite3.Connection,
    household_id: str | None = None,
    member_profile_id: str | None = None,
) -> dict[str, Any]:
    events = list_feedback_events(
        conn,
        household_id=household_id,
        member_profile_id=member_profile_id,
    )
    context = build_household_preference_context(
        events=events,
        household_id=_clean_text(household_id),
        member_profile_id=_clean_text(member_profile_id),
        dataset_profile=None,
    )
    meta = context.get("meta") if isinstance(context.get("meta"), dict) else {}
    return {
        "household_id": _clean_text(household_id),
        "member_profile_id": _clean_text(member_profile_id),
        "event_count": int(meta.get("event_count") or 0),
        "hard_filters": context.get("hard_filters", {}),
        "score_preferences": context.get("score_preferences", {}),
        "time_preferences": context.get("time_preferences", {}),
        "meta": meta,
    }


def save_daily_progress_snapshot(
    conn: sqlite3.Connection,
    snapshot_dict: dict[str, Any],
    *,
    max_snapshots_per_profile: int = 30,
) -> tuple[dict[str, Any], bool]:
    household_id = _clean_text(snapshot_dict.get("household_id"))
    member_profile_id = _clean_text(snapshot_dict.get("member_profile_id"))
    plan_id = _clean_text(snapshot_dict.get("plan_id"))
    day_index = int(_to_float(snapshot_dict.get("day_index")) or 0)
    existing = get_daily_progress_by_context(
        conn,
        member_profile_id=member_profile_id,
        plan_id=plan_id,
        day_index=day_index,
    )
    if existing is not None:
        return existing, False

    now = _utc_now_iso()
    planned = snapshot_dict.get("planned")
    if not isinstance(planned, dict):
        planned = {}
    target = snapshot_dict.get("target")
    if not isinstance(target, dict):
        target = planned
    consumed = snapshot_dict.get("consumed")
    if not isinstance(consumed, dict):
        consumed = {}
    meal_completion = snapshot_dict.get("meal_completion")
    if not isinstance(meal_completion, dict):
        meal_completion = {}
    day_snapshot = snapshot_dict.get("day_snapshot")
    if not isinstance(day_snapshot, dict):
        day_snapshot = {}

    progress_id = _clean_text(snapshot_dict.get("progress_id")) or _new_id(
        "daily_progress"
    )
    saved_at = _clean_text(snapshot_dict.get("saved_at")) or now
    conn.execute(
        """
        INSERT INTO saved_daily_progress (
            progress_id,
            household_id,
            member_profile_id,
            plan_id,
            day_index,
            saved_at,
            planned_kcal,
            planned_protein_g,
            planned_carbs_g,
            planned_fat_g,
            target_kcal,
            target_protein_g,
            target_carbs_g,
            target_fat_g,
            consumed_kcal,
            consumed_protein_g,
            consumed_carbs_g,
            consumed_fat_g,
            meal_completion_json,
            day_snapshot_json,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            progress_id,
            household_id,
            member_profile_id,
            plan_id,
            day_index,
            saved_at,
            _to_float(planned.get("kcal")),
            _to_float(planned.get("protein_g")),
            _to_float(planned.get("carbs_g")),
            _to_float(planned.get("fat_g")),
            _to_float(target.get("kcal") or planned.get("kcal")),
            _to_float(target.get("protein_g") or planned.get("protein_g")),
            _to_float(target.get("carbs_g") or planned.get("carbs_g")),
            _to_float(target.get("fat_g") or planned.get("fat_g")),
            _to_float(consumed.get("kcal")),
            _to_float(consumed.get("protein_g")),
            _to_float(consumed.get("carbs_g")),
            _to_float(consumed.get("fat_g")),
            _json_dumps(meal_completion),
            _json_dumps(day_snapshot),
            now,
            now,
        ),
    )
    _enforce_daily_progress_profile_limit(
        conn,
        member_profile_id=member_profile_id,
        max_snapshots=max_snapshots_per_profile,
    )
    saved = get_daily_progress_snapshot(conn, progress_id)
    if saved is None:
        raise RuntimeError("daily_progress_insert_failed")
    return saved, True


def get_daily_progress_by_context(
    conn: sqlite3.Connection,
    *,
    member_profile_id: str,
    plan_id: str,
    day_index: int,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            progress_id,
            household_id,
            member_profile_id,
            plan_id,
            day_index,
            saved_at,
            planned_kcal,
            planned_protein_g,
            planned_carbs_g,
            planned_fat_g,
            target_kcal,
            target_protein_g,
            target_carbs_g,
            target_fat_g,
            consumed_kcal,
            consumed_protein_g,
            consumed_carbs_g,
            consumed_fat_g,
            meal_completion_json,
            day_snapshot_json,
            created_at,
            updated_at
        FROM saved_daily_progress
        WHERE member_profile_id = ?
          AND plan_id = ?
          AND day_index = ?
        LIMIT 1
        """,
        (_clean_text(member_profile_id), _clean_text(plan_id), int(day_index or 0)),
    ).fetchone()
    return _daily_progress_row_to_dict(row) if row else None


def get_daily_progress_snapshot(
    conn: sqlite3.Connection,
    progress_id: str,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            progress_id,
            household_id,
            member_profile_id,
            plan_id,
            day_index,
            saved_at,
            planned_kcal,
            planned_protein_g,
            planned_carbs_g,
            planned_fat_g,
            target_kcal,
            target_protein_g,
            target_carbs_g,
            target_fat_g,
            consumed_kcal,
            consumed_protein_g,
            consumed_carbs_g,
            consumed_fat_g,
            meal_completion_json,
            day_snapshot_json,
            created_at,
            updated_at
        FROM saved_daily_progress
        WHERE progress_id = ?
        LIMIT 1
        """,
        (_clean_text(progress_id),),
    ).fetchone()
    return _daily_progress_row_to_dict(row) if row else None


def list_daily_progress_snapshots(
    conn: sqlite3.Connection,
    *,
    household_id: str,
    member_profile_id: str,
    limit: int = 30,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT
            progress_id,
            household_id,
            member_profile_id,
            plan_id,
            day_index,
            saved_at,
            planned_kcal,
            planned_protein_g,
            planned_carbs_g,
            planned_fat_g,
            target_kcal,
            target_protein_g,
            target_carbs_g,
            target_fat_g,
            consumed_kcal,
            consumed_protein_g,
            consumed_carbs_g,
            consumed_fat_g,
            meal_completion_json,
            day_snapshot_json,
            created_at,
            updated_at
        FROM saved_daily_progress
        WHERE household_id = ?
          AND member_profile_id = ?
        ORDER BY saved_at DESC, created_at DESC
        LIMIT ?
        """,
        (
            _clean_text(household_id),
            _clean_text(member_profile_id),
            int(limit or 30),
        ),
    ).fetchall()
    return [_daily_progress_row_to_dict(row) for row in rows]


def delete_daily_progress_snapshot(
    conn: sqlite3.Connection,
    progress_id: str,
) -> dict[str, Any] | None:
    existing = get_daily_progress_snapshot(conn, progress_id)
    if existing is None:
        return None
    conn.execute(
        "DELETE FROM saved_daily_progress WHERE progress_id = ?",
        (_clean_text(progress_id),),
    )
    return existing


def _enforce_daily_progress_profile_limit(
    conn: sqlite3.Connection,
    *,
    member_profile_id: str,
    max_snapshots: int,
) -> None:
    if max_snapshots <= 0:
        return
    rows = conn.execute(
        """
        SELECT progress_id
        FROM saved_daily_progress
        WHERE member_profile_id = ?
        ORDER BY saved_at DESC, created_at DESC, rowid DESC
        """,
        (_clean_text(member_profile_id),),
    ).fetchall()
    stale_ids = [row[0] for row in rows[max_snapshots:]]
    if not stale_ids:
        return
    placeholders = ",".join("?" for _ in stale_ids)
    conn.execute(
        f"DELETE FROM saved_daily_progress WHERE progress_id IN ({placeholders})",
        stale_ids,
    )


def _daily_plan_rows(response_json: dict[str, Any]) -> list[dict[str, Any]]:
    rows = response_json.get("daily_plan")
    if isinstance(rows, list) and rows:
        return [row for row in rows if isinstance(row, dict)]
    generator_plan = response_json.get("generator_plan")
    days = generator_plan.get("days") if isinstance(generator_plan, dict) else None
    if isinstance(days, list):
        return [day for day in days if isinstance(day, dict)]
    return []


def _profile_row_to_dict(row: Any) -> dict[str, Any]:
    return {
        "member_profile_id": row[0],
        "household_id": row[1],
        "display_name": row[2],
        "age": int(row[3]),
        "sex": row[4],
        "weight_kg": float(row[5]),
        "height_cm": float(row[6]),
        "activity_level": row[7],
        "goal": row[8],
        "goal_speed": row[9],
        "training": _json_loads(row[10]),
        "meal_config": _json_loads(row[11]),
        "dietary_preferences": _normalized_dietary_preferences(_json_loads(row[12])),
        "food_preferences": _normalized_food_preferences(_json_loads(row[13])),
        "health_and_diet_preferences": _normalized_health_and_diet_preferences(
            _json_loads(row[14])
        ),
        "bf_profile": "normal",
        "is_active": bool(row[15]),
        "created_at": row[16],
        "updated_at": row[17],
    }


def _profile_for_generation(profile: dict[str, Any]) -> dict[str, Any]:
    return {
        "member_profile_id": _clean_text(profile.get("member_profile_id")),
        "household_id": _clean_text(profile.get("household_id")),
        "display_name": _clean_text(profile.get("display_name")),
        "profile_name": _clean_text(profile.get("display_name")),
        "age": int(profile.get("age") or 0),
        "sex": _clean_text(profile.get("sex")),
        "weight_kg": float(profile.get("weight_kg") or 0.0),
        "height_cm": float(profile.get("height_cm") or 0.0),
        "activity_level": _clean_text(profile.get("activity_level")),
        "goal": _clean_text(profile.get("goal")),
        "goal_speed": _clean_text(profile.get("goal_speed")),
        "training": dict(profile.get("training") or {}),
        "meal_config": dict(profile.get("meal_config") or {}),
        "dietary_preferences": _normalized_dietary_preferences(
            profile.get("dietary_preferences")
        ),
        "food_preferences": _normalized_food_preferences(profile.get("food_preferences")),
        "health_and_diet_preferences": _normalized_health_and_diet_preferences(
            profile.get("health_and_diet_preferences")
        ),
        "bf_profile": _clean_text(profile.get("bf_profile")) or "normal",
    }


def _member_row_for_household(profile: dict[str, Any]) -> dict[str, Any]:
    member = _profile_for_generation(profile)
    member["member_id"] = member["member_profile_id"]
    return member


def _feedback_row_to_dict(row: Any) -> dict[str, Any]:
    return {
        "event_id": row[0],
        "household_id": row[1],
        "member_profile_id": row[2] or "",
        "recipe_id": row[3],
        "plan_id": row[4] or "",
        "slot": row[5] or "",
        "feedback_type": row[6],
        "notes": row[7] or "",
        "source": row[8],
        "created_at": row[9],
    }


def _daily_progress_row_to_dict(row: Any) -> dict[str, Any]:
    return {
        "progress_id": row[0],
        "household_id": row[1],
        "member_profile_id": row[2],
        "plan_id": row[3],
        "day_index": int(row[4]),
        "saved_at": row[5],
        "planned": {
            "kcal": row[6],
            "protein_g": row[7],
            "carbs_g": row[8],
            "fat_g": row[9],
        },
        "target": {
            "kcal": row[10] if row[10] is not None else row[6],
            "protein_g": row[11] if row[11] is not None else row[7],
            "carbs_g": row[12] if row[12] is not None else row[8],
            "fat_g": row[13] if row[13] is not None else row[9],
        },
        "consumed": {
            "kcal": row[14],
            "protein_g": row[15],
            "carbs_g": row[16],
            "fat_g": row[17],
        },
        "meal_completion": _json_loads(row[18]),
        "day_snapshot": _json_loads(row[19]),
        "created_at": row[20],
        "updated_at": row[21],
    }


def _json_dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _normalized_dietary_preferences(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    result: dict[str, Any] = dict(DIETARY_PREFERENCE_DEFAULTS)
    for key in DIETARY_PREFERENCE_DEFAULTS:
        result[key] = bool(source.get(key, result[key]))
    for key, item in source.items():
        if key not in result:
            result[str(key)] = item
    return result


def _normalized_food_preferences(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    ratings_source = source.get("ratings") if isinstance(source.get("ratings"), dict) else {}
    ratings: dict[str, str] = {}
    for key, item in ratings_source.items():
        food_key = _clean_text(key).lower()
        rating = _clean_text(item).lower()
        if food_key and rating in FOOD_PREFERENCE_RATINGS:
            ratings[food_key] = rating

    avoid_source = source.get("avoid_ingredients")
    if isinstance(avoid_source, str):
        avoid_source = [avoid_source]
    try:
        avoid_ingredients = [
            _clean_text(item)
            for item in avoid_source or []
            if _clean_text(item)
        ]
    except TypeError:
        avoid_ingredients = []

    cooking_time = _clean_text(source.get("cooking_time_preference")).lower()
    if cooking_time not in {"quick", "balanced", "no_rush"}:
        cooking_time = "balanced"

    return {
        "ratings": ratings,
        "avoid_ingredients": avoid_ingredients,
        "cooking_time_preference": cooking_time,
    }


def _normalized_health_and_diet_preferences(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, dict) else {}
    result = {
        "dietary_patterns": dict(HEALTH_AND_DIET_DEFAULTS["dietary_patterns"]),
        "health_modes": dict(HEALTH_AND_DIET_DEFAULTS["health_modes"]),
    }
    dietary_source = source.get("dietary_patterns")
    if isinstance(dietary_source, dict):
        for key in result["dietary_patterns"]:
            result["dietary_patterns"][key] = bool(dietary_source.get(key, False))
    health_source = source.get("health_modes")
    if isinstance(health_source, dict):
        for key in result["health_modes"]:
            result["health_modes"][key] = bool(health_source.get(key, False))
    return result


def _json_loads(payload: str | None) -> dict[str, Any]:
    if not payload:
        return {}
    try:
        data = json.loads(payload)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"none", "nan", "nat"}:
        return ""
    return text


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
