from __future__ import annotations

import pandas as pd

from round65_household_common import (  # noqa: E402
    AUDIT_DIR,
    active_members,
    format_counts,
    load_household_profile,
    member_target_rows,
)


SUMMARY_OUT = AUDIT_DIR / "generator_v1_round65_household_target_summary.txt"
MEMBER_TARGETS_OUT = AUDIT_DIR / "generator_v1_round65_household_member_targets.csv"


def main() -> None:
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    household_profile = load_household_profile()
    rows, targets = member_target_rows(household_profile)
    pd.DataFrame(rows).to_csv(MEMBER_TARGETS_OUT, index=False)
    SUMMARY_OUT.write_text(
        _summary_text(household_profile, rows, targets),
        encoding="utf-8",
    )
    print("Round65 household target audit written")
    print(f"summary={SUMMARY_OUT}")
    print(f"member_targets={MEMBER_TARGETS_OUT}")
    print(f"members={len(targets)}")


def _summary_text(
    household_profile: dict[str, object],
    rows: list[dict[str, object]],
    targets: dict[str, object],
) -> str:
    members = active_members(household_profile)
    totals = {
        "kcal": round(sum(float(target.kcal) for target in targets.values()), 1),
        "protein_g": round(sum(float(target.protein_g) for target in targets.values()), 1),
        "carbs_g": round(sum(float(target.carbs_g) for target in targets.values()), 1),
        "fat_g": round(sum(float(target.fat_g) for target in targets.values()), 1),
    }
    daily_rows = _unique_member_rows(rows)
    kcal_values = [float(row["daily_kcal_target"]) for row in daily_rows]
    kcal_spread = round(max(kcal_values) - min(kcal_values), 1) if kcal_values else 0.0
    slot_counts = pd.Series([row.get("slot") for row in rows]).value_counts()
    lines = [
        "Round65 Household target audit",
        "",
        f"household_id={household_profile.get('household_id')}",
        f"household_name={household_profile.get('household_name')}",
        f"active_member_count={len(members)}",
        f"slot_counts={format_counts(slot_counts)}",
        "",
        "Household totals:",
        f"total_kcal={totals['kcal']}",
        f"total_protein_g={totals['protein_g']}",
        f"total_carbs_g={totals['carbs_g']}",
        f"total_fat_g={totals['fat_g']}",
        f"kcal_target_spread={kcal_spread}",
        "",
        "Member targets:",
    ]
    for row in daily_rows:
        lines.append(
            "- "
            f"{row['display_name']} ({row['member_id']}): "
            f"kcal={row['daily_kcal_target']}, "
            f"protein={row['daily_protein_g_target']}g, "
            f"carbs={row['daily_carbs_g_target']}g, "
            f"fat={row['daily_fat_g_target']}g, "
            f"kcal_ratio={row['kcal_target_ratio']}"
        )
    lines.extend(
        [
            "",
            "Feasibility notes:",
            "- Profilele demo nu sunt edge/aggressive-cut si sunt potrivite pentru un prim audit household.",
            "- Diferenta de kcal dintre membri cere portii per-member, nu meniuri complet separate implicit.",
            "- Diferenta de protein/carbs inseamna ca shared lunch/dinner are nevoie de top-up individual prin breakfast/snack sau ajustari manuale ulterior.",
            "- Formulele de target sunt cele existente din Generator v1; acest audit nu le modifica.",
        ]
    )
    return "\n".join(lines) + "\n"


def _unique_member_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    seen: set[str] = set()
    unique_rows: list[dict[str, object]] = []
    for row in rows:
        member_id = str(row.get("member_id") or "")
        if member_id in seen:
            continue
        seen.add(member_id)
        unique_rows.append(row)
    return unique_rows


if __name__ == "__main__":
    main()
