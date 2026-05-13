from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra.build_recipes_v1_2_round30_plus15_nutrition_cache import (
    OUT_UNIT_RULES,
    PARSED,
    apply_round30_unit_rules,
    read_csv,
    write_csv,
)


def main() -> None:
    parsed_rows = read_csv(PARSED)
    unit_rows = apply_round30_unit_rules(parsed_rows)
    write_csv(OUT_UNIT_RULES, unit_rows, list(unit_rows[0].keys()) if unit_rows else [])
    print("Round30 +15 unit rules written")
    print(f"unit_rules={OUT_UNIT_RULES}")
    print(f"rows={len(unit_rows)}")


if __name__ == "__main__":
    main()

