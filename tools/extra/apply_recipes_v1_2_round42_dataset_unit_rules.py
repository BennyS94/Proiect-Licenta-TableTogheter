from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra.build_recipes_v1_2_round42_dataset_nutrition_cache import run_unit_rules


def main() -> None:
    run_unit_rules()


if __name__ == "__main__":
    main()
