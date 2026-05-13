from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tools.extra.build_recipes_v1_2_round37_plus100_nutrition_cache import run_mapping


def main() -> None:
    run_mapping()


if __name__ == "__main__":
    main()
