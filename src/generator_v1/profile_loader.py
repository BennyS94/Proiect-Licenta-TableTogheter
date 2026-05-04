from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_member_profile(path: str | Path) -> dict[str, Any]:
    profile_path = Path(path)
    with profile_path.open("r", encoding="utf-8") as handle:
        profile = json.load(handle)

    if not isinstance(profile, dict):
        raise ValueError("Profilul trebuie sa fie un obiect JSON.")

    return profile

