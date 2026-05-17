from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from validate_apply_grocery_product_catalog_v1_batch1 import (
    _counts,
    _format_counts,
    validate_row,
)


ROOT = Path(__file__).resolve().parents[2]
INPUT_PATH = ROOT / "data/grocery/draft/grocery_product_catalog_v1_price_research_filled_all.csv"
CATALOG_OUT = ROOT / "data/grocery/draft/grocery_product_catalog_v1_full.csv"
VALIDATION_OUT = ROOT / "data/grocery/audit/grocery_product_catalog_v1_full_validation.csv"
SUMMARY_OUT = ROOT / "data/grocery/audit/grocery_product_catalog_v1_full_summary.txt"
DEFERRED_OUT = ROOT / "data/grocery/audit/grocery_product_catalog_v1_full_deferred.csv"


def main() -> None:
    CATALOG_OUT.parent.mkdir(parents=True, exist_ok=True)
    VALIDATION_OUT.parent.mkdir(parents=True, exist_ok=True)
    batch = pd.read_csv(INPUT_PATH).fillna("")
    validation_rows = [validate_row(row.to_dict()) for _, row in batch.iterrows()]
    validation = pd.DataFrame(validation_rows)
    safe_mask = (
        (validation["decision_status"] == "safe_to_use_demo")
        & (validation["validation_status"] == "pass")
    )
    applied = batch.loc[safe_mask].copy()
    deferred = batch.loc[~safe_mask].copy()
    deferred_validation = validation.loc[
        ~safe_mask,
        [
            "validation_status",
            "validation_errors",
            "applied_to_catalog",
        ],
    ]
    deferred = pd.concat(
        [
            deferred.reset_index(drop=True),
            deferred_validation.reset_index(drop=True),
        ],
        axis=1,
    )

    validation.to_csv(VALIDATION_OUT, index=False)
    applied.to_csv(CATALOG_OUT, index=False)
    deferred.to_csv(DEFERRED_OUT, index=False)
    SUMMARY_OUT.write_text(
        build_summary(batch=batch, validation=validation, applied=applied, deferred=deferred),
        encoding="utf-8",
    )

    print("Grocery Product Catalog v1 full batch validated and applied")
    print(f"catalog={CATALOG_OUT}")
    print(f"validation={VALIDATION_OUT}")
    print(f"summary={SUMMARY_OUT}")
    print(f"deferred={DEFERRED_OUT}")
    print(
        "rows="
        f"{len(batch)}; safe_applied={len(applied)}; "
        f"needs_review={(batch['decision_status'] == 'needs_review').sum()}; "
        f"keep_deferred={(batch['decision_status'] == 'keep_deferred').sum()}"
    )


def build_summary(
    *,
    batch: pd.DataFrame,
    validation: pd.DataFrame,
    applied: pd.DataFrame,
    deferred: pd.DataFrame,
) -> str:
    status_counts = _counts(batch.get("decision_status", []))
    validation_counts = _counts(validation.get("validation_status", []))
    safe_rows = batch[batch["decision_status"] == "safe_to_use_demo"]
    failed_safe = validation[
        (validation["decision_status"] == "safe_to_use_demo")
        & (validation["validation_status"] != "pass")
    ]
    lines = [
        "Grocery Product Catalog v1 full validation summary",
        "",
        f"input_path={INPUT_PATH}",
        f"catalog_out={CATALOG_OUT}",
        "",
        f"rows_reviewed={len(batch)}",
        f"safe_rows_submitted={len(safe_rows)}",
        f"safe_rows_applied={len(applied)}",
        f"safe_rows_rejected={len(failed_safe)}",
        f"needs_review_count={status_counts.get('needs_review', 0)}",
        f"keep_deferred_count={status_counts.get('keep_deferred', 0)}",
        f"deferred_audit_count={len(deferred)}",
        f"decision_status_counts={_format_counts(status_counts)}",
        f"validation_status_counts={_format_counts(validation_counts)}",
        "",
        "Applied rules:",
        "- only decision_status=safe_to_use_demo rows with passing sanity checks are written to the usable catalog",
        "- needs_review and keep_deferred rows remain audit-only",
        "- source_url, confidence, and qc_notes are preserved",
        "- no prices were invented by the validator",
    ]
    if not failed_safe.empty:
        lines.extend(
            [
                "",
                "Rejected safe rows:",
                *[
                    (
                        f"- {row.get('catalog_item_id')} | {row.get('display_name_en')} | "
                        f"errors={row.get('validation_errors')}"
                    )
                    for _, row in failed_safe.iterrows()
                ],
            ]
        )
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    main()
