Grocery cooked-to-raw rules v1

Scope:
- Approximate purchase-helper conversions for Generator v1 grocery output.
- These rules do not change recipe nutrition, generator scoring, or exact grocery grams.
- Exact needed cooked grams remain preserved in detailed output.
- Raw equivalent grams are added as separate display/purchase fields.

Current covered cases:
- cooked rice -> raw rice estimate
- cooked pasta -> dry pasta estimate
- cooked beans/lentils/chickpeas -> dry legumes estimate

Current non-covered cases:
- cooked or boiled vegetables are not converted automatically.
- cooked meat, prepared dishes, and ambiguous items are not converted.

Warnings:
- Every conversion must expose `cooked_to_raw_estimate`.
- Items with cooked/raw ambiguity but no explicit rule remain marked with
  `cooked_raw_purchase_ambiguity`.

Limitations:
- These are approximate demo/helper rules, not nutrition calculations.
- No price, store, brand, pantry subtraction, or advanced package optimization is included.
