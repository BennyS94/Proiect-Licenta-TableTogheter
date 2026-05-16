Grocery Purchase Rules v1

This reference file supports Generator v1 grocery demo output.

Scope:
- This is not price logic.
- This is not store or brand logic.
- This is not advanced grocery optimization.
- These rules are approximate practical purchase suggestions for demo readability.
- Exact grams remain in the detailed grocery output.
- Purchase suggestions are derived from cleaned Grocery List v1 display items.
- Cooked-to-raw conversions live in the separate grocery_cooked_to_raw_rules_v1 reference file.
- Pantry basics are marked as check-at-home items and are not silently deleted.

Rule behavior:
- Eggs and common produce can be rounded to pieces.
- Pasta rice oats flour beans and similar staples can be rounded to simple package sizes.
- Milk and similar drinks can be rounded to liter cartons.
- Yogurt and some cheeses can be rounded to simple tubs or packs.
- Meat and fish stay grams-based with coarse rounding.
- Oils salt pepper herbs spices sugar and honey default to pantry check in v1.

Known limitations:
- Package sizes are generic and intentionally simple.
- Leftover is a rough display value only.
- No supermarket product is selected.
- No price estimate is produced.
- No pantry inventory subtraction is applied.
- Cooked-to-raw conversion is limited to explicit helper rules and remains approximate demo output.
