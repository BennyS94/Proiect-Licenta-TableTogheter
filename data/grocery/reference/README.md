# Grocery Reference Data

This folder contains small reference tables used to format and price the generated grocery list.

## Files

- `grocery_purchase_rules_v1.csv` - purchase-unit rules for turning ingredient needs into practical buy suggestions.
- `grocery_cooked_to_raw_rules_v1.csv` - simple cooked-to-raw conversion rules.
- `grocery_product_catalog_v1.csv` - product-level price and package references.
- `grocery_product_aliases_v1.csv` - aliases used by the grocery formatter.
- `grocery_price_fallbacks_v1.csv` - fallback prices for categories or common items.

## Runtime role

The generator builds a deterministic grocery list from the selected meal plan and enriches it with purchase suggestions and estimated prices using these reference tables.

Prices are static estimates for demo/product validation, not live supermarket prices.
