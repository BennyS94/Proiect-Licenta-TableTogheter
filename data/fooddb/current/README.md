# Food_DB Current

This folder contains the active canonical food table used by TableTogether.

## Files

- `fooddb_current.csv` - curated food items with normalized names, nutrition columns, category fields and helper metadata used by the generator.

## Runtime role

The mobile app does not read this CSV directly. The Python generator loads it through the backend/generator pipeline and uses it to calculate recipe nutrition, validate ingredient mappings and support macro-aware meal planning.

## Scope

This is an app-facing curated dataset, not a complete public nutrition database. Raw source files, drafts and audit outputs are intentionally excluded from the public repository.
