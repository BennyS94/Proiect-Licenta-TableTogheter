# Extra Tools

This folder contains optional audit, smoke-test and validation scripts used during development.

These scripts are not part of the mobile or backend runtime path. They are useful for checking data coverage, backend endpoints, generator behavior, mobile structure and repository consistency before a commit or demo.

Run scripts from the repository root, for example:

```powershell
python tools/extra/check_backend_m5_persistence_aware_generation.py
python tools/extra/check_data_qa_price_time_no_missing.py
python tools/extra/check_recipes_cooking_steps_complete.py
```

Prefer existing check scripts before adding new one-off validation code.
