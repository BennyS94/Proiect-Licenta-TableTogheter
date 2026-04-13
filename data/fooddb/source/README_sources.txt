Food_DB source references

Scopul acestui folder este sa tina evidenta surselor brute sau de rezerva pentru Food_DB,
astfel incat lucrul viitor si documentatia de licenta sa ramana clare.

Fisiere sursa deja prezente in proiect, in acest folder:
- `ciqual2020_cleand.xlsx`
- `comprehensive_foods_usda.csv`
- `foods_allergens.csv`
- `foods_dietary_restrictions.csv`
- `foods_health_scores_allergens.csv`
- `healthy_foods_database.csv`

Sursa primara CIQUAL prezenta acum in proiect:
- `ciqual2020_cleand.xlsx`
- workbook valid detectat local, cu sheet `Sheet1`
- acest fisier trebuie tratat ca sursa CIQUAL arhivata in repo pentru baseline-ul Food_DB curent

Aceste fisiere corespund setului adaugat din Kaggle:
- `Global Food & Nutrition Database 2026`

Folosite efectiv pana acum pentru Food_DB:
- baza curenta Food_DB v1 a fost construita din date derivate CIQUAL
- dovezi in proiect:
  - `src/enrich_foods.py` mentioneaza explicit CIQUAL 2020 prelucrat
  - fisierele Food_DB draft si triage pastreaza `ciqual_code` / `primary_source_ciqual_code`
- fisierul `ciqual2020_cleand.xlsx` este acum stocat in proiect si trebuie considerat sursa principala arhivata pentru aceasta etapa

Pastrate ca rezerva viitoare / posibila enrichare:
- toate fisierele din `Global Food & Nutrition Database 2026` de mai sus
- rolul lor actual este de rezerva pentru extindere selectiva, nu de sursa primara pentru baseline-ul curent
- daca Food_DB-ul actual bazat pe CIQUAL nu acopera suficiente itemuri pentru Recipes_DB,
  se pot adauga in viitor itemuri suplimentare in mod tintit din aceste fisiere
- nu este recomandat un import bulk doar pentru a creste volumul

Ce ar trebui mentionat mai tarziu in documentatia de licenta:
- CIQUAL este sursa primara folosita efectiv pentru baseline-ul Food_DB curent
- fisierul CIQUAL arhivat in proiect pentru aceasta etapa este `data/fooddb/source/ciqual2020_cleand.xlsx`
- fisierele `Global Food & Nutrition Database 2026` sunt deja stocate in proiect ca rezerva pentru enrichare viitoare
- baseline-ul curent Food_DB este derivat, triat si calibrat inainte de integrarea cu Recipes_DB
- extinderile viitoare trebuie ghidate de gap-uri reale descoperite in recipe-to-food mapping

Lipsuri de sursa de mentionat clar:
- nu este clar din acest folder daca `ciqual2020_cleand.xlsx` este exact fisierul brut original sau o varianta deja curatata
- daca vrei arhivare completa de cercetare, poti adauga manual mai tarziu si fisierele CIQUAL brute originale sau metadata suplimentare
