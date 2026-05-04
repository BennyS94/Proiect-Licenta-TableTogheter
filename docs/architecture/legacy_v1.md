# Arhitectura TableTogheter

## Ce exista acum (real)

### Repo layout (relevant)
- `src/legacy/` – pipeline ML/inference vechi (generator + core scoring/pools/daily_rules), pastrat pentru referinta
- `src/generator_v1/` – lucrul activ pentru Generator v1
- `profiles/` – input user profile (ex: `user_profile_sample.json`)
- `configs/` – reguli YAML (scoring, priors, daily rules, enrich)
- `templates/` – sabloane YAML pentru mese (slots/portion ranges)
- `data/` – dataset enrichuit + substitutii (parquet/csv.gz)
- `outputs/` – rezultate inference (CSV + summary + readable)

### Fluxul actual (CLI / pipeline local)
1. `profiles/<user>.json` + `configs/*.yaml` + `templates/*.yaml` + `data/foods_enriched.parquet`
2. `src/legacy/generator_v2.py` genereaza:
   - `outputs/plan_v2.csv` (structura planului, item-uri, macro-uri, alt_*)
   - `outputs/plan_v2_summary.txt` (tinte + totaluri)
   - `outputs/plan_v2_readable.txt` (render text pentru inspectie umana)

Nota: exemplul actual de readable arata formatul “Ps/Sc/Ve + Alt_* + totaluri” si totaluri zilnice. fileciteturn14file3

## Componenta API (planned)

### Scop
Adaugam un backend API (probabil FastAPI) fara a modifica logica inference-ului ML existent.
API-ul devine un strat de orchestrare peste generatorul vechi izolat in `src/legacy/generator_v2.py`.

### Flux to-be (ML -> API -> Android)
1. Android trimite `UserProfile` + constrangeri catre Backend API
2. Backend API apeleaza pipeline-ul ML existent (in-process) si obtine planul
3. Backend serializeaza planul intr-un JSON canon (MenuResponse)
4. (Optional, planned) Backend apeleaza GPT pentru `CookTimeEstimate` pe baza meniului
5. Android primeste JSON final (meniu + cooktime)

### Boundary de securitate
- Cheile (OpenAI API key, alte secrete) exista doar in backend (env vars / secret store)
- Android nu primeste niciodata chei sau prompturi interne

### Observatii de implementare (planned, fara cod acum)
- API-ul va avea un adaptor `csv->json` pornind din output-ul real al generatorului (`plan_v2.csv`).
- Endpoint-urile propuse sunt in `docs/02_api_contract.md`.
