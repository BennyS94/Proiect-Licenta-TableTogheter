# ADR 0002 — API strategy (planned)

Data: 2026-02-25
Status: Accepted (planned)

## Context
Pipeline-ul ML/inference exista deja in repo (generator_v2 + core/*) si produce meniuri in `outputs/`.
Urmatorul pas este integrarea cu Android printr-un backend API, fara a modifica pipeline-ul existent.
In plus, vrem estimarea timpului de gatit folosind GPT, dar cu output strict structurat (JSON schema).

## Decizie
1) Backend API: FastAPI
- Motiv: contract clar (OpenAPI), pydantic pentru validare, usor de containerizat, bun pentru prototipare.

2) Cooktime estimate: GPT cu JSON schema + validare
- Backend trimite un request strict catre GPT astfel incat output-ul sa respecte o schema.
- Backend valideaza raspunsul (pydantic/jsonschema).
- 1 retry daca invalid.
- Daca invalid si dupa retry: fallback simplu (interval generic + confidence scazut).

3) Chei si secrete
- Cheile (OpenAI API key) raman strict in backend (env vars).
- Android nu contine chei si nu cheama GPT direct.

## Alternative considerate
- Flask: respins (mai putin "batteries included" pentru schema/validare).
- Estimare manuala cooktime (heuristici): respins (cerinta este GPT direct, dar constrans).
- Chei in Android: respins (securitate).

## Consecinte
- Repo va include documentatie API + schema JSON canon pentru menu.
- Backend va avea nevoie de rate limiting + loguri (cost si debugging).
- Trebuie mentinut un adaptor stabil CSV->JSON (pornind din output-ul real al generatorului).
