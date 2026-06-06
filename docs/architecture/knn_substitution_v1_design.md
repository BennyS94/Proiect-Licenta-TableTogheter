# KNN Substitution v1 Design

## Purpose

KNN Substitution v1 este un strat auxiliar pentru similaritate intre retete si pentru pregatirea substitutiilor viitoare.

Scopul initial este KNN-lite, determinist:

- construieste un index simplu de similaritate intre retete;
- propune retete alternative pentru o reteta sursa;
- produce audituri de calitate pentru vecini;
- trimite candidatii printr-un gate de aprobare al generatorului.

KNN este candidate provider. Generatorul ramane validator si approver.

## Non-Goals

KNN v1 nu este:

- motorul principal al generatorului;
- inlocuire automata de mese in planuri generate;
- substitutie de ingrediente;
- training ML sau model supervizat;
- recalculare automata de grocery list dupa substitutii;
- integrare mobile cu inlocuire automata;
- mecanism care ignora hard filters, feedback bans sau profilul utilizatorului.

## Architecture

Fluxul dorit:

```text
recipe_similarity.py
  -> propune retete similare
generator approval audit
  -> valideaza slot, profil, macro, timp, realism si feedback
backend POST /recipes/similar
  -> expune alternative aprobate sau marcate review
mobile KNN-3 UI
  -> afiseaza alternative read-only pentru utilizator
```

Modulul `src/generator_v1/recipe_similarity.py` citeste datasetul demo si construieste feature-uri de reteta. Scripturile de audit din `tools/extra/` masoara coverage-ul, vecinii si rata de aprobare prin generator.

Generatorul nu foloseste KNN pentru selectia planului curent. Selectia ramane deterministic/scoring/constraints-based.

## Recipe Similarity Features

Feature-uri numerice:

- `kcal_per_serving`;
- `protein_g_per_serving`;
- `carbs_g_per_serving`;
- `fat_g_per_serving`;
- `effective_time_min_for_scoring`;
- fallback la `total_time_min` daca timpul efectiv lipseste.

Feature-uri categorice:

- overlap pe `allowed_slots_json`;
- `recipe_kind`;
- `recipe_category`;
- `recipe_family_name`;
- `main_protein_family`, derivat conservator din ingredient rows;
- `dominant_ingredient_name`, derivat din ingredientul acceptat/mapat cu grame cele mai mari.

Scorul KNN-lite este determinist:

- normalizeaza numeric macro-urile si timpul;
- calculeaza distanta numerica manual;
- adauga bonusuri pentru slot/kind/category/family/main protein;
- penalizeaza slot incompatibil, macro gap mare, nutritie incompleta sau timp lipsa;
- returneaza `similarity_score` intre `0` si `1`;
- sorteaza stabil dupa scor, distanta, nume si `recipe_id`.

## Approval Gate

KNN nu aproba singur candidatii. Gate-ul de generator trebuie sa verifice:

- hard filters din profil;
- compatibilitate de slot;
- feedback hard bans, mai ales `explicit_avoid`;
- cache nutritional utilizabil;
- macro fit pentru slot;
- timp si realism;
- restrictii alimentare;
- meal realism pentru slot;
- compatibilitate household intr-un pas viitor.

Statusurile de audit sunt:

- `approved`: candidatul trece criteriile curente de slot/macro/timp/realism;
- `review`: candidatul este posibil util, dar are macro/time/realism risk;
- `rejected`: candidatul este filtrat sau are hard reject.

## KNN Prep 1 Results

Dataset folosit:

`data/recipesdb/draft/v1_2_demo_final/`

Feature coverage:

- `recipe_count=266`;
- `active_recipe_count=261`;
- `complete_nutrition_count=266`;
- `usable_time_count=266`;
- `slot_feature_count=266`;
- `category_feature_count=266`;
- `family_feature_count=266`;
- `main_protein_feature_count=265`;
- `dominant_ingredient_feature_count=266`.

Neighbor audit:

- `seed_count=8`;
- `neighbor_rows=80`;
- `median_similarity_score=0.8535`;
- `good_match_count_score_ge_0_70=77`;
- `questionable_match_count_with_warnings=2`;
- common warning: `macro_gap_large`.

Generator approval audit:

- `selected_meals_tested=12`;
- `candidates_tested=120`;
- `approved_count=92`;
- `review_count=22`;
- `rejected_count=6`;
- `approval_rate=0.7667`.

Interpretare:

- KNN-lite este util ca provider de alternative pentru urmatorul checkpoint;
- approval gate-ul ramane obligatoriu;
- candidatii `review` si `rejected` arata ca similaritatea numerica nu este suficienta singura.

## Future Roadmap

KNN-2:
- implementat ca endpoint backend `POST /recipes/similar`;
- input: `recipe_id`, profil/member optional, slot optional;
- output: alternative aprobate/review cu explicatii;
- nu persista alternative si nu modifica planuri.

KNN-3:
- implementat ca buton mobile `Alternatives` pe meal rows cu `recipe_id`;
- apeleaza backend `POST /recipes/similar`;
- afiseaza alternative `approved` si `review` in mod read-only;
- nu face inlocuire automata, nu modifica planuri si nu recalculeaza grocery list.

KNN-4:
- ingredient substitution candidates;
- foloseste Food_DB si recipe ingredient rows;
- ramane separat de schimbarea meniului.

KNN-5:
- replacement flow validat;
- generator recalculeaza macro/timp/grocery pentru inlocuire;
- planul se modifica doar dupa aprobare explicita.

## Limitations

- Similaritatea foloseste doar feature-uri disponibile in datasetul demo.
- `main_protein_family` este euristic si trebuie tratat ca semnal auxiliar.
- Nu exista inca feedback propagation pe familie/ingredient.
- Nu exista household approval complet.
- Nu exista recalculare grocery pentru alternative.
- API integration exista prin backend KNN-2.
- Mobile integration exista doar ca display read-only KNN-3, fara replacement flow.
