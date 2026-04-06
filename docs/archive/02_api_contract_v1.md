# Contract API (planned) — TableTogheter

Acest document defineste un contract propus pentru un backend API peste pipeline-ul ML existent.
Nu descrie implementarea (inca nu exista), doar interfata.

## Principii
- Nu modificam pipeline-ul ML: generatorul ramane sursa de adevar pentru meniu.
- API-ul normalizeaza output-ul in JSON si (planned) cere cooktime de la GPT in format strict.
- Cheile OpenAI raman strict in backend.

---

## Endpoint: POST /menu/predict

### Scop
Primeste un user profile + constrangeri si intoarce un meniu generat in JSON.

### Request (propunere)
```json
{
  "profile": { "TODO": "copy exact schema din profiles/user_profile_sample.json" },
  "constraints": {
    "days": 1,
    "seed": 42,
    "topk": { "protein": 3, "carb": 3, "veg": 3 },
    "templates_path": "templates/meal_templates.yaml",
    "rules_path": "configs/culinary_rules.yaml"
  }
}
```

TODO: confirm profile schema from `profiles/user_profile_sample.json` (attach if needed).

### Response (MenuResponse)
- Output-ul se bazeaza pe `outputs/plan_v2.csv` (real). Coloane observate:
[
  "meal",
  "carb_bucket",
  "protein_name",
  "protein_uid",
  "protein_portion_g",
  "side_carb_name",
  "side_carb_uid",
  "side_carb_portion_g",
  "side_veg_name",
  "side_veg_uid",
  "side_veg_portion_g",
  "kcal_meal",
  "protein_meal_g",
  "carb_meal_g",
  "fat_meal_g",
  "sugars_meal_g",
  "fibres_meal_g",
  "salt_meal_g",
  "score",
  "reasons",
  "protein_bucket",
  "protein_portion_range",
  "side_carb_portion_range",
  "side_veg_portion_range",
  "template_used",
  "alt_protein_1_uid",
  "alt_protein_1_name",
  "alt_protein_1_sim",
  "alt_protein_2_uid",
  "alt_protein_2_name",
  "alt_protein_2_sim",
  "alt_protein_3_uid",
  "alt_protein_3_name",
  "alt_protein_3_sim",
  "alt_carb_1_uid",
  "alt_carb_1_name",
  "alt_carb_1_sim",
  "alt_carb_2_uid",
  "alt_carb_2_name",
  "alt_carb_2_sim",
  "alt_carb_3_uid",
  "alt_carb_3_name",
  "alt_carb_3_sim",
  "alt_veg_1_uid",
  "alt_veg_1_name",
  "alt_veg_1_sim",
  "alt_veg_2_uid",
  "alt_veg_2_name",
  "alt_veg_2_sim",
  "alt_veg_3_uid",
  "alt_veg_3_name",
  "alt_veg_3_sim"
]

Exemplu minimal (derivat din plan_v2.csv; campurile `veg_bucket` nu exista in sample si raman TODO daca vei adauga ulterior):
```json
{
  "menu_version": "v2",
  "days": [
    {
      "day": 1,
      "meals": [
        {
          "meal_name": "Breakfast",
          "template_used": null,
          "items": [
            {
              "role": "protein",
              "uid": "soft-ripened-round-cheese-with-bloomy-rind-around-11-fat-coulommiers-type-cheese-reduced-fat_12012",
              "name": "Soft-ripened round cheese with bloomy rind, around 11% fat, Coulommiers-type cheese, reduced fat",
              "portion_g": 111.1,
              "bucket": "eggs"
            },
            {
              "role": "side_carb",
              "uid": "puffed-cereals-textured-bread_7353",
              "name": "Puffed cereals textured bread",
              "portion_g": 107.9,
              "bucket": "bread"
            },
            {
              "role": "side_veg",
              "uid": "button-mushroom-or-cultivated-mushroom-sauteed-pan-fried-without-fat_20125",
              "name": "Button mushroom or cultivated mushroom, saut\u00e9ed/pan-fried",
              "portion_g": 75.0,
              "bucket": null
            }
          ],
          "macros": {
            "kcal": 661.5,
            "protein_g": 37.4,
            "carb_g": 92.3,
            "fat_g": 15.4,
            "sugars_g": 1.1,
            "fibres_g": 3.9,
            "salt_g": 1.27
          },
          "score": 32.48146207360391,
          "reasons": "variety_pro:+eggs;variety_carb:+bread;tree_dist:4;fibre<5.0g;assoc0 eggs-bread",
          "alternatives": {
            "protein": [
              {
                "uid": "firm-cheese-around-14-fat-maasdam-type-cheese-reduced-fat_12705",
                "name": "Firm cheese, around 14% fat, Maasdam-type cheese, reduced fat",
                "sim": 0.998
              },
              {
                "uid": "star-fruit-juice-non-filtered-sampled-in-the-island-of-la-martinique_13409",
                "name": "Star fruit juice, non filtered, sampled in the island of La Martinique",
                "sim": 0.997
              },
              {
                "uid": "bresse-blue-cheese-from-cow-s-milk-reduced-fat-around-15-fat_12528",
                "name": "Bresse blue cheese, from cow's milk, reduced fat, around 15% fat",
                "sim": 0.991
              }
            ],
            "side_carb": [
              {
                "uid": "couscous-precooked-durum-wheat-semolina-cooked-unsalted_9683",
                "name": "Couscous, cooked, unsalted",
                "sim": 0.995
              },
              {
                "uid": "dried-egg-pasta-cooked-unsalted_9822",
                "name": "Dried egg pasta, cooked, unsalted",
                "sim": 0.994
              },
              {
                "uid": "fresh-egg-pasta-cooked-unsalted_9816",
                "name": "Fresh egg pasta, cooked, unsalted",
                "sim": 0.994
              }
            ],
            "side_veg": [
              {
                "uid": "broad-bean-fresh-frozen_20536",
                "name": "Broad bean, fresh",
                "sim": 0.844
              },
              {
                "uid": "lentil-pink-or-red-dried_20535",
                "name": "Lentil, pink or red, dried",
                "sim": 0.837
              },
              {
                "uid": "lentil-blond-dried_20586",
                "name": "Lentil, blond, dried",
                "sim": 0.83
              }
            ]
          }
        },
        {
          "meal_name": "Lunch",
          "template_used": "ln_fishwhite_potato_veg",
          "items": [
            {
              "role": "protein",
              "uid": "white-fish-with-provencal-style-sauce-tomato-sauce-prepacked_25128",
              "name": "White fish with Provencal-style sauce",
              "portion_g": 200.0,
              "bucket": "fish_white"
            },
            {
              "role": "side_carb",
              "uid": "puffed-salty-snacks-made-from-potato-and-soy_38108",
              "name": "Puffed salty snacks, made from potato and soy",
              "portion_g": 170.0,
              "bucket": "potatoes"
            },
            {
              "role": "side_veg",
              "uid": "grated-carrots-with-sauce-prepacked_26257",
              "name": "Grated carrots",
              "portion_g": 140.0,
              "bucket": null
            }
          ],
          "macros": {
            "kcal": 947.9,
            "protein_g": 49.6,
            "carb_g": 113.7,
            "fat_g": 27.5,
            "sugars_g": 23.5,
            "fibres_g": 22.9,
            "salt_g": 5.87
          },
          "score": 70.3055774999998,
          "reasons": "tpl;salt>2.0g;variety_pro:+fish_white;variety_carb:+potatoes;tree_dist:6;assoc+fish_white-potatoes:0.90",
          "alternatives": {
            "protein": [
              {
                "uid": "soy-and-wheat-burger-or-bite-vegan_25223",
                "name": "Soy and wheat burger or bite",
                "sim": 0.981
              },
              {
                "uid": "poultry-on-skewer-with-vegetables-onion-sweet-pepper-cooked_25586",
                "name": "Poultry on skewer, cooked",
                "sim": 0.975
              },
              {
                "uid": "beef-on-skewer-with-vegetables-onion-sweet-pepper-cooked_25585",
                "name": "Beef on skewer, cooked",
                "sim": 0.958
              }
            ],
            "side_carb": [
              {
                "uid": "dried-egg-pasta-cooked-unsalted_9822",
                "name": "Dried egg pasta, cooked, unsalted",
                "sim": 0.994
              },
              {
                "uid": "bread-french-bread-baguette-or-ball-multigrain-from-bakery_7255",
                "name": "Bread, French bread, , multigrain, from bakery",
                "sim": 0.994
              },
              {
                "uid": "rusk-multigrain_7330",
                "name": "Rusk, multigrain",
                "sim": 0.994
              }
            ],
            "side_veg": [
              {
                "uid": "beetroot-salad-with-sauce-prepacked_26258",
                "name": "Beetroot salad",
                "sim": 0.972
              },
              {
                "uid": "tomato-sauce-w-olives-prepacked_11178",
                "name": "Tomato sauce, w olives",
                "sim": 0.91
              },
              {
                "uid": "tomato-sauce-with-onions-prepacked_11107",
                "name": "Tomato sauce",
                "sim": 0.909
              }
            ]
          }
        },
        {
          "meal_name": "Snack",
          "template_used": null,
          "items": [
            {
              "role": "protein",
              "uid": "ti-nain-pulp-steamed_20834",
              "name": "Ti nain, pulp, steamed",
              "portion_g": 300.0,
              "bucket": "veggie"
            },
            {
              "role": "side_carb",
              "uid": "rice-pudding-cake-canned_39232",
              "name": "Rice pudding, cake",
              "portion_g": 250.0,
              "bucket": "grains_refined"
            },
            {
              "role": "side_veg",
              "uid": "sweet-pepper-red-cooked_20088",
              "name": "Sweet pepper, red, cooked",
              "portion_g": 165.0,
              "bucket": null
            }
          ],
          "macros": {
            "kcal": 362.4,
            "protein_g": 16.4,
            "carb_g": 61.5,
            "fat_g": 5.4,
            "sugars_g": 39.9,
            "fibres_g": 4.4,
            "salt_g": 0.38
          },
          "score": 37.06785250000003,
          "reasons": "variety_pro:+veggie;variety_carb:+grains_refined;tree_dist:4;kcal>220;assoc0 veggie-grains_refined;dessert@snack",
          "alternatives": {
            "protein": [
              {
                "uid": "pigeon-pea-whole-steamed_20833",
                "name": "Pigeon pea, whole, steamed",
                "sim": 1.0
              },
              {
                "uid": "yellow-banana-pulp-steamed-sampled-in-the-island-of-la-martinique_20800",
                "name": "Yellow banana, pulp, steamed, sampled in the island of La Martinique",
                "sim": 1.0
              },
              {
                "uid": "star-fruit-juice-filtered-sampled-in-the-island-of-la-martinique_13408",
                "name": "Star fruit juice, filtered, sampled in the island of La Martinique",
                "sim": 1.0
              }
            ],
            "side_carb": [
              {
                "uid": "breakfast-cereals-chocolate-puffed-popped-rice-not-fortified-with-vitamins-and-chemical-elements_32012",
                "name": "Breakfast cereals, chocolate puffed/popped rice",
                "sim": 0.953
              },
              {
                "uid": "breakfast-cereals-chocolate-puffed-popped-rice-fortified-with-vitamins-and-chemical-elements_32131",
                "name": "Breakfast cereals, chocolate puffed/popped rice, fortified with vitamins and chemical elements",
                "sim": 0.942
              },
              {
                "uid": "sweet-potato-cooked_4102",
                "name": "Sweet potato, cooked",
                "sim": 0.846
              }
            ],
            "side_veg": [
              {
                "uid": "pepper-sweet-red-sauteed-pan-fried-without-fat_20329",
                "name": "Pepper, sweet, red, saut\u00e9ed/pan-fried",
                "sim": 0.991
              },
              {
                "uid": "beetroot-cooked_20003",
                "name": "Beetroot, cooked",
                "sim": 0.991
              },
              {
                "uid": "tomato-dried_20189",
                "name": "Tomato, dried",
                "sim": 0.991
              }
            ]
          }
        },
        {
          "meal_name": "Dinner",
          "template_used": null,
          "items": [
            {
              "role": "protein",
              "uid": "chicken-breast-without-skin-cooked-organic_36041",
              "name": "Chicken, breast, cooked, organic",
              "portion_g": 120.0,
              "bucket": "animal_lean"
            },
            {
              "role": "side_carb",
              "uid": "rice-red-cooked-unsalted_9110",
              "name": "Rice, red, cooked, unsalted",
              "portion_g": 250.0,
              "bucket": "grains_refined"
            },
            {
              "role": "side_veg",
              "uid": "haricot-beans-with-tomato-sauce-canned_20194",
              "name": "Haricot beans with tomato sauce",
              "portion_g": 165.0,
              "bucket": null
            }
          ],
          "macros": {
            "kcal": 628.8,
            "protein_g": 53.7,
            "carb_g": 87.2,
            "fat_g": 4.5,
            "sugars_g": 2.7,
            "fibres_g": 19.1,
            "salt_g": 1.7
          },
          "score": 6.621587500000064,
          "reasons": "variety_pro:+animal_lean;variety_carb:+grains_refined;tree_dist:4;assoc0 animal_lean-grains_refined",
          "alternatives": {
            "protein": [
              {
                "uid": "yellow-banana-pulp-steamed-sampled-in-the-island-of-la-martinique_20800",
                "name": "Yellow banana, pulp, steamed, sampled in the island of La Martinique",
                "sim": 1.0
              },
              {
                "uid": "chicken-breast-without-skin-cooked_36018",
                "name": "Chicken, breast, cooked",
                "sim": 1.0
              },
              {
                "uid": "tuna-roasted-baked_26041",
                "name": "Tuna, roasted/baked",
                "sim": 1.0
              }
            ],
            "side_carb": [
              {
                "uid": "dried-pasta-wholemeal-cooked-unsalted_9871",
                "name": "Dried pasta, wholemeal, cooked, unsalted",
                "sim": 0.997
              },
              {
                "uid": "millet-whole_9330",
                "name": "Millet, whole",
                "sim": 0.996
              },
              {
                "uid": "bread-wholemeal-or-integral-bread-made-with-flour-type-150_7110",
                "name": "Bread, wholemeal or integral bread",
                "sim": 0.996
              }
            ],
            "side_veg": [
              {
                "uid": "flageolet-bean-canned-drained_20508",
                "name": "Flageolet bean, canned, drained",
                "sim": 0.98
              },
              {
                "uid": "haricot-bean-canned-drained_20511",
                "name": "Haricot bean, canned, drained",
                "sim": 0.979
              },
              {
                "uid": "diced-mixed-vegetables-canned-drained_20051",
                "name": "Diced mixed vegetables, canned, drained",
                "sim": 0.975
              }
            ]
          }
        }
      ]
    }
  ]
}
```

---

## Endpoint: POST /cooktime/estimate (planned)

### Scop
Primeste un meniu (sau o parte din meniu) si intoarce estimarea timpului de gatit (strict JSON schema).

### Request (propunere)
```json
{
  "menu": { "TODO": "MenuResponse sau subset (doar o zi / o masa)" },
  "options": {
    "assume_parallel_cooking": true,
    "skill_level": "intermediate",
    "equipment": ["stove", "oven"]
  }
}
```

### Response (CookTimeEstimate)
```json
{
  "total_minutes": 45,
  "range_minutes": [35, 60],
  "breakdown": [
    { "step": "prep", "minutes": 10 },
    { "step": "cook", "minutes": 30 },
    { "step": "plate", "minutes": 5 }
  ],
  "assumptions": [
    "ingredients already available",
    "single person cooking",
    "standard kitchen tools"
  ],
  "confidence": 0.62
}
```

### Validare + retry + fallback (planned)
- Backend valideaza raspunsul GPT pe schema (jsonschema/pydantic).
- Daca invalid: 1 retry cu instructiuni mai stricte.
- Daca tot invalid: fallback simplu:
  - `range_minutes: [30, 60]`, `confidence: 0.25`, assumptions standard.

---

## Endpoint: POST /menu/with_cooktime (planned)

### Scop
One-shot: genereaza meniu + calculeaza cooktime prin GPT.

### Request
- Identic cu `/menu/predict`, cu un camp optional `cooktime_options`.

### Response
```json
{
  "menu": { "TODO": "MenuResponse" },
  "cooktime": { "TODO": "CookTimeEstimate" }
}
```

---

## Erori (propunere)
- 400: request invalid (schema/profile lipsa)
- 422: validare schema (pydantic/jsonschema)
- 500: eroare interna (pipeline inference / IO)
- 502: eroare furnizor GPT (daca se foloseste cooktime)

---

## Observatii de mapping CSV -> JSON
- `meal` devine `meal_name`
- perechile `protein_*`, `side_carb_*`, `side_veg_*` devin `items[]`
- `kcal_meal`, `protein_meal_g`, ... devin `macros`
- `alt_*` devin `alternatives` pe rol
- `reasons` si `score` se pastreaza ca debug/trace (optional sa fie ascuns in Android UI)
