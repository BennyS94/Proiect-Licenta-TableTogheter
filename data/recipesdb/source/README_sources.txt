Recipes_DB source references

Scopul acestui folder este sa tina evidenta surselor brute pentru Recipes_DB pilot,
astfel incat lucrul viitor si documentatia de licenta sa ramana clare.

Fisiere sursa deja prezente in proiect, in acest folder:
- `1_Recipe_csv.csv`
- `2_Recipe_json.json`

Folosit efectiv pana acum pentru Recipes_DB pilot:
- sursa pilot folosita este datasetul tip `Recipes Dataset 64k Dishes`
- fisierul brut folosit efectiv in proiect este:
  - `data/recipesdb/source/1_Recipe_csv.csv`
- acest fisier a fost folosit pentru:
  - dedupe
  - pilot subset selection
  - ingredient parsing
  - mapping pilot catre Food_DB

Pastrat ca rezerva viitoare:
- `data/recipesdb/source/2_Recipe_json.json`
- in acest stadiu, fisierul JSON este pastrat ca rezerva / reprezentare alternativa a aceleiasi surse
- nu a fost folosit in pipeline-ul pilot curent

Ce ar trebui mentionat mai tarziu in documentatia de licenta:
- Recipes_DB pilot a pornit din `Recipes Dataset 64k Dishes`
- fisierul brut folosit efectiv in implementarea pilot a fost `1_Recipe_csv.csv`
- JSON-ul a ramas disponibil doar ca rezerva viitoare si nu a fost sursa activa in acest pilot
- subsetul pilot, parsingul si mappingul au fost construite incremental peste aceasta sursa CSV

Lipsuri de sursa de mentionat clar:
- nu exista in prezent un fisier separat de metadata sau licenta pentru acest dataset in proiect
- daca vrei arhivare mai completa pentru licenta, astfel de metadata trebuie adaugate manual mai tarziu
