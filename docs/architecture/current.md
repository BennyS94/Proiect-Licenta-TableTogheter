# Current architecture

## 1. Scope and current status

TableTogether este in prezent un proiect-concept de licenta pentru planificarea meniurilor pe mai multe zile, orientat spre gospodarie / familie. Sistemul actual urmareste generarea unui plan alimentar pe baza preferintelor, obiectivelor si restrictiilor utilizatorilor, folosind o abordare in principal rule-based, sustinuta de scoring si de un strat de feedback de baza.

Implementarea existenta este functionala la nivel de pipeline, dar arhitectura nu este considerata finala. Proiectul se afla intr-o faza de tranzitie, iar urmatoarea prioritate majora este reorganizarea stratului de date si clarificarea separarii dintre alimente canonice si entitati compuse.

## 2. Current functional pipeline

In forma actuala, sistemul este organizat in jurul urmatoarelor etape principale:

- enrich pentru alimente si atasare de buckets, micro-buckets si tag-uri utile
- construire item index pentru similaritate si substitutions
- generare de substitutions item-based
- generator principal rule/scoring-driven pentru selectia planului
- ajustari zilnice de portii prin daily rules
- feedback de baza agregat in preferinte simple
- generare de output operational, inclusiv plan si grocery list

Acest pipeline permite obtinerea unui plan functional si ofera deja o baza practica pentru experimentare si iteratie.

Nota operationala: codul generatorului vechi este izolat pentru referinta in `src/legacy/`. Lucrul activ pentru Generator v1 este separat in `src/generator_v1/` si `src/generator_v1_cli.py`.

## 3. Current data model reality

Modelul actual este construit peste un dataset nutritional prelucrat, imbogatit cu clasificari suplimentare si semnale utile pentru generare. In aceasta forma, baza de date curenta este suficienta pentru rularea pipeline-ului existent, dar nu separa inca suficient de clar:

- alimente canonice / atomice
- preparate sau entitati compuse
- nivelul de ingredient
- nivelul de reteta

Aceasta lipsa de separare face ca unele componente sa devina mai greu de extins elegant, mai ales in perspectiva introducerii unui model mai clar de recipes, feedback mai expresiv si ML ulterior.

## 4. Current strengths

Arhitectura actuala are cateva puncte forte importante:

- exista deja un pipeline cap-coada functional
- exista un nucleu de scoring si selectie care poate produce rezultate utilizabile
- exista output-uri practice pentru plan si grocery
- exista o baza initiala pentru substitutions si feedback
- exista deja documentatie tehnica si structura modulara suficient de buna pentru refactorizare incrementala

Aceste lucruri reprezinta o baza buna pentru urmatoarea etapa de dezvoltare.

## 5. Current limitations

Forma actuala a sistemului are si limitari importante:

- modelul de date nu este inca suficient de curat pentru a sustine natural separarea Food_DB / Recipes_DB
- logica de generare a acumulat datorie tehnica si euristici distribuite in mai multe module
- unele reguli, fallback-uri si conventii de coloane trebuie canonizate mai clar
- unele documente mai vechi descriu o directie intermediara si nu trebuie tratate ca sursa finala de adevar
- structura actuala este suficienta pentru experimentare, dar nu este forma dorita pe termen mediu

## 6. Immediate next priority

Prioritatea imediata a proiectului nu este extinderea directa a componentei de ML, ci reorganizarea stratului de date si a modelului de lucru. Inainte de KNN mai avansat, feedback mai bogat sau ranking supervizat, este necesara clarificarea unei fundatii mai curate pentru:

- alimente canonice
- retete
- relatia dintre retete si ingrediente
- semnale de timp, cost si feedback

Aceasta etapa este considerata preconditie pentru dezvoltarea urmatoarelor componente ale sistemului.

## 7. Transitional note

Acest document descrie starea curenta a proiectului si limitele ei. El nu trebuie interpretat ca descrierea arhitecturii tinta. Directia de evolutie planificata va fi descrisa separat in documentul `docs/architecture/restructure_target.md`.
