# Target architecture

## 1. Scop

Arhitectura tinta urmareste transformarea proiectului intr-un motor hibrid de planificare a meniurilor, bazat pe reguli si scoring, extins gradual cu feedback explicit si mecanisme de similaritate de tip KNN.

Sistemul nu urmareste generarea opaca a meniurilor printr-un model ML complet autonom. In schimb, selectia finala ramane determinista si interpretabila, iar componentele inteligente sunt introduse gradual pentru a imbunatati alegerea candidatilor, substitutions si adaptarea la preferintele utilizatorului.

## 2. Principii de proiectare

Noua arhitectura se bazeaza pe urmatoarele principii:

- separare clara intre alimente canonice si retete
- model de date curat, potrivit pentru extindere ulterioara
- reguli hard pastrate explicit si interpretabil
- scoring controlabil si usor de ajustat
- feedback simplu si util pentru UI
- KNN folosit ca mecanism de suport, nu ca motor unic de generare
- posibilitatea de a integra servicii externe pentru timp si pret, fara a face motorul dependent complet de ele

## 3. Componentele principale

### 3.1 Food_DB

Food_DB contine doar entitati alimentare canonice, utile ca ingrediente sau unitati alimentare de baza.

Acest nivel nu trebuie sa includa preparate complete sau entitati de tip dish. El reprezinta stratul de baza pentru nutritie, taxonomie, substitutions, ingredient matching si analiza ulterioara.

Food_DB va stoca:
- nume canonic
- valori nutritionale per 100 g
- clasificari utile pentru scoring si filtrare
- tags si buckets
- restrictii sau atribute alimentare relevante
- eventual informatii auxiliare pentru cost, substitutions si utilizare in mese

### 3.2 Recipes_DB

Recipes_DB contine retete compuse, nu itemi atomici.

Fiecare reteta este definita prin:
- identitate proprie
- lista de ingrediente
- gramaje
- metadata de masa
- metadata de timp
- eventual atribute de cost si complexitate

Reteta devine unitatea principala de generare a meniului.

### 3.3 Recipe_Ingredients

Acest strat leaga retetele de ingrediente.

El permite:
- calcul nutritional agregat
- grocery list determinist
- substitutions pe ingrediente
- explicabilitate
- ajustari ulterioare pe cost si timp

### 3.4 Generator

Generatorul nou nu mai opereaza direct pe un univers amestecat de rows alimentare si preparate, ci pe candidati de tip recipe.

Rolul lui este:
- sa aplice filtre hard
- sa calculeze scoring-ul
- sa selecteze candidati potriviti pentru contextul curent
- sa construiasca meniul pe una sau mai multe zile
- sa evite repetitiile nedorite
- sa respecte constrangerile household-ului

### 3.5 Feedback layer

Feedback-ul initial este mentinut intentionat simplu, pentru a putea fi testat usor si integrat natural in UI.

Tipurile de feedback vizate in prima versiune sunt:
- liked
- disliked
- too_long

Aceste semnale trebuie sa poata fi atasate fie la nivel de reteta, fie la nivel de ingredient selectat din reteta, in functie de UI si scenariul de utilizare.

### 3.6 Similarity / KNN layer

KNN nu reprezinta motorul principal de generare, ci un strat auxiliar.

In prima versiune tinta, el va fi folosit pentru:
- substitutions intre ingrediente similare
- eventuala extindere sau sugerare de candidati apropiati
- sprijinirea adaptarii pe baza preferintelor istorice

Selectia finala ramane controlata de filtre si scoring.

### 3.7 External services layer

Serviciile externe sunt tratate ca straturi auxiliare.

Se intentioneaza:
- estimare de cooking time prin API/AI, cu stocare locala a rezultatului
- estimare de grocery/price prin API, cu posibilitate de caching local

Arhitectura nu trebuie sa depinda complet de aceste apeluri la fiecare rulare. Rezultatele trebuie sa poata fi reutilizate local.

## 4. Fluxul logic tinta

Fluxul tinta al sistemului este urmatorul:

1. se incarca profilul utilizatorului sau al household-ului
2. se aplica filtre hard peste universul de retete
3. se calculeaza scoruri pentru candidatii ramasi
4. se folosesc, unde este cazul, substitutions sau similaritate pentru suport decizional
5. se selecteaza meniul final in mod determinist
6. meniul este expandat in ingrediente
7. se produce grocery list
8. optional, se obtin sau se recupereaza estimari de pret si timp
9. feedback-ul rezultat este stocat pentru iteratiile viitoare

## 5. Rolul scoring-ului

Scoring-ul ramane componenta centrala a sistemului.

El trebuie sa poata integra:
- compatibilitatea nutritionala
- preferinte
- varietate
- timp
- eventual cost
- efectul feedback-ului istoric

Scoring-ul nu trebuie sa ascunda reguli hard. Constrangerile obligatorii raman separate de evaluarea soft.

## 6. Rolul feedback-ului

Feedback-ul nu trebuie tratat initial ca un mecanism ML complet, ci ca un semnal explicit care modifica gradual preferintele si influenteaza selectia viitoare.

Astfel, sistemul ramane:
- usor de explicat
- usor de testat
- usor de integrat cu UI
- pregatit pentru o extindere ulterioara spre modele mai avansate

## 7. Rolul UI

UI-ul nu este o preconditie pentru arhitectura, dar devine important pentru testarea realista a fluxului de feedback.

Pentru faza initiala, un UI simplu de tip Streamlit este suficient pentru:
- vizualizarea meniului
- inspectarea retetelor selectate
- trimiterea de feedback
- testarea rapida a iteratiilor sistemului

## 8. Directia de implementare

Implementarea se va face incremental, in urmatoarea ordine:

1. clarificarea si stabilizarea Food_DB
2. definirea Recipes_DB si Recipe_Ingredients
3. adaptarea generatorului la noua arhitectura
4. refolosirea sau rescrierea controlata a modulelor existente
5. reintroducerea scoring-ului pe baza noilor entitati
6. integrarea feedback-ului simplu
7. integrarea KNN in rol auxiliar
8. conectarea serviciilor externe pentru timp si pret

### 8.1 Generator v1 - directie incrementala

Generatorul va fi construit incremental, ca sistem modular.

Obiectivul imediat nu este implementarea celei mai avansate metode de optimizare, ci obtinerea unei prime versiuni functionale, coerente si testabile.

Prima faza a generatorului va fi:
- recipe-based
- deterministic
- scoring-based
- compatibila cu household model
- limitata initial la 1 member_profile activ si 1 zi

Deciziile privind optimizare matematica mai avansata, OR-Tools sau alte straturi suplimentare vor fi luate ulterior, pe baza problemelor reale observate in practica.

Food_DB v1 draft ramane baseline-ul activ.
Completari ulterioare din surse externe vor fi selective, pornind din gaps reale observate in recipe mapping si generare.

## 9. Concluzie

Arhitectura tinta urmareste o fundatie mai curata si mai extensibila decat cea actuala. Nucleul sistemului va fi format din Food_DB, Recipes_DB si generatorul bazat pe reguli si scoring, in timp ce feedback-ul, KNN-ul si serviciile externe vor completa treptat motorul fara a-i compromite interpretabilitatea.
