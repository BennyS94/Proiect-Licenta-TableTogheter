# Generator roadmap

Premisa de baza:
- un cont principal
- un household
- mai multe member profiles
- contul este folosit comun de membrii casei
- generatorul v1 foloseste un singur `member_profile` activ
- household optimization vine mai tarziu

Model conceptual minim:
- `account`
- `household`
- `member_profile`
- `plan`
- `feedback_event`

Checkpoint-uri:
- Checkpoint 0: contract generator, profil, `nutrition_target`, feedback, hard filters, scoring
- Checkpoint 1: 1 household + 1 `member_profile` activ + 1 zi
- Checkpoint 2: retete realiste + feedback minim + polish pe pilot
- Checkpoint 3: extindere la mai multe zile
- Checkpoint 4: uniformizare, ingredient reuse, grocery realism
- Checkpoint 5: household generation cu mai multe profile active simultan

Rolul KNN:
- nu este motor principal in v1
- ramane strat auxiliar pentru substitutii, retete similare, candidate expansion si propagare feedback

Rolul Streamlit:
- unealta de testare rapida
- nu produs final
