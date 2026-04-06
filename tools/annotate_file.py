# tools/annotate_file.py
# Comentarii in romana, fara diacritice

import re, sys
from pathlib import Path

FILE_HEADER = '''"""
========================================================================
TableTogetherAPP - generator_v2.py (annotat)

Scop: Creeaza planuri zilnice/multi-zi pe baza profilului utilizatorului,
a regulilor culinare si a sabloanelor (templates), cu ajustari la nivel
de zi si cu suport pentru preferinte utilizator (feedback).

Cum se foloseste (exemplu PowerShell):
  python src\\generator_v2.py ^
    --profile profiles\\user_profile_sample.json ^
    --rules configs\\culinary_rules.yaml ^
    --templates templates\\meal_templates.yaml ^
    --foods data\\foods_enriched.parquet ^
    --taxonomy configs\\taxonomy.yaml ^
    --subs data\\substitution_edges.csv.gz ^
    --prefs outputs\\user_prefs.json ^
    --days 3 ^
    --out_csv outputs\\plan_v2.csv ^
    --out_txt outputs\\plan_v2_summary.txt

Structura generala:
 - IO si incarcari (rules, templates, foods, taxonomy, prefs)
 - Filtre globale si dietetice
 - Selectie mese din sablon (sau fallback blueprint) + scoring
 - Ajustari zilnice (daily rules)
 - Scriere rezultate (CSV + TXT) si sumare
 - Integrare preferinte utilizator (ingest_feedback.py -> user_prefs.json)

Fisiere conexe:
 - configs/culinary_rules.yaml, templates/meal_templates.yaml
 - data/foods_enriched.parquet
 - tools/ingest_feedback.py (user_prefs.json)
 - src/core/scoring.py, src/core/pools.py, src/core/daily_rules.py
 - src/analyze_plan.py (readable)

Nota: Toate comentariile sunt in romana, fara diacritice. Intre functii:
###########################################################################
# nume_functie
# Ce face: descriere scurta
# Legaturi: fisier/chei externe (daca exista)

========================================================================
"""
'''

DESC = {
  "load_user_prefs": ("incarca preferintele utilizatorului din JSON", "outputs/user_prefs.json"),
  "soft_pick_from_topk": ("alege un element din top-k folosind softmax pe scoruri", None),
  "_has_slots": ("detecteaza daca un obiect YAML contine chei 'slots'", None),
  "warn_if_rules_have_slots": ("avertizeaza daca rules YAML include 'slots'", "configs/culinary_rules.yaml"),
  "load_rules_and_templates": ("incarca rules si templates; validari", "configs/culinary_rules.yaml; templates/meal_templates.yaml"),
  "read_foods": ("citeste dataset alimente (parquet/csv)", "data/foods_enriched.parquet"),
  "read_rules": ("citeste YAML de reguli culinare", "configs/culinary_rules.yaml"),
  "read_templates": ("citeste YAML de sabloane de masa", "templates/meal_templates.yaml"),
  "filter_slot": ("filtreaza candidatii pentru un slot pe rol + filtre", "templates + tag-uri din foods"),
  "pick_meal_from_template": ("construieste si puncteaza o masa din sablon", "score_combo; user_prefs"),
  "allowed_for_meal": ("verifica allowed_meals pentru item", None),
  "apply_global_bans": ("aplica interdictii globale din rules", "rules.global_hard_bans"),
  "filter_by_dietary": ("aplica preferinte dietetice din profil", "profile.dietary_preferences"),
  "clamp": ("taie valoarea in interval [lo, hi]", None),
  "macros_for": ("calculeaza macro-uri pentru o portie (per 100g)", "*_g_100g_ml"),
  "combo_hard_banned": ("filtru combinational dur pe masa", "rules.meal_hard_bans"),
  "user_pref_delta": ("mici ajustari de scor din preferinte", "outputs/user_prefs.json"),
  "pick_meal": ("alege cea mai buna combinatie Ps+Sc(+Ve)", "build_pool; score_combo; user_prefs"),
  "generate": ("pipeline principal de generare plan", "toate celelalte"),
  "load_subs": ("incarca substitutii similare din CSV", "data/substitution_edges.csv.gz"),
  "alt_candidates": ("propune alternative similare (swap)", "subs_map + foods_by_uid"),
  "write_readable": ("apeleaza analyze_plan.py pt readable", "src/analyze_plan.py"),
  "main": ("CLI -> parse args -> generate()", None),
}

SEP = "#" * 75

def block(fname: str):
    ce, leg = DESC.get(fname, ("", None))
    lines = [SEP, f"# {fname}", f"# Ce face: {ce}"]
    if leg:
        lines.append(f"# Legaturi: {leg}")
    lines.append("")  # linie goala
    return "\n".join(lines)

def annotate(src_text: str) -> str:
    # adauga blocuri inainte de fiecare "def nume("
    pat = re.compile(r"^def\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(", re.MULTILINE)
    parts, last = [], 0
    for m in pat.finditer(src_text):
        fname = m.group(1)
        parts.append(src_text[last:m.start()])
        parts.append(block(fname))
        parts.append(src_text[m.start():m.end()])
        last = m.end()
    parts.append(src_text[last:])
    return FILE_HEADER + "".join(parts)

def main():
    if len(sys.argv) < 3:
        print("Usage: python tools/annotate_file.py <in.py> <out.py>")
        sys.exit(2)
    inp = Path(sys.argv[1]); outp = Path(sys.argv[2])
    txt = inp.read_text(encoding="utf-8")
    out = annotate(txt)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(out, encoding="utf-8")
    print(f"[OK] Scris: {outp}")

if __name__ == "__main__":
    main()
