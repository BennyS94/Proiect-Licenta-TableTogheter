###########################################################################
# FISIER: tools/ingest_feedback.py
#
# SCOP
#   Colectare feedback barebones fara UI:
#   - Citeste outputs/plan_v2.csv (planul generat de generator_v2)
#   - Permite utilizatorului sa completeze manual coloane (in CSV / Excel):
#       user_decision: ate | skipped | like | dislike (sau gol)
#       swap_applied:  0/1
#       rating:        1..5 (optional)
#       notes:         text liber (optional)
#   - Scrie evenimente append-only in outputs/logs/events.jsonl
#   - Agrega evenimentele intr-un fisier outputs/user_prefs.json
#
# DE CE ASA
#   In faza curenta nu avem aplicatie/UI. Scriptul asta iti permite sa:
#   1) generezi un plan
#   2) marchezi manual in CSV ce iti place/nu iti place
#   3) acumulezi preferinte pe termen lung in user_prefs.json
#
# CUM SE RULEAZA (PowerShell)
#   python tools\ingest_feedback.py --plan outputs\plan_v2.csv --events outputs\logs\events.jsonl --prefs outputs\user_prefs.json
#
# IESIRI
#   - outputs/logs/events.jsonl : jsonl (un event per linie, append)
#   - outputs/user_prefs.json   : agregare simpla (items + buckets), valori clamp [-1..+1]
#
# LEGATURI
#   - src/generator_v2.py: produce plan_v2.csv
#   - src/core/scoring.py: poate consuma user_prefs.json (delta mic in scor)
#
# NOTE
#   - Comentarii in romana fara diacritice.
#   - Nu facem ML aici; doar logare si agregare simpla pentru preferinte.
###########################################################################
# tools/ingest_feedback.py
# Barebones feedback: read annotated plan_v2.csv, emit events.jsonl and aggregate user_prefs.json.
# Usage:
#   python tools/ingest_feedback.py --plan outputs/plan_v2.csv --events outputs/logs/events.jsonl --prefs outputs/user_prefs.json

import argparse, json, os, sys
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

###########################################################################
# _now_iso
# Ce face: timestamp ISO (timezone-aware) pentru evenimente
# Legaturi: datetime

def _now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat()

###########################################################################
# _to_list
# Ce face: helper: intoarce lista cu valori nenule (filtreaza None/empty)

def _to_list(*vals):
    return [v for v in vals if v]

###########################################################################
# main
# Ce face: CLI: citeste plan CSV, scrie events.jsonl, agregare prefs.json
# Legaturi: outputs/plan_v2.csv -> outputs/logs/events.jsonl + outputs/user_prefs.json

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", required=True)  # plan_v2.csv (output real din generator)
    ap.add_argument("--events", default="outputs/logs/events.jsonl")  # jsonl append-only cu evenimente
    ap.add_argument("--prefs", default="outputs/user_prefs.json")  # agregare preferinte pe termen lung
    ap.add_argument("--user_id", default="local")
    args = ap.parse_args()

    plan = Path(args.plan)
    if not plan.exists():
        print(f"[ERR] plan csv not found: {plan}", file=sys.stderr); sys.exit(2)

    df = pd.read_csv(plan)  # citim planul; utilizatorul poate edita manual coloane in CSV/Excel

    # optional columns to annotate in Excel:
    # user_decision ∈ {ate, skipped, like, dislike}, swap_applied ∈ {0,1}, rating ∈ {1..5}, notes (str)
    # if they do not exist, create empties
    for col in ["user_decision","swap_applied","rating","notes"]:  # coloane optionale pe care le poti completa manual
        if col not in df.columns:
            df[col] = None

    events_path = Path(args.events); events_path.parent.mkdir(parents=True, exist_ok=True)
    prefs_path = Path(args.prefs); prefs_path.parent.mkdir(parents=True, exist_ok=True)

    # 1) append events.jsonl
    n_ev = 0
    with events_path.open("a", encoding="utf-8") as fout:  # append: nu stergem istoric (important pentru ML ulterior)
        for r in df.itertuples(index=False):
            ev = {
                "ts": _now_iso(),
                "user_id": args.user_id,
                "day": int(getattr(r, "day", 1) if "day" in df.columns else 1),
                "meal": getattr(r, "meal", ""),
                "event_type": (str(getattr(r, "user_decision", "") or "").lower() or "view"),  # tip eveniment: view/ate/skipped/like/dislike (default view)
                "rating": None if pd.isna(getattr(r, "rating", None)) else float(getattr(r, "rating")),
                "swap_applied": int(getattr(r, "swap_applied", 0) or 0),
                "template_id": str(getattr(r, "template_used", "") or ""),
                "score_at_gen": float(getattr(r, "score", 0.0) or 0.0),  # scorul la generare (debug; optional pt analize)
                "item_ids": _to_list(getattr(r, "protein_uid", None),  # uid-uri pentru itemii din masa (protein/carb/veg)
                                     getattr(r, "side_carb_uid", None),
                                     getattr(r, "side_veg_uid", None)),
                "roles": _to_list("protein","side_carb","side_veg"),
                "buckets": _to_list(str(getattr(r, "protein_bucket","") or ""),  # bucket-uri (protein/carb/veg) daca sunt in CSV
                                    str(getattr(r, "carb_bucket","") or ""),
                                    str(getattr(r, "veg_bucket","") or "")),
                "tags_union": [],  # optional: can be filled later if you export tags
                "notes": ("" if pd.isna(getattr(r, "notes", "")) else str(getattr(r, "notes")))
            }
            fout.write(json.dumps(ev, ensure_ascii=False) + "\n")
            n_ev += 1
    print(f"[OK] appended {n_ev} events -> {events_path}")

    # 2) aggregate to user_prefs.json (tiny weights)
    # schema:
    # {
    #   "version": 1,
    #   "items": {"uid": float(score)},         # [-1..+1], clamp
    #   "buckets": {"protein_bucket": {"animal_lean": +w, ...},  # bucket-uri (protein/carb/veg) daca sunt in CSV
    #               "carb_bucket": {"grains_whole": +w, ...},
    #               "veg_bucket": {"leafy": +w, ...}},
    #   "tags": {"tag_high_fibre_choice": +w, ...}
    # }
    prefs = {"version": 1, "items": {}, "buckets": {"protein_bucket":{}, "carb_bucket":{}, "veg_bucket":{}}, "tags": {}}  # bucket-uri (protein/carb/veg) daca sunt in CSV
    if prefs_path.exists():
        try:
            prefs = json.loads(prefs_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    def _bump(d, key, delta, lo=-1.0, hi=1.0):  # clamp la [-1..+1] ca sa nu explodeze preferintele
        d[key] = float(d.get(key, 0.0) + float(delta))
        if d[key] > hi: d[key] = hi
        if d[key] < lo: d[key] = lo

    # weights (small, conservative)
    W_ITEM_LIKE = +0.20  # ponderi mici (conservatoare) pentru preferinte
    W_ITEM_DISLIKE = -0.20
    W_BUCKET_LIKE = +0.10
    W_BUCKET_DISLIKE = -0.10
    W_SKIP = -0.05
    W_ATE = +0.05

    # recount by scanning the just-written events
    # (simple policy: apply deltas per row)
    with events_path.open("r", encoding="utf-8") as fin:  # recitim jsonl ca sa agregam doar user_id curent
        for line in fin:
            try:
                e = json.loads(line)
            except Exception:
                continue
            if e.get("user_id") != args.user_id:
                continue
            items = e.get("item_ids", []) or []  # uid-uri pentru itemii din masa (protein/carb/veg)
            buckets = e.get("buckets", []) or []  # bucket-uri (protein/carb/veg) daca sunt in CSV
            et = str(e.get("event_type","")).lower()  # tip eveniment: view/ate/skipped/like/dislike (default view)

            # items
            for uid in items:
                if not uid: continue
                if et == "like":      _bump(prefs["items"], uid, W_ITEM_LIKE)  # ponderi mici (conservatoare) pentru preferinte
                elif et == "dislike": _bump(prefs["items"], uid, W_ITEM_DISLIKE)
                elif et == "ate":     _bump(prefs["items"], uid, W_ATE)
                elif et == "skipped": _bump(prefs["items"], uid, W_SKIP)

            # buckets (protein/carb/veg in order, if present)
            if len(buckets) >= 1 and buckets[0]:
                _bump(prefs["buckets"]["protein_bucket"], buckets[0], (W_BUCKET_LIKE if et in ("like","ate") else (W_BUCKET_DISLIKE if et in ("dislike","skipped") else 0)))  # bucket-uri (protein/carb/veg) daca sunt in CSV
            if len(buckets) >= 2 and buckets[1]:
                _bump(prefs["buckets"]["carb_bucket"], buckets[1], (W_BUCKET_LIKE if et in ("like","ate") else (W_BUCKET_DISLIKE if et in ("dislike","skipped") else 0)))  # bucket-uri (protein/carb/veg) daca sunt in CSV
            if len(buckets) >= 3 and buckets[2]:
                _bump(prefs["buckets"]["veg_bucket"], buckets[2], (W_BUCKET_LIKE if et in ("like","ate") else (W_BUCKET_DISLIKE if et in ("dislike","skipped") else 0)))  # bucket-uri (protein/carb/veg) daca sunt in CSV

    prefs_path.write_text(json.dumps(prefs, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] wrote prefs -> {prefs_path}")

if __name__ == "__main__":
    main()
###############################################################################
# OBSERVATII / POSIBILE OPTIMIZARI (NU SCHIMBA ACUM, DOAR DE TINUT MINTE)
#
# 1) Agregarea reciteste tot events.jsonl la fiecare rulare
#    - E simplu, dar O(n) pe istoric. Pentru fisiere mari, poti:
#      - tine un cursor (offset) per user_id, sau
#      - agrega doar ultimele N linii, sau
#      - salva prefs incremental (aplica delta doar pentru batch-ul nou).
#
# 2) Nu agregi tag-uri (tags_union e gol)
#    - Daca vrei preferinte pe tag-uri, ai nevoie fie de:
#      - join cu foods_enriched (uid -> tag_*), fie
#      - export explicit tags in plan_v2.csv.
#
# 3) `roles` este mereu ["protein","side_carb","side_veg"]
#    - Nu folosesti efectiv acest camp. Il poti elimina sau il poti folosi in viitor
#      pentru a valida dimensiunea listelor (item_ids/buckets).
#
# 4) tipurile evenimentelor sunt foarte simple
#    - In viitor, vei vrea: swap (ce ai inlocuit cu ce), partial_accept, cook_time, etc.
#      Poti extinde schema JSONL fara sa strici compatibilitatea (additive).
#
# 5) user_decision completat manual in CSV
#    - E ok in faza asta. Pentru a evita erori de scriere, poti valida input-ul:
#      daca event_type nu e in set, seteaza "view".
#
# 6) Ponderi fixe
#    - Ponderile W_* sunt hard-codate. Mai flexibil: citeste ponderile din YAML (configs)
#      ca sa le tunezi fara modificari de cod.
###############################################################################
