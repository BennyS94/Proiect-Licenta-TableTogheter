###########################################################################
# FISIER: src/ontology.py
#
# SCOP
#   Taxonomie simpla (arbore) folosita ca fallback semantic in scoring.
#   Permite:
#     - incarcare YAML (tree + mapari bucket->node)
#     - mapare aliment -> nod taxonomic (pe baza bucket-ului)
#     - distanta intre doua noduri (numar de muchii pana la LCA)
#
# UNDE SE FOLOSESTE
#   - src/core/scoring.py: semnal/penalizare tree_distance() intre Ps si Sc
#
# INTRARE
#   - configs/taxonomy.yaml (tree, bucket_to_node, name_hints optional)  # mapare bucket-uri (protein_bucket etc.) catre noduri
#
# NOTE
#   - Comentarii in romana fara diacritice.
#   - Implementare minimalista, suficienta pentru scoring si fallback.
###########################################################################
# src/ontology.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, List, Optional
import yaml

###########################################################################
# Taxonomy
# Ce face: clasa care construieste parent-map si calculeaza distante in arbore
# Legaturi: load_taxonomy()

class Taxonomy:
    def __init__(self, tree: Dict, bucket_to_node: Dict, name_hints: Dict=None):  # mapare bucket-uri (protein_bucket etc.) catre noduri
        self.tree = tree
        self.bucket_to_node = bucket_to_node or {}  # mapare bucket-uri (protein_bucket etc.) catre noduri
        self.name_hints = name_hints or {}
        # Build parent map for distance computations
        self.parent = {}  # map: child_node_id -> parent_node_id (pentru distante)
        self._index_paths("", tree.get("root", {}))  # construim parent map o singura data la init

    def _index_paths(self, prefix: str, subtree: Dict):
        for k, v in subtree.items():
            node_id = f"{prefix}/{k}" if prefix else k  # first level has no prefix  # construim id-ul nodului ca 'a/b/c'
            node_id = node_id.strip("/")  # construim id-ul nodului ca 'a/b/c'
            if isinstance(v, dict):
                for child in v.keys():
                    child_id = f"{node_id}/{child}".strip("/")
                    self.parent[child_id] = node_id  # map: child_node_id -> parent_node_id (pentru distante)
                self._index_paths(node_id, v)  # construim parent map o singura data la init

    def path_to_root(self, node_id: str) -> List[str]:
        path = []
        cur = node_id
        while cur:
            path.append(cur)
            cur = self.parent.get(cur, "" if cur=="root" else None)  # map: child_node_id -> parent_node_id (pentru distante)
            if cur is None:
                break
        return path

    def lca(self, a: str, b: str) -> Optional[str]:
        pa = set(self.path_to_root(a))
        for x in self.path_to_root(b):
            if x in pa:
                return x
        return None

    def distance(self, a: str, b: str) -> int:
        """Tree distance as edges count between nodes (via LCA)."""
        if not a or not b or a==b:
            return 0 if a==b else 3  # unknowns -> small constant  # daca lipsesc noduri: penalizare mica constanta
        la = self.path_to_root(a)
        lb = self.path_to_root(b)
        sa = set(la)
        for i, node in enumerate(lb):
            if node in sa:
                j = la.index(node)
                # distance = up from a to LCA + up from b to LCA
                return j + i
        return len(la) + len(lb)  # fallback (shouldn't happen)

###########################################################################
# load_taxonomy
# Ce face: incarca YAML si construieste obiect Taxonomy
# Legaturi: configs/taxonomy.yaml

def load_taxonomy(path_yaml: str) -> Taxonomy:
    data = yaml.safe_load(Path(path_yaml).read_text(encoding="utf-8"))  # YAML -> dict (tree + mapping bucket->node)
    return Taxonomy(tree=data.get("tree", {}),
                    bucket_to_node=data.get("bucket_to_node", {}),  # mapare bucket-uri (protein_bucket etc.) catre noduri
                    name_hints=data.get("name_hints", {}))

###########################################################################
# node_for_food
# Ce face: mapare rand aliment -> nod taxonomic folosind bucket_to_node  # mapare bucket-uri (protein_bucket etc.) catre noduri
# Legaturi: foods_enriched.*_bucket

def node_for_food(row, role: str, tax: Taxonomy) -> str:
    """Map a food row to a taxonomy node using bucket mapping + (optional) name hints."""
    role = role.lower()
    if role == "protein":
        b = str(row.get("protein_bucket",""))
        node = tax.bucket_to_node.get("protein_bucket", {}).get(b, "")  # mapare bucket-uri (protein_bucket etc.) catre noduri
    elif role == "side_carb":
        b = str(row.get("carb_bucket",""))
        node = tax.bucket_to_node.get("carb_bucket", {}).get(b, "")  # mapare bucket-uri (protein_bucket etc.) catre noduri
    else:
        b = str(row.get("veg_bucket",""))
        node = tax.bucket_to_node.get("veg_bucket", {}).get(b, "")  # mapare bucket-uri (protein_bucket etc.) catre noduri
    return node or ""

###########################################################################
# tree_distance
# Ce face: wrapper pentru Taxonomy.distance

def tree_distance(tax: Taxonomy, a_node: str, b_node: str) -> int:
    return tax.distance(a_node, b_node)
###############################################################################
# OBSERVATII / POSIBILE OPTIMIZARI (NU SCHIMBA ACUM, DOAR DE TINUT MINTE)
#
# 1) Parent indexing / root
#    - _index_paths() seteaza parent pentru copii, dar nu seteaza explicit parent pentru 'root'.
#      In practica distance() functioneaza, dar poti clarifica root handling (optional).
#
# 2) distance() pentru noduri lipsa
#    - Returnezi 3 (constanta) cand unul lipseste. Alternative:
#      - 0 (nu penaliza lipsa info), sau
#      - o valoare proportionala cu adancimea medie (penalizare mai naturala).
#
# 3) name_hints nefolosit
#    - node_for_food() nu foloseste name_hints. Poti adauga un fallback optional:
#      daca bucket lipseste, cauta hint-uri in name_core si alege nod sugerat.
#      Atentie: tine-l optional, ca sa nu introduci zgomot.
#
# 4) Performanta
#    - lca() foloseste set(pa) si parcurge path b. E ok la arbori mici.
#      Daca taxonomia creste, poti pre-calcula depth si stramosi (rareori necesar aici).
###############################################################################
