import json
from pathlib import Path
R=Path(__file__).resolve().parents[1]
def test_contract():
 for p in ["health/license.json","health/provenance.json","health/source-coverage.json","health/field-disposition.json","health/overlap.json","health/build-status.json","health/ontology-policy.json","rdf-health.json"]:json.loads((R/p).read_text())
