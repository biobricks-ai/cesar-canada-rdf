#!/usr/bin/env python3
"""Stream Canadian CEPA/CMP assessment and prioritization records to RDF."""
from __future__ import annotations
import argparse,gzip,hashlib,json,re
from pathlib import Path
import pyarrow.parquet as pq
from rdflib import URIRef
from rdflib.namespace import DCTERMS,OWL,PROV,RDF,RDFS,SKOS
BASE="https://biobricks.ai/cesar-canada-rdf/";CAS="https://identifiers.org/cas/";CANADA=URIRef("https://www.geonames.org/6251999/");IAO=URIRef("http://purl.obolibrary.org/obo/IAO_0000027");DCAT=URIRef("http://www.w3.org/ns/dcat#Dataset");ABOUT=URIRef("https://schema.org/about");CAS_RE=re.compile(r"^\d{2,7}-\d{2}-\d$")
TABLES=("assessments","challenge_substances","cmp3_substances")
def text(v):v="" if v is None else str(v).strip();return v if v and v.lower() not in {"nan","none","null"} else None
def valid_cas(v):
 if not v or not CAS_RE.fullmatch(v):return False
 d=v.replace("-","");return sum(int(x)*n for n,x in enumerate(reversed(d[:-1]),1))%10==int(d[-1])
def canonical_cas(v):
 v=text(v);v=v.rstrip("*").strip() if v else v
 if valid_cas(v):return v
 if v and v.isdigit() and len(v)>=5:
  candidate=f"{v[:-3]}-{v[-3:-1]}-{v[-1]}"
  if valid_cas(candidate):return candidate
 return None
def iri(v):return URIRef(str(v)).n3()
def tr(s,p,o,oi=False):return f"{iri(s)} {iri(p)} {iri(o) if oi else json.dumps(str(o),ensure_ascii=False)} .\n"
def slug(v):return hashlib.sha256(str(v).encode()).hexdigest()[:20]
def pred(table,col):return BASE+f"source-column/{table}/{col}"
def label(row):return text(row.get("chemical_name")) or text(row.get("substance_name")) or "unnamed Canadian substance"
def concepts(table,row):
 fields={"assessments":["prioritization_mechanism","initiative","publication_status","conclusion"],"challenge_substances":["batch_number","batch_status","categorization_decision"],"cmp3_substances":["categorization_category","sub_category","proposed_groups","ecological_approach","human_health_approach"]}[table];return [(f,text(row.get(f))) for f in fields if text(row.get(f))]
def convert_table(path,out,batch_size=4096):
 table=path.stem;pf=pq.ParquetFile(path);ev=gzip.open(out/f"{table}.source.nt.gz","wt");sem=gzip.open(out/f"{table}.nt.gz","wt");tot={"table":table,"source_rows":pf.metadata.num_rows,"represented_rows":0,"source_nonempty_cells":0,"mapped_nonempty_cells":0,"cas_source_rows":0,"cas_linked_rows":0,"n_hexane_rows":0,"qualified_concepts":0}
 for n,row in enumerate(x for b in pf.iter_batches(batch_size=batch_size) for x in b.to_pylist()):
  raw_cas=text(row.get("cas_number"));cas=canonical_cas(raw_cas);sub=BASE+("substance/cas/"+cas if cas else "substance/record/"+slug(row));rec=BASE+f"record/{table}/{n}";source=text(row.get("source_url")) or BASE+"source/unknown";dataset=BASE+"dataset/"+table
  lines=[tr(rec,RDF.type,IAO,True),tr(rec,RDFS.label,f"Canadian {table} row {n}"),tr(rec,PROV.wasDerivedFrom,source,True),tr(rec,ABOUT,sub,True),tr(rec,DCTERMS.isPartOf,dataset,True),tr(dataset,RDF.type,DCAT,True),tr(dataset,RDFS.label,text(row.get("source")) or table),tr(dataset,DCTERMS.spatial,CANADA,True),tr(sub,RDFS.label,label(row))]
  if cas:lines.extend([tr(sub,SKOS.exactMatch,CAS+cas,True),tr(sub,OWL.sameAs,CAS+cas,True)])
  for field,value in concepts(table,row):
   concept=BASE+f"canada-conclusion/{field}/"+slug(value.lower());lines.extend([tr(rec,DCTERMS.subject,concept,True),tr(concept,RDF.type,SKOS.Concept,True),tr(concept,SKOS.prefLabel,value),tr(concept,SKOS.scopeNote,"Source-qualified Canadian CEPA/CMP assessment or prioritization term; not a universal hazard assertion")]);tot["qualified_concepts"]+=1
  date=text(row.get("publication_date")) or text(row.get("draft_start_date"))
  if date:lines.append(tr(rec,DCTERMS.date,date))
  evidence=list(lines);non=0
  for col,val in row.items():
   if text(val) is not None:non+=1;evidence.append(tr(rec,pred(table,col),text(val)))
  ev.write("".join(evidence));sem.write("".join(lines));tot["represented_rows"]+=1;tot["source_nonempty_cells"]+=non;tot["mapped_nonempty_cells"]+=non;tot["cas_source_rows"]+=int(bool(raw_cas));tot["cas_linked_rows"]+=int(bool(cas));tot["n_hexane_rows"]+=int(cas=="110-54-3")
 ev.close();sem.close();tot["row_coverage"]=tot["represented_rows"]/tot["source_rows"];tot["mapped_cell_coverage"]=1;tot["cas_link_coverage"]=tot["cas_linked_rows"]/tot["cas_source_rows"] if tot["cas_source_rows"] else 1;(out/f"{table}.coverage.json").write_text(json.dumps(tot,indent=2)+"\n");return tot
def main():
 p=argparse.ArgumentParser();p.add_argument("source",type=Path);p.add_argument("output",type=Path);a=p.parse_args();a.output.mkdir(parents=True,exist_ok=True);print(json.dumps([convert_table(a.source/f"{t}.parquet",a.output) for t in TABLES],indent=2))
if __name__=="__main__":main()
