from pipeline.BaseAnnotator import BaseAnnotator
from pipeline.WikidataObject import WikidataObject, from_json, from_uri
from elasticsearch import Elasticsearch
from SPARQLWrapper import SPARQLWrapper, POST, JSON
from multiprocessing.pool import ThreadPool
import os

# wikidataSPARQL="https://query.wikidata.org/bigdata/namespace/wdq/sparql" 
es_endpoint = "http://localhost:9200"
wikidataSPARQL="https://wikidata.demo.openlinksw.com/sparql"
wde = os.environ['ES_ENTITY_INDEX']
wdp = os.environ['ES_PROPERTY_INDEX']

class SerialAnnotator(BaseAnnotator):
  def __init__(self, name="SerialAnnotator"):
    super().__init__(name=name)
    self.es = Elasticsearch(es_endpoint, timeout=10000)
  
  def batch_annotate(self, batch_linked):
    # assert len(utterances) == len(ents) == len(rels)
    annotated = []
    pool = ThreadPool()
    map_results = pool.map(lambda l: self.annotate(**l), batch_linked)
    pool.close()
    pool.join()
    for r in map_results:
        # print(r)
        annotated.append(r)
    return annotated 


  
  def annotate(self, utterance, ents = [], rels = []):
    fragments = []

    for wd_obj in ents + rels:
      if isinstance(wd_obj, dict):
        wd_obj = from_json(wd_obj)
      fragments.append("[DEF]")
      fragments.append(wd_obj.prefix)

      label = self._find_by_es(wd_obj)

      # print(label)
      fragments.append(wd_obj.id + ' ' + label)
    
    results = {
      "utterance": utterance,
      "fragments": fragments,
    }

    return results
  
  def _find_by_es(self, wd_obj):
    # print("using elastic")
    query = {
      "match": {
        "uri": f"<http://www.wikidata.org/entity/${wd_obj.id}>",
      }
    }
    indexName = wde if wd_obj.id.startswith('Q') else wdp
    size = 1
    response = self.es.search(index=indexName, query=query, size=size)
    results = response['hits']['hits']
    assert(len(results) == size)

    return results[0]['_source']['label']


  def _find_by_sparql(self, wd_obj):
    label_sparql = SPARQLWrapper(wikidataSPARQL)
    label_sparql.setQuery(
      """
      SELECT ?vr0 WHERE {
        wd:"""+wd_obj.id+""" rdfs:label ?vr0 .
        FILTER (langMatches( lang(?vr0), "EN" ))
      }
      LIMIT 1
      """
    )
    label_sparql.setReturnFormat(JSON)
    label_sparql.setMethod(POST)
    label_res = label_sparql.query().convert()
    try:
      label = label_res["results"]["bindings"][0]['vr0']['value']
    except IndexError:
      print("Index error, no label found for")
      print(wd_obj)
      label = ''
    return label
    

def get_label(id):
  return 

if __name__ == "__main__":
  sample_annotator = SerialAnnotator()
  sample_utterance = "Who is the wife of Barack Obama?"
  wd_obama = from_uri("http://www.wikidata.org/entity/Q76")
  wd_son = from_uri("http://www.wikidata.org/prop/direct/P26", as_type="json")
  sample_ents = [wd_obama]
  sample_rels = [wd_son]
  sample_annotation = sample_annotator.annotate(sample_utterance,
                                                sample_ents,
                                                sample_rels)
  print(sample_annotation)