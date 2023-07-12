from pipeline.BaseAnnotator import BaseAnnotator
from pipeline.WikidataObject import WikidataObject, from_json, from_uri
from SPARQLWrapper import SPARQLWrapper, POST, JSON

wikidataSPARQL="https://query.wikidata.org/bigdata/namespace/wdq/sparql" 

class SerialAnnotator(BaseAnnotator):
  def __init__(self, name="SerialAnnotator"):
    super().__init__(name=name)
  
  def annotate(self, utterance, ents = [], rels = []):
    fragments = []

    for wd_obj in ents + rels:
      if isinstance(wd_obj, dict):
        wd_obj = from_json(wd_obj)
      fragments.append("[DEF]")
      fragments.append(wd_obj.prefix)
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
      label = label_sparql.query().convert()["results"]["bindings"][0]['vr0']['value']
      # print(label)
      fragments.append(wd_obj.id + ' ' + label)
    
    results = {
      "utterance": utterance,
      "fragments": fragments,
    }

    return results

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