from pipeline.BaseConverter import BaseConverter
from pathlib import Path
import json
import re

class T5Converter(BaseConverter):
  def __init__(self, name='t5'):
    super().__init__(name)
    vocab_mask_path = Path(__file__).parent / "t5vocab.json"
    with open(vocab_mask_path) as f:
      self.vocab_mask = json.load(f)
    self.vocab_unmask = {}
    for k,v in self.vocab_mask.items():
      if v in self.vocab_unmask:
        raise Exception("T5Converter vocab mask has duplicate values")
      self.vocab_unmask[v] = k
  
  def text2sparql(self, utterance: str, fragments: list[str]):
    sparql = ""

    return {
      "sparql": sparql,
    }
 
  def preprocess(self, utterance, fragments, wikisparql) -> dict[str, str]:
    training_data = {
      "inputs": "",
      "labels": "",
    }

    training_data["inputs"] = self.preprocess_inputs(utterance, fragments)
    training_data["labels"] = self.preprocess_labels(wikisparql)

    return training_data

  def preprocess_inputs(self, utterance, fragments) -> str:
    return utterance + " " + self.get_annotations_from_fragments(fragments)
  
  def preprocess_labels(self, wikisparql):
    return self._mask_wikisparql_gold(wikisparql)
  
  def get_annotations_from_fragments(self, fragments: list[str]):
    annotations = []

    for fragment in fragments:
      annotations.append(self._mask_fragment(fragment))  
    
    return ' '.join(annotations)
  
  def _mask_wikisparql_gold(self, wikisparql):
    wikisparql_replacement_table = {
      "(": " ( ", ")": " ) ",
      "{": " { ", "}": " } ",
      "wd:": "wd: ",
      "wdt:": "wdt: ",
      " p:": " p: ",
      " ps:": " ps: ",
      "pq:": "pq: ",
      ",": " , ",
      ",'": ", '",
      "'": " ' ",
      ".": " . ",
      "=": " = ",
      "  ": " ",
    }
    sparql, hashi = self._mask_filter_literals(wikisparql)
    
    for k, v in wikisparql_replacement_table.items():
        sparql = sparql.replace(k, v)
    
    sparql = sparql.lower()

    sparql = self._standardize_vars(sparql)

    sparql_split = sparql.split()

    for i, item in enumerate(sparql_split):
      sparql_split[i] = self._mask_fragment(sparql_split[i])
    
    sparql = ' '.join(sparql_split)

    for keys, val in hashi.items():
      sparql = sparql.replace(keys, val)

    return sparql
  
  def _mask_filter_literals(self, wikisparql: str) -> tuple[str, dict]:
    match_str = r"\'(.*?)\'"
    hashi = {}
    if re.search(match_str, wikisparql):
        lits=re.findall(match_str,wikisparql)
        # print(f"Old: {wikisparql}")
        for j, lit in enumerate(lits):
            idx = j + 1
            wikisparql = wikisparql.replace(f"'{lit.strip()}'", f"'###{idx}'")
            hashi[f'###{idx}'] = lit.strip()
        # print(f"New: {wikisparql}")
    return wikisparql, hashi

  def _unmask_filter_literals(self, sparql: str, hashi: dict) -> str:
    for keys in hashi:
      sparql = sparql.replace(keys, hashi[keys])

  def _standardize_vars(self, sparql):
    num_vars = 6
    newvars = [f"?vr{i}" for i in range(num_vars)]
    sparql_split = sparql.split()
    variables = set([x for x in sparql_split if x[0] == "?"]) # {?var, ?obj}
    
    for i, var in enumerate(sorted(variables)):
      sparql = sparql.replace(var, newvars[i]) # Standardize var names
    
    return sparql


  def _mask_fragment(self, wd_string):
    if wd_string in self.vocab_mask:
      return self.vocab_mask[wd_string]
    else:
      return wd_string
  
  def _unmask_generic(self, masked_string):
    split = masked_string.split()
    lut = self.vocab_unmask

    for i, item in enumerate(split):
      if item in lut:
        split[i] = lut[item]

    return " ".join(split).strip()

if __name__=="__main__":
  sample_converter = T5Converter()
  sample_annotated = {
    "utterance": "Who is the wife of Barack Obama",
    "fragments": ['[DEF]', 'wd:', 'Q76 Barack Obama', '[DEF]', 'wdt:', 'P26 spouse'],
  }
  sample_wikisparql = "SELECT ?xyz WHERE { wd:Q76 wdt:P26 ?xyz }"
  sample_masked = sample_converter.preprocess(
    **sample_annotated,
    wikisparql=sample_wikisparql
  )
  print(sample_masked)
  print(sample_converter._unmask_generic(sample_masked["wikisparql"]))

"""
SELECT ?vr0 WHERE {
  wd:Q76 rdfs:label ?vr0 .
  FILTER (langMatches( lang(?vr0), "EN" ))
}
LIMIT 1
"""
