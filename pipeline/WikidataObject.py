'''
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wds: <http://www.wikidata.org/entity/statement/>
PREFIX wdv: <http://www.wikidata.org/value/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX p: <http://www.wikidata.org/prop/>
PREFIX ps: <http://www.wikidata.org/prop/statement/>
PREFIX pq: <http://www.wikidata.org/prop/qualifier/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX bd: <http://www.bigdata.com/rdf#>
'''

PREFIXES = {
  'http://www.wikidata.org/entity/': 'wd:',
  "http://www.wikidata.org/entity/statement/": "wds:",
  "http://www.wikidata.org/value/": "wdv:",
  "http://www.wikidata.org/prop/direct/": "wdt:",
  "http://wikiba.se/ontology#": "wikibase:",
  "http://www.wikidata.org/prop/": "p:",
  "http://www.wikidata.org/prop/statement/": "ps:",
  "http://www.wikidata.org/prop/qualifier/": "pq:",
  "http://www.w3.org/2000/01/rdf-schema#": "rdfs:",
  "http://www.bigdata.com/rdf#": "bd:",
}
class WikidataObject(object):
  def __init__(self, uri, prefix, id):
    self.uri = uri
    self.prefix = prefix
    self.id = id
  
  def get_metadata(self):
    pass

  def __str__(self):
    return self.prefix + ":" + self.id
  
  def json(self):
    return {
      "uri": self.uri,
      "prefix": self.prefix,
      "id": self.id,
    }

def from_json(json_obj):
  for key in ["uri", "prefix", "id"]:
    if key not in (json_obj):
      raise Exception("WikidataObject Json missing key: " + key)

  return WikidataObject(json_obj["uri"],
                        json_obj["prefix"],
                        json_obj["id"])

def from_uri(uri: str, as_type=None):
  for prefix in PREFIXES:
    if prefix in uri:
      fragments = uri.split(prefix)
      assert len(fragments) == 2
      assert fragments[0].strip() == ""
      id = fragments[1]
      wd_obj = WikidataObject(uri, PREFIXES[prefix], id)
      if as_type is None:
        return wd_obj
      elif as_type == "json":
        return wd_obj.json()
      else:
        raise Exception("Unknown as_type")

  raise Exception("Prefix of URI not found")
