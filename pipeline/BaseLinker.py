import pydantic

class BaseLinker:
  def __init__(self, name: str):
    self.name = name
  
  def __str__(self):
    return "(Linker) " + self.name
  
  def link(self, utterance: str):
    results = {
      "utterance": utterance,
      "ents": [],
      "rels": [],
    }

    print("WARN: Linker returning empty links")
    return results
  

def flat_map(f, xs):
  ys = []
  for x in xs:
    ys.extend(f(x))
  return ys
