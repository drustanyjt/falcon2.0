from pipeline.WikidataObject import WikidataObject

class BaseConverter:
  def __init__(self, name: str):
    self.name = name
  
  def preprocess(self, utterance, fragments, wikisparql):
    return NotImplementedError
  
  def __str__(self):
    return "(Converter) " + self.name
