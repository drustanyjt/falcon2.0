
class BaseAnnotator:
  def __init__(self, name: str):
    self.name = name
  
  def annotate(self, link_results):
    return [link_results["utterance"]]
  
  def __str__(self):
    return "(Annotator) " + self.name
  
  

