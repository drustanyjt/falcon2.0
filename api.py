from fastapi import FastAPI
from pydantic import BaseModel
from main import process_text_E_R
app = FastAPI()


@app.get("/")
async def root():
  return {"message": "Hello World"}

class LinkerQuery(BaseModel):
  text: str

@app.post("/linker")
async def linker(body: LinkerQuery):

  text = body.text
  result = {
    "entities_wikidata": [],
    "relations_wikidata": [],
  }
  entities = result["entities_wikidata"]
  relations = result["relations_wikidata"]

  rules = [1,2,3,4,5,8,9,10,12,13,14]

  response = process_text_E_R(text, rules)
  raw_ents = response[0]
  raw_rels = response[1]
  for ent in raw_ents:
    entities.append({
      "URI": ent[0].lstrip('<').rstrip('>'),
      "surface form": ent[1]
    })

  for rel in raw_rels:
    relations.append({
      "URI": rel[0].lstrip('<').rstrip('>'),
      "surface form": rel[1]
    })

  return result

