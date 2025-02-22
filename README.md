# FALCON 2.0

This fork serves to store the amendments I made during my research of converting text-2-SPARQL.
It primarily contains the code needed to generate datasets of annotated queries using Falcon2.0.
The main difference is that the reranking algorithm using a Wikidata SPARQL endpoint is commented out.
(Making requests to online Wikidata endpoints is either slow or rate limited.
Creating a locally hosted version might be possible, officially with [Blazegraph](https://github.com/wikimedia/wikidata-query-rdf/blob/master/docs/getting-started.md), or other [methods](https://wikidataworkshop.github.io/2022/papers/Wikidata_Workshop_2022_paper_4558.pdf) but it takes upwards of a week to load a full dump
and a large amount of disk space.)

## Environment Setup

Before running anything setup the conad environment:

```bash
conda env create -f environment.yml
```

Then activate the environment, and install the spacy pre-trained model:

```bash
conda activate falcon2
python -m spacy download en_core_web_sm
```

Note the environment created with `environment.yml` should already have all the packages from `requirements.txt` as well.

## ElasticSearch Setup

From the original repository, notice that an Elasticsearch endpoint must be available for Falcon2 to work.
To set this up, we need to first create a new environment where `npm` is available to have access to the elasticdump cli tool,
and a locally hosted version of Elasticsearch.

### Locally Host ElasticSearch

We will need to install Elasticsearch from an archive with the following commands:

```bash
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.9.0-linux-x86_64.tar.gz
wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.9.0-linux-x86_64.tar.gz.sha512
shasum -a 512 -c elasticsearch-8.9.0-linux-x86_64.tar.gz.sha512 
tar -xzf elasticsearch-8.9.0-linux-x86_64.tar.gz
```

Once it's installed, we will disable ssl authentication for convenience since this is not meant to be exposed online.
The configuration file is `elasticsearch-8.9.0/config/elasticsearch.yml`.
Around line 92,94,98,103 you should be able to configure various security features.
As an example on line 92:

```yml
xpack.security.enabled: true
```

Should be set to

```yml
xpack.security.enabked: false
```

Lastly run Elasticsearch in the background (using screen, tmux, or just another terminal window):

```bash
./elasticsearch-8.9.0/bin/elasticsearch
```

After some time, the Elasticsearch endpoint should be available on port 9200 by default.

### Load indexes into Elastic search

Now create a new conda environment with `nodejs`:

```bash
conda create -n nodeenv nodejs
```

Activate this environment and the elasticdump utility.

```bash
conda activate nodeenv
npm install elasticdump
```

You can now follow the steps to import the data as described in the [original README](https://github.com/SDM-TIB/falcon2.0#elastic-search-and-background-knowledge).

## Run A Dataset generation pipeline

Once the elastic dump is loaded, you can run a dataset generation pipeline by using any of the pipeline notebooks.
1. pipeline1.ipynb is for using the normal falcon algorithm
2. pipeline2.ipynb is for only creating top k entities + gold entities, with no relations
3. pipeline3.ipynb is for creating top k entities + gold entities, with top k relations + gold relations.
4. pipeline0.ipynb is for no annotations at all.

**k** refers to the number of entities or relations suggested by Falcon2.0 for each entity/relation that was identified
in the query. The value of k can be set by adjusting the **topKelements** variable in `./main.py`.

All 3 look for lcquad2 data in `./lcquad2`,
and output the generated links at the `output_links_dir` path.
This must already be a valid path to an existing directory for the notebook to work.

# Original README

Falcon 2.0 is an entity and relation linking tool over Wikidata (accepted in CIKM 2020). The full CIKM paper can be found at the link: [Falcon 2.0 Paper](https://arxiv.org/pdf/1912.11270.pdf)

It leverages fundamental principles of the English morphology (e.g., N-Gram tiling and N-Gramsplitting) to accurately map entities and relations in short texts to resources in  Wikidata. Falcon is available as Web API and can be queried using CURL: 
```
curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"text":"Who painted The Storm on the Sea of Galilee?"}' \
  https://labs.tib.eu/falcon/falcon2/api?mode=long
```
This is the first resource of this repository. The second resource is described in the ElasticSearch section. 


# Implementation
To begin with, install the libraries stated in the requirements.txt file as follows:
```
pip install -r requirements.txt
```
The FALCON 2.0 tool's code has three main aspects: elastic search, algorithm, and evaluation. 
## Elastic Search and Background Knowledge
Before beginning working with the Wikidata Dump, we first need to connect to an elasticsearch endpoint and a Wikidata endpoint. The elasticsearch endpoint is used to interact with our cluster through the Elasticsearch API. 
The ElasticSearch dump (Also knowns as R2: Background Knowledge) for Falcon 2.0 can be downloaded from this link:
https://doi.org/10.6084/m9.figshare.11362883

To import the Elasticsearch dump please use elasticdump and execute the following commands:
```
elasticdump  --output=http://localhost:9200/wikidataentityindex/  --input=wikidataentity.json  --type=data

elasticdump  --output=http://localhost:9200/wikidatapropertyindex/  --input=wikidatapropertyindex.json  --type=data
```

To change your elasticsearch endpoint, makes changes in Elastic/searchIndex.py and Elastic/addIndex.py:
```
es = Elasticsearch(['http://localhost:9200'])
```
Wikidata SPARQL endpoint helps us to quickly search and analyze big volumes of the data stored in the knowledge graph (here, Wikidata). To change Wikidata endpoint, make changes in main.py:
```
wikidataSPARQL = " "
```
We then create indices for property search and entity search over Wikidata. Refer to the following two functions in Elastic/addIndex.py for the code:
```
def propertyIndexAdd(): ...
def entitiesIndexAdd(): ...
```
Furthermore, we need to execute a search query and get back search hits that match the query. The search query feature is used to find whether a mention is an entity or a property in Wikidata. Note that Elasticsearch uses JSON as the serialization format for the documents. The elasticsearch query used to retrieve candidates from elasticsearch is as follows:
```
{
  "query": {
    "match" : { "label" : "operating income" }
  }
}
```
Search queries over Wikidata are implemented in Elastic/searchIndex.py. Refer to the following two functions in the same file for entity search and property search in Wikidata:
```
def entitySearch(query): ...
def propertySearch(query): ...
```

## Algorithm
main.py contains the code for automatic entity and relation linking to resources in Wikidata using rule-based learning. Falcon 2.0 uses the same approach for Wikidata knowledge graph as used in Falcon for DBpedia(https://labs.tib.eu/falcon/). The rules that represent the English morphology are maintained in a catalog; a forward chaining inference process is performed on top of the catalog during the tasks of extraction and linking. Falcon 2.0 also comprises several modules that identify and link entities and relations to Wikidata knowledge graph. These modules implement POS Tagging, Tokenization & Compounding, N-Gram Tiling, Candidate  ListGeneration, Matching & Ranking, Query Classifier, and N-Gram Splitting. The modules are reused from the implementation of Falcon. 

## Evaluation

### Usage
To run Falcon 2.0, you have to call the function "process_text_E_R(question)" where the question is the short text to be processed by Falcon 2.0 We

For evaluating Falcon 2.0, we relied on three different question answering datasets, namely SimpleQuestion dataset for Wikidata, WebQSP-WD, and LC-QuAD 2.0.

For reproducing the results, "evaluateFalconAPI.py" and "evaluateFalconAPI_entities.py" can be used.

"evaluateFalconAPI_entities.py" evaluates entity linking.

"evaluateFalconAPI.py" evaluates entity and relation linking.

## Experimental Results for Entity Linking

### SimpleQuestions dataset
[SimpleQuestion dataset ](https://github.com/askplatypus/wikidata-simplequestions) contains 5622 test questions which are answerable using Wikidata as underlying Knowledge Graph. Falcon 2.0 reports precision value 0.56, recall value 0.64 and F-score value 0.60 on this dataset. 

### LC-QuAD 2.0 dataset
[LC-Quad 2.0](http://lc-quad.sda.tech/) contains 6046 test questions that are mostly complex (more than one entity and relation). On this dataset, Falcon 2.0 reports a precision value 0.50, recall value 0.56 and F-score 0.53. 


### WebQSP-WD dataset
[WebQSP-WD](https://github.com/UKPLab/coling2018-graph-neural-networks-question-answering/blob/master/WEBQSP_WD_README.md) contains 1639 test questions with a single entity and relation per question. Falcon 2.0 outperforms all other baselines with the highest F-score value 0.82, precision value 0.80, and highest recall value 0.84 on the WebQSP-WD dataset. 

## Experimental Results for Relation Linking

### SimpleQuestions dataset
Falcon 2.0 reports a precision value of 0.35, recall value 0.44 and F-score 0.39 on SimpleQuestions dataset for relation linking task.

### LC-QuAD 2.0
Falcon 2.0 reports a precision value of 0.44, recall value 0.37 and F-score 0.40 on LC-Quad 2.0 dataset. 

# Cite our work

```
@inproceedings{10.1145/3340531.3412777,
author = {Sakor, Ahmad and Singh, Kuldeep and Patel, Anery and Vidal, Maria-Esther},
title = {Falcon 2.0: An Entity and Relation Linking Tool over Wikidata},
year = {2020},
isbn = {9781450368599},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3340531.3412777},
doi = {10.1145/3340531.3412777},
booktitle = {Proceedings of the 29th ACM International Conference on Information &amp; Knowledge Management},
pages = {3141–3148},
numpages = {8},
keywords = {wikidata, dbpedia, relation linking, nlp, english morphology, entity linking, background knowledge},
location = {Virtual Event, Ireland},
series = {CIKM '20}
}

```

