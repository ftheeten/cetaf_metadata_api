# CETAF API


## Aim 
Harvest the survey data and metadata on European Natural Sciences institutiond and collections gathered by CETAF and DISSCO during the "passport" (self description by managers) activities. 
These data are collected via Google Forms, Google Sheets, and for the detail of collection, raw Excel documents uploaded in the Google cloud.
Data are exposed through an API (not trully a REST but JSON over HTTP). 

## Requirements
1. This API can keep an history with the differen versions of data, by presenting the most recent ones by default. 
2. The API can expose the different existing identifiers for collections and instutitons, and use them as resolver or query arguments :
   1. ROR
   2. GrSCiCOLL (GBIF)
   3. Grid
   4. Wikidata
   5. CETAF specific identifiers (eg. BE-RBINS, FR-CIRAD, ES-ICM-CSIC)
3. The API will expose the following types of object
   1. Institutions (location, contact coordinates, summary of the collections and their size)
   2. Collections (with link to parent institutions, taxonomic scope)
   3. Facilities (storage, equipments and labs)
   4. Expertise (registry of persons, compliant with GDPR)
5. We seek interoperability with GBIF, and DISSCO, and plan to map the datastructure by using Latimercore terms
6. the API must document the different networks a Museum belongs to (CETAF, DISSCO etc...)
7. Most recent data can be pushed to ElasticSearch indexes as this is platform used by the CETAF website and other project partners 

## Technical aspects
1. The API is developped in Django (using a Python 3.11 env)
2. Data are stored in a PostgreSQL database (using JSON field to store the core data)
3. Data are read from Google Sheet and Excel spreadhette uploaded in the Google cloud
4. Data are pushed to ElasticSearch (aside of its own endpoint in the Web)

Main Python librairies

  1. Pandas
  2. pygsheets (to read Google Sheet)
  3. cUrL
  4. elasticsearch
  5. google.oauth2.credentials (to read uploaded Excel)
  6. googleapiclient (to read uploaded Excel)
  7. openpyxl (to read uploaded Excel)

## Workflow

 ```mermaid
flowchart LR;
   A[Google sheet] -->|Command parser 1| C[Django API]
   B[Excel in google cloud] -->|Command parser 2| C[Django API]
   C[Django API] -->|command parser 3| D[ElasticSearch index]
```
 
## contact persons
Franck Theeten : Africamuseum Belgiuem (franck.theeten@africamuseum.be)

Patrick Semal : Head of colelction, RBINS, Belgium (p.semal@africamuseum.be)
