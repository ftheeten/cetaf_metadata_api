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
   6. Index Herbarium
3. The API will expose the following types of object
   1. Institutions (location, contact coordinates, summary of the collections and their size)
   2. Collections (with link to parent institutions, taxonomic scope)
   3. Facilities (storage, equipments and labs)
   4. Expertise (registry of persons, compliant with GDPR)
5. We seek interoperability with GBIF, and DISSCO, and plan to map the datastructure by using Latimercore terms
6. the API must document the different networks a Museum belongs to (CETAF, DISSCO etc...)
7. Most recent data can be pushed to ElasticSearch indexes as this is platform used by the CETAF website and other project partners 

## Technical aspects
1. The API is developped in Django (using a Python 3.11 env and brdged to apache via Gunicorn)
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

Overview :
 ```mermaid
flowchart LR;
   A[Google sheet] -->|Command parser 1| C[Django API]
   B[Excel in google cloud] -->|Command parser 2| C[Django API]
   C[Django API] -->|command parser 3| D[ElasticSearch index]
```
Linked Django commands (via shell at the root of the project - **manage.py** level)
1. *Command parser 1* and
2. *Command parser 2*
   1. **python3 manage.py loadindb --extra_apis  institution_overview** (load institutions from Google Sheet, by default do nothing if more recent data in DB)
   2. **python3 manage.py loadindb --extra_apis  institution_overview --force true** (load institutions from Google Sheet, force Google sheet data as being the most recent)
   3. **python manage.py loadindb --extra_apis  grscicoll_institutions grscicoll_collections_from_institutions** (loads institutions from GogoleSheet, if the GRSciColl ID is given get metadata from GriSciColl (grscicoll_institutions) and  forces the cration of the declared collections (grscicoll_collections_from_institutions)
   4. **python3 manage.py loadindb --extra_apis  collection_overview** create collections from the summary wssheet of the institutions
3. *Command parser 3*
   1.  **python manage.py copy_es --target_index institutions** pushes institutions to target ElasticSearch
   2.  **python manage.py copy_es --target_index collections** pushes collections to target ElasticSearch    
      
  ## Model     
  ```mermaid
  classDiagram
class cetaf_api_institutions_normalized{
       -pk
       -uuid
       -data:jsonb (identifier list)
       -creation_date:timestamp
       -modification_date:timestamp
  }
 class cetaf_api_collections_normalized{
       -pk       
       -uuid
       -fk_institution_normalized:int
       -uuid_institution_normalized
       -data:jsonb (identifier list)
       -creation_date:timestamp
       -modification_date:timestamp
  }
  class cetaf_api_institutions{
       -pk
       -uuid
       -fk_institution_normalized: int
       -uuid_institution_normalized
       -cetaf_identifier
       -data:jsonb
       -harvesting_date:timestamp
       -creation_date:timestamp
       -is_current:boolean
       -version:int
  }
class cetaf_api_collections{
       -pk
       -uuid
       -fk_institution_normalized: int
       -uuid_institution_normalized
       -fk_collection_normalized: int
       -uuid_collection_normalized
       -cetaf_identifier
       -data:jsonb
       -harvesting_date:timestamp
       -creation_date:timestamp
       -is_current:boolean
       -version:int
       -source_uri:varchar
  }


cetaf_api_institutions-->cetaf_api_institutions_normalized
cetaf_api_collections_normalized-->cetaf_api_institutions_normalized
cetaf_api_collections-->cetaf_api_institutions_normalized
cetaf_api_collections-->cetaf_api_collections_normalized
  ```
## Current access points (dev) :
So far (nov. 2024 only the institutions and colelctions parts are functionnal) 
1. List all **institutions** (note the paging mechanism)
   1. https://naturalheritage.africamuseum.be/cetaf_survey_api/institutions/?operation=list
   2. https://naturalheritage.africamuseum.be/cetaf_survey_api/institutions/?operation=list&size=2&page=2
2. get **institutions** by identifiers (bot the possibilty to change the protocols) :
   1.  https://naturalheritage.africamuseum.be/cetaf_survey_api/institutions/?operation=get_by_id&protocol=ror&values=02v6zg374
   2. https://naturalheritage.africamuseum.be/cetaf_survey_api/institutions/?operation=get_by_id&protocol=grscicoll&values=LMOB
   3. https://naturalheritage.africamuseum.be/cetaf_survey_api/institutions/?operation=get_by_id&protocol=index_herbarium&values=LI
   4. Parameter has path via .htaccess or rewriterule :https://naturalheritage.africamuseum.be/cetaf/api/institutions/ror/02v6zg374
4. Fuzzy query (**institutions**):
   1. https://naturalheritage.africamuseum.be/cetaf_survey_api/institutions/?operation=query_str&q=wien
5. list of **collections** :
   1. https://naturalheritage.africamuseum.be/cetaf_survey_api/collections/?operation=list&size=100
6. **collections** by collectiob identifier :
   1. https://naturalheritage.africamuseum.be/cetaf_survey_api/collections/?operation=get_by_id&protocol=cetaf&values=BE-MBG%20-%20Algae,%20Fungi,%20Plants%20(Old%20Botany)
 7 **collections** by identifier of the parent institution
   1. https://naturalheritage.africamuseum.be/cetaf_survey_api/collections/?operation=get_by_institution_id&protocol=wikidata&values=Q655542

Elasticsearch institutions https://naturalheritage.africamuseum.be/cetaf_dissco_institutions_dev 

Elasticsearch collections https://naturalheritage.africamuseum.be/cetaf_dissco_collections_dev 

## configuration ##
Definition of the JSON structure is in settings.py.
Principle : the number is the index (0-based of the column in the soruce Google sheet), the term and nested level define the structure of the JSON output

     MAPPING_G_SHEET={
     "institutions": {
        "timestamp":0,
        "timestamp_format":"%d/%m/%Y %H:%M:%S",
        "timestamp_default":"01/01/0001 00:00:00",
        "fields":
        {
            8: "name_institution_en",
            9: "name_institution_en_alternate",
            12: "original_name",
            "address":
            {
                21 : "street",
                22: "city",
                23 :"province",
                24: "postcode" ,
                25: "country"
            }
            ,
            "contact":
            {
                27: "phone",
                28: "mail"
            },
            "social_network":
            {
                30: "X",
                31: "Facebook",
                32: "Instagram",
                33: "LinkedIn",
                34: "YouTube",
            },
            "institution_data":
            {
                10: "logo",
                29: "website",
            },
            "direction":
             {
                41: "title",
                43 : "first_name",
                44: "last_name",
                45: "phone",
                46: "mail",
                47: "orcid"
             },
             "organisation":
             {
                48: "chart_description",
                49: "chart_url",
                50: "chart_file",
                51: "governing_and_exectutive_bodies",
                52: "year_data"
             },
             58: "cetaf_membership",
             "cetaf_representative":
             {
                59: "title",
                61 : "first_name",
                62: "last_name",
                64: "phone",
                63: "mail",
                65: "orcid",
                66: "role_in_institution"
             },
             "cetaf_executive_1":
             {
                69: "title",
                71: "first_name",
                72: "last_name",
                73: "phone",
                74: "mail",
                75: "orcid",
                76: "role_in_institution"
             },
             "cetaf_executive_2":
             {
                79: "title",
                81: "first_name",
                82: "last_name",
                83: "phone",
                84: "mail",
                85: "orcid",
                86: "role_in_institution"
             }  
        },
        "identifiers_api":
        {
            35: "ror"
        },
        "main_identifier_db": {
            "fields" :
            {
                8 :"name_institution_en",
                9: "name_institution_en_alternate",
                12: "original_name",
             },
             "null_value":"Other"
        },
        "other_identifiers":
        {
            35: "ror",
            36: "grid",
            37: "index_herbarium",
            38: "wikidata",
            39: "isni",
            40: "grscicoll",
        }
        ,
        "apis":
        {
            #keep this order for grscicoll
            "ror":35,
            "grscicoll_institutions":40,
            "grscicoll_collections_from_institutions" : None
        }
        
    }    
      } 
           #alternate mapping identifier in column
        MAPPING_G_SHEET_OVERVIEW={
       "cetaf_collection_overview":
        {
            "institution_id_fields":
            {
                "institution_name":4,
               "alternate_institution_name":5
           },
           "appended_excel":
           {
               "collection_size": { "col":25, "sheet": "Collection overview"}
           }
      }
    }
## Exemple of syntax for identifiers
### Institution
     list_identifiers": [
          {
            "type": "cetaf",
            "value": "AT-BC"
          },
          {
            "type": "cetaf_complete",
            "value": "AT-BC Upper Austrian State Museum - Biology Centre"
          },
          {
            "uri": "https://ror.org/No",
            "type": "ror",
            "value": "No"
          },
          {
            "type": "index_herbarium",
            "value": "LI"
          },
          {
            "type": "wikidata",
            "value": "Q2011578"
          },
          {
            "uri": "https://scientific-collections.gbif.org/institution/b6d73ad2-2b45-45b0-9a72-6bc7f6251038",
            "type": "grscicoll",
            "value": "LMOB"
          }
        ],
### Collection
     list_identifiers": [
     {
            "type": "cetaf",
            "value": "EE-EMÜ-ULS - Algae, Fungi, Plants (Old Botany)"
          }
        ],
        "institution_list_identifiers": [
          {
            "type": "cetaf",
            "value": "EE-EMÜ-ULS"
          },
          {
            "type": "cetaf_complete",
            "value": "EE-EMÜ-ULS Estonian University of Life Sciences"
          },
          {
            "type": "ror",
            "value": "00s67c790"
          },
          {
            "type": "grid",
            "value": "grid.16697.3f"
          },
          {
            "type": "index_herbarium",
            "value": "TAA, TAAM, IZBE"
          },
          {
            "type": "wikidata",
            "value": "Q655542"
          },
          {
            "type": "isni",
            "value": 106711127
          },
          {
            "type": "grscicoll",
            "value": "TAA\nTAAM\nIZBE"
          }
        ]
      }
## contact persons
Franck Theeten : Africamuseum Belgium (franck.theeten@africamuseum.be)

Patrick Semal : Head of collection, RBINS, Belgium (p.semal@africamuseum.be)
