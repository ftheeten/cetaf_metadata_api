from ..models import Institutions, Collections, InstitutionsNormalized, CollectionsNormalized
from django.conf import settings
from django.db.models import Max
from django.db.models.functions import Cast, Coalesce
from django.db.models import Q
from datetime import datetime
from .es_mapping.es_mapping_cetaf_institutions import ESMappingCetafInstitutions
#install version 7 of es
from elasticsearch import Elasticsearch
import traceback

class ESLoader():

    es_client=None
    es_url=""
    es_institutions=""
    es_collections=""

    def __init__(self):        
        self.es_url=settings.ES_URL
        self.es_institutions=settings.ES_INDEX_INSTITUTIONS
        self.es_collections=settings.ES_INDEX_COLLECTIONS
        self.es_client=Elasticsearch(self.es_url)
        
    def delete_all_institutions(self):
        self.es_client.delete_by_query(index=self.es_institutions, body={"query": {"match_all": {}}})
        
    def load_current_institutions(self):
        q=Institutions.objects.filter(current=True)        
        for inst in q:
            print(inst.uuid_institution_normalized)
            #print(inst.uuid_institution_normalized)
            #print(inst.data)
            data=inst.data
            #data["modification_date"]=inst.modification_date
            #data["version"]=inst.version
            options={}
            options["gs_collection_overview"]=settings.CETAF_DATA_ADDRESS["collection_overview"]
            flag_record, data=ESMappingCetafInstitutions.GetMapping(inst.uuid_institution_normalized, data, options)
            print("======================================>")
            if flag_record:
                print(data)
                resp = self.es_client.index(index=self.es_institutions, id=inst.uuid_institution_normalized, body=data)
                print(resp)
            
    def delete_all_collections(self):
        self.es_client.delete_by_query(index=self.es_collections, body={"query": {"match_all": {}}})
        
    def load_current_collections(self):
        q=Collections.objects.filter(current=True)        
        for coll in q:
            print(coll.uuid)
            print(coll.uuid_institution_normalized)
            print(coll.data)
            data=coll.data
            data["uuid_institution_normalized"]=coll.uuid_institution_normalized
            data["modification_date"]=coll.modification_date
            data["version"]=coll.version
            resp = self.es_client.index(index=self.es_collections, id=coll.uuid_collection_normalized, body=data)
            print(resp)