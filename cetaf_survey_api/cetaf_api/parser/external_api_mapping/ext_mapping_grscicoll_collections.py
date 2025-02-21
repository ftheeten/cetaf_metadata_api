from  .ext_mapping_interface import ExtMappingInterface
import sys
import json
import requests
from ..helper import is_valid_uuid 

class ExtMappingGrSciCollCollections(ExtMappingInterface):
    
    @staticmethod
    def GetUUIDFromCode(p_code):
        #print("CODE")
        #print(p_code)
        #sys.exit()
        tmp_direct_uri=None
        tmp_url="https://api.gbif.org/v1/grscicoll/collection?code="+p_code  
        tmp_val=ExtMappingInterface.go_for_api_logic(tmp_url)
        tmp_direct_uri=None
        if "results" in tmp_val:
            if len(tmp_val["results"])>0:
                tmp_key=tmp_val["results"][0]["key"]
                p_code=tmp_key
                tmp_direct_uri="https://scientific-collections.gbif.org/collection/"+p_code
            else:
                tmp_url="https://api.gbif.org/v1/grscicoll/collection?alternativeCode="+p_code
                tmp_val=ExtMappingInterface.go_for_api_logic(tmp_url)
                if "results" in tmp_val:
                    if len(tmp_val["results"])>0:
                        tmp_key=tmp_val["results"][0]["key"]
                        p_code=tmp_key
                        tmp_direct_uri="https://scientific-collections.gbif.org/collection/"+p_code
        return p_code, tmp_direct_uri
                                        
    @staticmethod
    def GetMapping(p_val, p_api_data):
        pass
        
    
    @staticmethod
    def TestGrsciCollURL(p_url):
        returned="UNK"
        go=False
        #print(p_url)        
        resp = requests.get(p_url)          
        #print(resp.headers.get('content-type'))        
        if resp.headers.get('content-type') != 'application/json':
            #print("NOT_JSON")
            elems=p_url.split("/")
            test_uuid=elems[-1]
            #print(test_uuid)
            if is_valid_uuid(test_uuid):
                #print("uuid")
                p_url="https://api.gbif.org/v1/grscicoll/collection/"+test_uuid
                resp = requests.get(p_url)
                if resp.headers.get('content-type') == 'application/json':
                    go=True
        else:
            go=True
        if go:
            json_data = json.loads(resp.text)
            if "code" in json_data:
                #print(json_data["code"])
                returned=json_data["code"]
        return returned
            
    

    
