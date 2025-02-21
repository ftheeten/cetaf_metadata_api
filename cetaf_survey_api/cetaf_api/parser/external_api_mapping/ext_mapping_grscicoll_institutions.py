from  .ext_mapping_interface import ExtMappingInterface
import sys

class ExtMappingGrSciCollInstitutions(ExtMappingInterface):
    
    @staticmethod
    def GetUUIDFromCode(p_code):
        tmp_direct_uri=None
        tmp_url="https://api.gbif.org/v1/grscicoll/institution?code="+p_code  
        tmp_val=ExtMappingInterface.go_for_api_logic(tmp_url)
        tmp_direct_uri=None
        if "results" in tmp_val:
            if len(tmp_val["results"])>0:
                tmp_key=tmp_val["results"][0]["key"]
                p_code=tmp_key
                tmp_direct_uri="https://scientific-collections.gbif.org/institution/"+tmp_key
            else:
                tmp_url="https://api.gbif.org/v1/grscicoll/institution?alternativeCode="+p_code
                tmp_val=ExtMappingInterface.go_for_api_logic(tmp_url)
                if "results" in tmp_val:
                    if len(tmp_val["results"])>0:
                        tmp_key=tmp_val["results"][0]["key"]
                        p_code=tmp_key
                        tmp_direct_uri="https://scientific-collections.gbif.org/institution/"+tmp_key
        #if tmp_direct_uri is not None:
        #    print(tmp_direct_uri)
        #    sys.exit()
        return p_code, tmp_direct_uri
                                        
    @staticmethod
    def GetMapping(p_val, p_api_data):
        if "results" in p_api_data:
            p_val["grscicoll"]={}
            #id_grscicoll_collections_from_institutions=ExtMappingGrSciCollInstitutions.parse_path(p_data["results"], "/results:0/identifier:@identifierType=Institution GRSciColl key|identifierValue")

            description=ExtMappingGrSciCollInstitutions.parse_path(p_api_data["results"], "/results:0/measurementOrFact:@measurementType=Institution description|measurementFactText")

            research=ExtMappingGrSciCollInstitutions.parse_path(p_api_data["results"], "/results:0/measurementOrFact:@measurementType=Research discipline|measurementFactText", "list")

            p_val["grscicoll"]["description"]=description
            p_val["grscicoll"]["research"]=research
            return p_val
