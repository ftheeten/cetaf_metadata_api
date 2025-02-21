from  .es_mapping_interface import ESMappingInterface
from ...models import Institutions, Collections, InstitutionsNormalized, CollectionsNormalized
from ..helper import extract_field, affect
from django.db.models import Q
import sys


class ESMappingCetafInstitutions(ESMappingInterface):
    

    
    @staticmethod
    def get_collections(p_uuid_institution_normalized):
        query = (Q(current=True) & Q(uuid_institution_normalized=p_uuid_institution_normalized))
        colls = Collections.objects.all().filter(query).order_by('identifier')
        return colls
        
        
    @staticmethod
    def GetMapping(p_uuid,  p_object, options=None):
        to_record=False
        print(options)
        returned = {}
        returned["institution_address"]={}
        returned["research"]={}
        returned["project_category"]=[]
        returned["director_or_legal_representative"]={}
        research_fields=[]
        tmp=ESMappingInterface.prepare_data(p_object)
        print(tmp)
        tmp_survey=ESMappingInterface.get_by_source(tmp, "cetaf_survey")
        print(tmp_survey)
        
        if tmp_survey is not None:            
            if "name_institution_en" in tmp_survey:
                name_institution_en=tmp_survey["name_institution_en"]
                print("name_en")
                print(name_institution_en)
                
                to_record=True
                collections=ESMappingCetafInstitutions.get_collections(p_uuid)
                print(collections)
                
                for coll in collections:
                    print(coll.local_identifier)
                    research_fields.append(coll.local_identifier)
                #returned["research"]["research_fields"]=research_fields
                returned["institution_name"]=name_institution_en
                if "address" in tmp_survey:
                    affect(returned["institution_address"], "country", tmp_survey["address"], "country" )
                if "contact" in tmp_survey:
                    affect(returned["institution_address"], "email", tmp_survey["contact"], "mail" )
            if "membership" in tmp_survey:
                print(tmp_survey["membership"])
                m_d=[]
                for m in tmp_survey["membership"]:
                    if "cetaf" in m.lower():
                        m_d.append("CETAF")
                    elif "dissco" in m.lower():
                        m_d.append("DISSCO")
                returned["project_category"]=m_d    
            if "direction" in tmp_survey:
                direction=tmp_survey["direction"]
                last_name= extract_field(direction,"last_name")
                first_name= extract_field(direction,"first_name")
                title= extract_field(direction,"title")
                mail= extract_field(direction,"mail")
                full_name=first_name+" "+last_name
                full_name=full_name.strip()
                returned["director_or_legal_representative"]["dir_rep_name"]=full_name
                returned["director_or_legal_representative"]["dir_rep_title"]=title
                returned["director_or_legal_representative"]["dir_rep_email"]=mail
            
            #returned["modification_date"]=p_object.modification_date
            #returned["version"]=p_object.version
        tmp_grscicoll=ESMappingInterface.get_by_source(tmp, "grscicoll_institutions")
        if tmp_grscicoll is not None:
            print(tmp_grscicoll)
            measurements=ESMappingCetafInstitutions.grscicoll_mf_parser(tmp_grscicoll, "measurementOrFact", "measurementType")
            print(measurements)
             
            research=ESMappingCetafInstitutions.grscicoll_mf_reply_parser(measurements, "Research discipline", ["measurementFactText"] )
            print("=========")
            print(research)
            
            description=ESMappingCetafInstitutions.grscicoll_mf_reply_parser(measurements, "Institution description", ["measurementFactText"] )
            if len(description)>0:
                print(description)
                description=".".join(description)+ "<br/>(Source : GBIF / GrSciColl)"
                returned["institution_description"]=description 
            """
            research=ESMappingCetafInstitutions.parse_path(tmp_grscicoll, "/results:0/measurementOrFact:@measurementType=Research discipline|measurementFactText", "list")
            print(research)
            """
            for res in research:
                research_fields.append(res)
            print(research_fields)
            #sys.exit()
        returned["research"]["research_fields"]=research_fields
        print('!!!!!!!!!!!!!!!')
        print(returned["research"]["research_fields"])
        print('>!!!!!!!!!!!!!!!')
        return to_record, returned