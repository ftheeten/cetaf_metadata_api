from ..models import Institutions, Collections, InstitutionsNormalized, CollectionsNormalized
from django.conf import settings
from datetime import datetime
import pandas as pnd
import pygsheets
import requests 
import sys
from django.db.models import Max
from django.db.models.functions import Cast, Coalesce
from django.db.models import Q
import re
from .read_excel import read_excel
import traceback
import sys
#import numpy as np

class GSCetafParser():

    sheet_institutions=None
    gs_auth_file=None
    mapping_institution=None
    extra_apis=[]
    RE_SIMPLE_PATTERN=r'\s|\(|\)|\\|\.|\/'
    RE_PATTERN_CETAF=re.compile('([^\s]+)(.*)')
    
    
    def __init__(self):
         self.sheet_institutions=settings.CETAF_DATA_ADDRESS["institutions"]
         self.gs_auth_file=settings.GOOGLE_AUTH_FILE
         self.mapping_institution=settings.MAPPING_G_SHEET["institutions"]    
         self.sheet_collection_overview=settings.CETAF_DATA_ADDRESS["collection_overview"]         
         self.mapping_collection_overview=settings.MAPPING_G_SHEET_OVERVIEW["cetaf_collection_overview"]
         self.extra_apis=settings.APIS_SHEET
         print("EXTRA")
         print(self.extra_apis)
         #sys.exit()
         
    def add_field(self, src, value):
        returned={
            "source":src,
            "value": value
        }
        return returned
      
    def add_field_value(self, src, row, index):        
        return self.add_field(src, row[index])
      
    def go_for_api(self, url, param):
        print(url)
        print(param)
        url=url+str(param)
        print(url)
        return self.go_for_api_logic( url)
        
    def go_for_api_logic(self, url):
        returned=None
        resp=requests.get(url, headers={'Content-type': 'application/json', 'Accept': 'application/json'})
        print(resp)
        if resp.status_code==200:
            print(resp.json())
            return resp.json()
        return returned
        
    def parse_cetaf_ident(self, p_val):        
        tmp=self.RE_PATTERN_CETAF.match(p_val)
        if tmp is not None:
            return tmp[1]
        else:
            return p_val
        
    def load_institution_sheet(self, extra_apis, force):     
        self.load_sheet_logic(self.mapping_institution, extra_apis, force)
        
    def get_other_institution_identifiers(self, protocol, value):
        q=Institutions.objects.extra(where=["uuid in (select d.uuid  FROM cetaf_api_institutions d cross join lateral jsonb_array_elements(d.data->'list_identifiers') o(obj) where (o.obj->>'type')=%s and  (o.obj->>'value')=%s)"], params=(protocol, value))
        print(q)
        print(q.count())
        if q.count()>0:
            first_inst=q.first()
            returned=first_inst.get_identifiers()
            return returned
        return {}
        
    def load_collection_overview_sheet(self, extra_apis=None): 
        self.load_collection_overview_logic(extra_apis)
    
    def parse_collection_df(self,inst_uuid, inst_idents, p_df, inst_id, modification_date):        
        returned={}
        p_df.columns = [re.sub('_+','_',re.sub(self.RE_SIMPLE_PATTERN, '_', x.lower())).strip('_').strip() for x in p_df.columns]
        print(p_df)
        print( p_df.columns )
        for i, row in p_df.iterrows():
            collection=row["discipline"]
            print(collection)
            if collection is not None:
                collection_id=inst_id+" - "+collection
                if "objects_specimens_quantity_count_or_estimate" in row:
                    obj_count=row['objects_specimens_quantity_count_or_estimate'] 
                else:
                    obj_count = None
                    
                if 'types_quantity_count_or_estimate' in row:
                    type_count=row['types_quantity_count_or_estimate']
                else:
                    type_count=None
                
                if 'uncertainty_level' in row:
                    obj_uncert=row['uncertainty_level']
                else:
                    obj_uncert=None
                
                if 'uncertainty_level_1' in row:
                    type_cert=row['uncertainty_level_1']
                else:
                    type_cert:None
                if not obj_count is None or not type_count is None:
                    print(collection_id)
                    returned[collection_id]={"inst_uuid": inst_uuid, "inst_idents": inst_idents, "local_id":  collection, "objects_count":obj_count , "objects_count_uncertainty_level": obj_uncert,"types_count":type_count , "types_count_uncertainty_level": type_cert }
                else:
                    print("!!!!!!! no data found for "+collection_id)
        return returned
        
    def load_collection_overview_logic(self, extra_apis):
        print("mapping coll overview")
        institution_id_field=self.mapping_collection_overview["institution_id_fields"]["institution_name"]
        alternate_institution_name=self.mapping_collection_overview["institution_id_fields"]["alternate_institution_name"]
        appended_excel_summary=self.mapping_collection_overview["appended_excel"]["collection_size"]
        appended_excel_summary_id=appended_excel_summary["col"]
        appended_excel_summary_sheet=appended_excel_summary["sheet"]
        df, update_date=self.read_sheet(self.sheet_collection_overview, 0)
        regex_sheet_id=re.compile("(?<=id\=)(.*?)($|&)", re.I)
        excel_reader=read_excel()
        for i, row in df.iterrows():
            print(i)
            #print(row)
            inst_identifier=row.iloc[int(institution_id_field)]                       
            if str(inst_identifier).lower().strip()=="other":
                inst_identifier=row.iloc[int(alternate_institution_name)]
            print(inst_identifier)
            inst_idents=self.get_other_institution_identifiers("cetaf_complete", inst_identifier)
            print(inst_idents)
            ror_id=inst_idents.get('ror', False) or ''
            cetaf_id=inst_idents.get('cetaf', False) or ''
            r_existing_inst=InstitutionsNormalized.search_by_ident("cetaf", cetaf_id)
            if r_existing_inst is not None:
                if len(r_existing_inst)>0:
                    existing_inst=r_existing_inst.first()
                    print(ror_id)
                    print(cetaf_id)
                    inst_uuid=existing_inst.uuid
                    print(inst_uuid)
                    print(appended_excel_summary_id)
                    excel_detail_url=str(row.iloc[int(appended_excel_summary_id)])
                    print(excel_detail_url)
                    if len(excel_detail_url)>0:
                        print("visit_excel")
                        results=regex_sheet_id.search(excel_detail_url).groups()
                        if len(results)>0:
                            id_xls_detail=results[0]
                            print(id_xls_detail)
                            df_detail, modification_date=excel_reader.get_excel( id_xls_detail, appended_excel_summary_sheet, p_header=0, skiprows=2)
                            print(df_detail)
                            print(modification_date)
                            dict_collec=self.parse_collection_df(inst_uuid, inst_idents, df_detail,cetaf_id, modification_date )
                            print(dict_collec)
                            self.store_collection_in_db( dict_collec, inst_uuid,  modification_date,  preferred_fk_ident='cetaf')
           
            
                
   
        
    def parse_path(self, data, path):
        if len(path)>0:
            first=path.pop(0)            
            parse_first=first.split(":")
            if len(parse_first)==1:
                if isistance(data, dict):
                    if first in data:
                        return self.parse_path(data[first], path)
            elif len(parse_first)==2 :
                field=parse_first[0]
                subpath=parse_first[1]
                if subpath.startswith("@") and len(subpath)>0 and isinstance(data, dict):
                    subpath=subpath[1:]
                    subpath_3=subpath.split("|")
                    if len(subpath_3)==2:
                        subpath_2=subpath_3[0].split("=")
                        if len(subpath_2)==2:
                            if field in data:
                                for elem in data[field]:
                                    if(subpath_2[0] in elem):
                                        if elem[subpath_2[0]]==subpath_2[1]:
                                            return self.parse_path(elem[subpath_3[1]], path)                        
                elif subpath.isnumeric() and isinstance(data, list):
                    subpath_idx=int(subpath)
                    if subpath_idx< len(data):
                        data=data[subpath_idx]
                        return self.parse_path(data, path)
        elif len(path)==0:
            return data
        return None
        
    def get_recursive_in_dict_list(self, data, path):
        returned=None
        #print("=>")
        if len(path)>0:
            first=path.pop(0)
            #print(first)
            #print(path)
            parse_first=first.split(":")
            if len(parse_first)==1:
                if isinstance(data, dict):
                    if first in data:
                        return self.get_recursive_in_dict_list(data[first], path)
                elif isinstance(data, list):
                    returned=[]
                    for elem in data:
                        if isinstance(elem, dict):
                            if first in elem:
                                returned.append(self.get_recursive_in_dict_list(elem[first], path))
                    return returned
            elif len(parse_first)==2 :
                #print("composite")
                field=parse_first[0]
                subpath=parse_first[1]
                #print("field="+field)
                #print("subpath="+subpath)
                ##print(data)
                if subpath.startswith("@") and len(subpath)>0 and isinstance(data, dict):
                    #print("attributeclea")
                    subpath=subpath[1:]
                    subpath_3=subpath.split("|")
                    if len(subpath_3)==2:
                        subpath_2=subpath_3[0].split("=")
                        if len(subpath_2)==2:
                            if field in data:
                                for elem in data[field]:
                                    if(subpath_2[0] in elem):
                                        if elem[subpath_2[0]]==subpath_2[1]:
                                            return self.get_recursive_in_dict_list(elem[subpath_3[1]], path)
                elif subpath.startswith("@") and len(subpath)>0 and isinstance(data, list):
                    returned=[]
                    #print("attributelist")
                    subpath=subpath[1:]
                    subpath_3=subpath.split("|")
                    if len(subpath_3)==2:
                        subpath_2=subpath_3[0].split("=")
                        if len(subpath_2)==2:
                            for data_tmp in data:
                                if field in data_tmp:
                                    for elem in data_tmp[field]:
                                        if(subpath_2[0] in elem):
                                            if elem[subpath_2[0]]==subpath_2[1]:
                                                tmp=self.get_recursive_in_dict_list(elem[subpath_3[1]], path)
                                                returned.append(tmp)
        elif len(path)==0:
            return data
        
        return returned
                        
      
    def parse_array_path(self, p_path_str):
        p_path=p_path_str.split("/")
        return  list(filter(lambda a: a or "" !="" ,p_path))
        
    def parse_array_paths(self, p_paths):
        returned=[]
        for path in p_paths:
            returned.append(self.parse_array_path(path))
        return returned
    
    def add_data_to_dict(self, p_dict, key, source, data, timestamp):
        if not key in p_dict:
            p_dict[key]={}
            p_dict[key]["modification_date"]=timestamp
            p_dict[key]["data"]=[]
            
        p_dict[key]["data"].append({"source": source, "data": data, "modification_date":timestamp})
        if timestamp>p_dict[key]["modification_date"]:
            p_dict[key]["modification_date"]=timestamp
        return p_dict
        
    def load_sheet_logic(self, mapping, p_extra_apis, force):     
        print(p_extra_apis)
        df, update_date=self.read_sheet(self.sheet_institutions, 0)
        self.load_sheet_logic_df( mapping, p_extra_apis, df, update_date, force)
        
    def go_recurs(self, p_row, p_mapping ):
        val={}
        for vid, field in p_mapping.items():
            print("recurs")
            print(vid)
            if isinstance(vid, int):
                if (p_row[vid] or "") !="":
                    val[field]=p_row[vid]
            else:
                val[vid]=self.go_recurs(p_row, p_mapping[vid])
        return val
        
        
    def load_sheet_logic_df(self, mapping, p_extra_apis, df, update_date, force): 
        #data=df.to_json(orient="records")
        ##print(data)
        #all_vals={}
        inst_array={}
        coll_array={}
        recorded_identifiers_all={}
        normalized_identifiers={}
        
        timestamp=mapping["timestamp"]
        timestamp_format=mapping["timestamp_format"] or '%Y-%m-%d %H:%M:%S'
        timestamp_default=mapping["timestamp_default"] or '01/01/0001 00:00:00'
        identifiers=mapping["identifiers_api"]
        identifier_db=mapping["main_identifier_db"]
        identifier_db2=mapping["other_identifiers"]
        for i, row in df.iterrows():
            print("////////////////////////////////////////////////////////// ////////////////////////////////////////////////////////// "+str(i))
            val={}
            name_id=""            
            identifier_db_val=""
            for vid, name in identifier_db["fields"].items():
                if row[vid]!=(identifier_db["null_value"] or ""):
                    identifier_db_val=row[vid]
                    break
            data_date=row[timestamp] or timestamp_default
            data_date=datetime.strptime(data_date, timestamp_format).isoformat()
            recorded_identifiers=[]
            cetaf_ident=self.parse_cetaf_ident(identifier_db_val)
            recorded_identifiers.append({"type":"cetaf", "value": cetaf_ident})
            recorded_identifiers.append({"type":"cetaf_complete", "value": identifier_db_val})
            for vid, name in identifier_db2.items():
                if (row[vid] or "") !="":
                    p_val= str(row[vid])
                    if name=="ror":
                        recorded_identifiers.append({"type":name, "value":p_val, "uri":"https://ror.org/"+p_val})
                    elif name=="grscicoll":
                        tmp_url="https://api.gbif.org/v1/grscicoll/institution?code="+p_val
                        tmp_val=self.go_for_api_logic(tmp_url)
                        tmp_direct_uri="unknown"
                        if "results" in tmp_val:
                            if len(tmp_val["results"])>0:
                                tmp_key=tmp_val["results"][0]["key"]
                                tmp_direct_uri="https://scientific-collections.gbif.org/institution/"+tmp_key
                            else:
                                tmp_url="https://api.gbif.org/v1/grscicoll/institution?alternativeCode="+p_val
                                tmp_val=self.go_for_api_logic(tmp_url)
                                if "results" in tmp_val:
                                    if len(tmp_val["results"])>0:
                                        tmp_key=tmp_val["results"][0]["key"]
                                        tmp_direct_uri="https://scientific-collections.gbif.org/institution/"+tmp_key                       
                        recorded_identifiers.append({"type":name, "value":p_val, "uri":tmp_direct_uri})                        
                    else:
                        recorded_identifiers.append({"type":name, "value":p_val})
            recorded_identifiers_all[identifier_db_val]=recorded_identifiers
            fields=mapping["fields"]
            api_data=mapping["apis"]
            key_for_gescicoll=""
            for vid, name_field_tmp in identifiers.items():       
                name_id=row[vid]
            for vid, field in fields.items():
                #val[field]=self.add_field_value("survey",  row, id )
                if isinstance(vid, int):
                    if (row[vid] or "") !="":
                        val[field]=row[vid]
                else:
                    print("recurs")
                    print(vid)
                    print(fields[vid])
                    val[vid]=self.go_recurs(row, fields[vid])
                    #sys.exit()
            id_grscicoll_collections_from_institutions=None
            for  api, vid in api_data.items():              
                if api in self.extra_apis and api in p_extra_apis:
                    storage_mode="inst"
                    api_data={}
                    api_data_path=None
                    print("go for "+api)
                    key_api=None                    
                    if api=="grscicoll_institutions":
                        key_api=row[vid]
                    elif api=="grscicoll_collections_from_institutions":
                        storage_mode="coll"
                        key_api=id_grscicoll_collections_from_institutions
                        api_data_path= {"collectionName":"/results/collectionName", 
                                        "discipline": "/results/discipline", 
                                        "grscicoll_identifier":"/results/identifier:@identifierType=Collection GRSciColl key|identifierValue"
                                        }
                    else:
                        key_api=row[vid]
                    data=None
                    if key_api or "" !="":
                        data=self.go_for_api(self.extra_apis[api]["endpoint"], key_api )
                    #print(data)
                    if data is not None and storage_mode=="inst" and identifier_db_val !="" :
                        #nio date in GrSciColl
                        self.add_data_to_dict(inst_array, identifier_db_val, api, data, data_date)
                    if api=="grscicoll_institutions" and data is not None:
                        #print("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXx")
                        ##print(data)
                        if "results" in data:
                            #print("TRY GRSICOLL ????????????????????????????????????????????????????????????????? " +key_api)
                            tmp_path=self.parse_array_path("/results:0/identifier:@identifierType=Institution GRSciColl key|identifierValue")
                            print(tmp_path)
                            id_grscicoll_collections_from_institutions=self.parse_path(data["results"], tmp_path)
                            #print("id_grscicoll_collections_from_institutions!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
                            #print(id_grscicoll_collections_from_institutions)
                    if api_data_path is not None:
                        #print(data)
                        print("PATHS=")
                        #paths=self.parse_array_paths(api_data_path)
                        tmp_data={}
                        for key, path_tmp in api_data_path.items():
                            path=self.parse_array_path(path_tmp)
                            tmp_data[key]=self.get_recursive_in_dict_list(data, path) or []
                        #print(tmp_data)
                        if tmp_data is not None:
                            v = [dict(zip(tmp_data,t)) for t in zip(*tmp_data.values())]
                            print(v)
                        if v is not None and identifier_db_val !="":
                            coll_array[identifier_db_val]={"source": "grscicoll", "results":v}                    
                else:
                    print(api + " api has no endpoint in settings")
            self.add_data_to_dict(inst_array, identifier_db_val, "survey", val, data_date)
            #all_vals[name_id]=val
            #if i==1:
            #   sys.exit()
        #print(all_vals)
        for i, inst in inst_array.items():
            print(i)
            print(inst)
        print("------------------------")
        for inst, identifiers in recorded_identifiers_all.items():
            print(identifiers)
            inst_array[inst]["list_identifiers"]=identifiers
            normalized_identifiers[inst]=identifiers
        print(inst_array)
        print("------------------------")
        print(coll_array)
        self.store_inst_in_db(inst_array, normalized_identifiers, force)
        
        
    def get_set_uuid_normalized_inst(self, dict_ids, list_ids):
        if "cetaf" in dict_ids:
            print("TEST_NORMALIZED_INST!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print(dict_ids["cetaf"])
            existing=InstitutionsNormalized.search_by_ident("cetaf", dict_ids["cetaf"])
            print(existing)
            if len(existing)==0:
                print("create")
                inst=InstitutionsNormalized(data={"list_identifiers": list_ids})
                inst.save()
                print(inst)
                #return inst.fpk, inst.uuid
                return inst
            else:
                print("exists")
                inst=existing.first()   
                return inst
  
    def get_set_uuid_normalized_coll(self, uuid_inst, inst_identifiers, coll_id_cetaf, coll_id_all):
        print("TEST_NORMALIZED_COLL!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print(uuid_inst)
        print(coll_id_cetaf)
        existing=CollectionsNormalized.search_by_ident(uuid_inst, "cetaf", coll_id_cetaf)            
        print(existing)
        if len(existing)==0:
            print("create")
            inst_fk=InstitutionsNormalized.search_by_uuid(uuid_inst)
            if inst_fk is not None:
                coll=CollectionsNormalized(fk_institution_normalized=inst_fk, uuid_institution_normalized= uuid_inst, data={"institution_list_identifiers": inst_identifiers, "list_identifiers": coll_id_all})
                coll.save()
                print(coll)
            #return inst.fpk, inst.uuid
                return coll
            return None
        else:
            print("exists")
            coll=existing.first()   
            return coll  
            
            
    
    #https://stackoverflow.com/questions/28170972/django-filter-query-using-database-function
    #https://stackoverflow.com/questions/3467557/proper-way-to-call-a-database-function-from-django
    def store_inst_in_db(self, inst_array, normalized_identifiers,  force=False):
        print("=>")
        print(force)
        versions = Institutions.objects.values('identifier').annotate(max_cetaf=Max('version'))
        dates = Institutions.objects.values('identifier').annotate(max_cetaf=Max('modification_date'))
        fk_lists={}
        for key, list_ids in normalized_identifiers.items():           
            dict_ids={entry["type"]:entry["value"] for entry in list_ids}
            fk_inst=self.get_set_uuid_normalized_inst(dict_ids, list_ids)            
            fk_lists[key]=fk_inst            
        for key, data in inst_array.items():
            version_v=(self.get_max(versions, "identifier", key) or 0)+1            
            #go=True
            last_date=self.get_max(dates, "identifier", key) or datetime.fromisoformat('0001-01-01 00:00:00')
            print("date_db")
            print(last_date.replace(tzinfo=None).isoformat())
            print("date_modification")
            print(data["modification_date"])
            if "data" in data:
                if last_date.replace(tzinfo=None).isoformat()<data["modification_date"] or force:    
                    fk_inst=fk_lists[key]
                    inst=Institutions(identifier=key, fk_institution_normalized=fk_inst,  uuid_institution_normalized=fk_inst.uuid ,data=data, modification_date=data["modification_date"], current=True, version=version_v)
                    inst.save()
                    old_inst = Institutions.objects.filter( Q(identifier=key) & Q(version__lt=version_v) ).update(current=False)
                    print("saved")
                else:
                    print("more recent in db")
         
    
    #source_uri=excel
    def store_collection_in_db(self, collection_list, inst_uuid, modification_date,  preferred_fk_ident='cetaf'):
        print('go_record')
        print("------------------------------")
        print(inst_uuid)        
        print("====> main collection data <=======")
        print(collection_list)
        versions = Collections.objects.values('identifier').annotate(max_cetaf=Max('version'))
        dates = Collections.objects.values('identifier').annotate(max_cetaf=Max('modification_date'))
        for coll_key, coll_data in collection_list.items():
            try:
                print(coll_key)
                print(coll_data)
                inst_identifiers_tmp=coll_data["inst_idents"]
                inst_identifiers=[]
                for type_i, value in inst_identifiers_tmp.items():
                    inst_identifiers.append({"type":type_i, "value": value})
                coll_ids_all=[{"type":"cetaf", "value": coll_key }]
                fk_inst=InstitutionsNormalized.search_by_uuid(inst_uuid)
                if fk_inst is not None:
                    fk_coll=self.get_set_uuid_normalized_coll(inst_uuid, inst_identifiers, coll_key, coll_ids_all)
                    coll_norm_uuid=fk_coll.uuid
                    object_count=coll_data["objects_count"]
                    objects_count_uncertainty_level=coll_data["objects_count_uncertainty_level"]
                    types_count=coll_data["types_count"]
                    types_count_uncertainty_level=coll_data["types_count_uncertainty_level"]
                    
                    print(coll_norm_uuid)
                    if object_count is not None or types_count is not None:
                        version_v=(self.get_max(versions, "identifier", coll_key) or 0)+1
                        last_date=self.get_max(dates, "identifier", coll_key) or datetime.fromisoformat('0001-01-01 00:00:00')
                        if last_date.replace(tzinfo=None)<modification_date.replace(tzinfo=None):
                            json_data={}
                            data={}
                            data["data"]={}
                            data["data"]["description"]={}
                            go=False
                            if object_count is not None:
                                if int(object_count)>0:
                                    data["data"]["description"]["objects_count"]=object_count
                                    data["data"]["description"]["objects_count_uncertainty_level"]=objects_count_uncertainty_level
                                    go=True
                            if types_count is not None:
                                if int(types_count) >0:
                                    data["data"]["description"]["types_count"]=types_count
                                    data["data"]["description"]["types_count_uncertainty_level"]=types_count_uncertainty_level
                                    go=True
                            if go:
                                data["institution_list_identifiers"]=inst_identifiers
                                data["list_identifiers"]=coll_ids_all
                                coll=Collections(uuid_institution_normalized=fk_inst.uuid, fk_institution_normalized=fk_inst, fk_collection_normalized =fk_coll,  identifier=coll_key, data=data, current=True, version=version_v )
                                coll.save()
                                coll = Collections.objects.filter( Q(identifier=coll_key) & Q(version__lt=version_v) ).update(current=False)
                        else:
                            print("more recent in db")                
                
                else:
                   print("PB : INST_NOT_FOUND")
            except Exception as e:
                #print ("Error: unable to fetch data")
                #print(e)
                #traceback.print_exc()
                print("Error: unable to fetch data_DELETE")
                print(e)
                print(traceback.print_exc())
                #sys.exit()
        #versions = Collections.objects.values('identifier').annotate(max_cetaf=Max('version'))
        #dates = Collections.objects.values('identifier').annotate(max_cetaf=Max('modification_date'))
        """
        returned[collection_id]={"inst_uuid": inst_uuid, "inst_idents": inst_idents, "local_id":  collection, "objects_count":obj_count , "objects_count_uncertainty_level": obj_uncert,"types_count":type_count , "types_count_uncertainty_level": type_cert }
        get_set_uuid_normalized_coll(self, uuid_inst, cetaf_main_id, list_ids_institution, list_ids):
        """
        """
        for full_cetaf_id, data in collection_list.items():
            latest=Collections.objects.filter(identifier=full_cetaf_id).latest("version")
            inst_uuid=data["inst_uuid"]
            inst_idents=data["inst_idents"]
            local_id=data["local_id"]
            coll_id=["cetaf":full_cetaf_id, "local_name": collection]
            fk_coll=self.get_set_uuid_normalized_coll(inst_uuid, full_cetaf_id, inst_idents, coll_id)
            #version_v=(self.get_max(versions, "identifier", full_cetaf_id) or 0)+1
            if latest is not None:
                version_v=latest.version + 1
                last_date=latest.modification_date
                source=latest.source_uri
            else:
                version_v=1
                last_date=datetime.fromisoformat('0001-01-01 00:00:00')
                source=""
            if source_uri != source:

            else:
                
            #go=True
            #last_date=self.get_max(dates, "identifier", full_cetaf_id) or datetime.fromisoformat('0001-01-01 00:00:00')
            last.date
        """
    
    def get_max(self, rs, field, value):
        #https://stackoverflow.com/questions/49875288/django-filter-field-as-a-string
        test=rs.values("max_cetaf").filter(**{field: value})
        print(test)
        if len(test)>0:            
            return test.all()[0]["max_cetaf"]
        else:
            return None
            
            
    def read_sheet(self, ws_name, idx_sheet):
        print(self.gs_auth_file)
        client = pygsheets.authorize(service_file=self.gs_auth_file)
        sh = client.open_by_key(ws_name)
        wks=sh.worksheet('index',idx_sheet)
        df = wks.get_as_df()
        identifier = df.columns.to_series().groupby(level=0).transform('cumcount')
        df.columns = df.columns.astype('string') + identifier.astype('string')
        return df, sh.updated