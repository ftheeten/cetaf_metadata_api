from django.conf import settings
from .helper import print_date, norm_str, val_not_none, extract_field, del_json_none_logic
from ..models import GoogleSheetIndexResponses,  GoogleSheetCollectionReply, InstitutionsNormalized, Collections, CollectionsNormalized
from django.db.models import Max
from django.db.models import Q
from django.forms.models import model_to_dict
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from datetime import datetime
from .read_excel import read_excel
from io import BytesIO, StringIO
import pandas as pnd
import ast
import validators
from oauth2client.service_account import ServiceAccountCredentials
#from collections import OrderedDict
import numpy as np
from urllib.parse import urlparse
from urllib.parse import parse_qs
from .form_mapping.FormMappingGeoRegions import FormMappingGeoRegions
from .form_mapping.FormMappingCountries import FormMappingCountries
from .form_mapping.FormMappingStorage import FormMappingStorage
from .external_api_mapping.ext_mapping_grscicoll_collections import ExtMappingGrSciCollCollections
import json

import traceback

import sys



# SHARE folder to TO franck-theeten@cetafapi.iam.gserviceaccount.com to allow reading
#https://stackoverflow.com/questions/60736955/how-to-connect-pydrive-with-an-service-account
#https://www.merge.dev/blog/get-folders-google-drive-api



class GSCetafCollectionsParser():

    uuid_folder=None
    gs_auth_file=None
    g_drive=None
    SCOPES = ["https://www.googleapis.com/auth/drive.metadata.readonly"]
    list_files={}
    list_index_files={}
    inst_not_found=[]
    coll_grsci_coll_not_found=[]
    colls_code_mapper={}
    found_colls=[]
    not_found_colls=[]
    
    def __init__(self, p_uuid_folder):
        self.uuid_folder=p_uuid_folder
        self.gs_auth_file=settings.GOOGLE_AUTH_FILE       
        
    def get_gs_content(self, p_file, p_mime_type):
        p_file.FetchContent( p_mime_type)
        returned= p_file.content
        return returned
    
    def explore_drive(self):
        gauth =GoogleAuth()
        scope = ["https://www.googleapis.com/auth/drive"]
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(self.gs_auth_file, scope)
        self.g_drive = GoogleDrive(gauth)
        #version=GoogleSheetIndexResponses.objects.aggregate(Max('version'))
        #version_v=(version["version__max"] or 0) +1
        #excel_reader=read_excel()
        self.explore_drive_logic(self.uuid_folder)
        #old_reply = GoogleSheetIndexResponses.objects.filter(  Q(version__lt=version_v) ).update(current=False)
    
    """
    def explore_drive_old(self):
        gauth =GoogleAuth()
        scope = ["https://www.googleapis.com/auth/drive"]
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(self.gs_auth_file, scope)
        self.g_drive = GoogleDrive(gauth)
        version=GoogleSheetCollectionReply.objects.aggregate(Max('version'))
        version_v=(version["version__max"] or 0) +1
        excel_reader=read_excel()
        self.explore_drive_recurs_old(self.uuid_folder,excel_reader, version_v)
        old_reply = GoogleSheetCollectionReply.objects.filter(  Q(version__lt=version_v) ).update(current=False)
    """
    
    def get_gs_data(self, p_file, p_excel_reader ):
        data=None 
        sheets_str=None
        df=None
        if "exportLinks" in p_file:
            export_links=p_file["exportLinks"]
            #print(export_links)
        else:
            export_links=[]
        export_links=export_links or []
        try:
            if 'application/x-vnd.oasis.opendocument.spreadsheet' in export_links:
                #print('CASE_1')
                content=self.get_gs_content(p_file, 'application/x-vnd.oasis.opendocument.spreadsheet')                
                df = pnd.read_excel(content, sheet_name=None, header=0, skiprows=0, engine="odf")
            elif 'application/vnd.oasis.opendocument.spreadsheet' in export_links:
                #print('CASE_2')
                content=self.get_gs_content(p_file, 'application/vnd.oasis.opendocument.spreadsheet' )
                df = pnd.read_excel(content, sheet_name=None, header=0, skiprows=0, engine="odf")
            elif "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"  in export_links:
                #print('CASE_3')
                content=self.get_gs_content(p_file, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                df = pnd.read_excel(content, sheet_name=None, header=0, skiprows=0, engine="odf")                
            else:               
                #print("NO_CASE")
                df=p_excel_reader.get_gs_by_id(p_file["id"], p_header=0)
                #print(df)
                #print("test_raw_google2")
                #sys.exit()
        except: 
            print("EXCEPTION")
            traceback.print_exc()
            #sys.exit()
        if df is not None:
            sheets=[]
            #sheets_str=",".join(sheets)
            data={}
            for kd,vd in df.items():
                kd=norm_str(kd)
                vd=vd.reset_index()
                vd=p_excel_reader.panda_unique_cols(vd)
                data[kd]=vd.to_json()
                sheets.append(kd) 
            sheets_str=str(sheets)
            
        return data, sheets_str
    
  
    
    
    def process_collections_details_from_reply_index(self):
        #normally initialized before this function, remove wne prod
        gauth =GoogleAuth()
        scope = ["https://www.googleapis.com/auth/drive"]
        gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(self.gs_auth_file, scope)
        self.g_drive = GoogleDrive(gauth)
        #
        excel_reader=read_excel()
        self.inst_not_found=[]
        self.coll_grsci_coll_not_found=[]
        cetaf_ids=InstitutionsNormalized.get_all_identifiers_by_protocol("cetaf")
        data=GoogleSheetIndexResponses.objects.filter(current=True)
        version=GoogleSheetCollectionReply.objects.aggregate(Max('version'))
        version_v=(version["version__max"] or 0) +1
        for d in data:
            #print("----------------------------------")
            print(d.google_id)
            #google_src_id=d.google_id
            #print(d.title)
            #print(d)
            pnd_coll_metadata=self.get_df_data(d.data)
            #print(pnd_coll_metadata)
            if len(pnd_coll_metadata)>0:
                list_sheets=d.list_sheets
                if val_not_none(list_sheets):
                    #print(list_sheets)
                    list_sheets_l = ast.literal_eval(list_sheets)
                    #print(list_sheets_l)
                    if len(list_sheets_l)>0:
                        key=list_sheets_l[0]
                        #print(key)
                        if key in pnd_coll_metadata:
                            
                            main_sheet=pnd_coll_metadata[key]
                            main_sheet.columns = [norm_str(x) for x in main_sheet.columns]
                            main_sheet = main_sheet.replace({np.nan: None})
                            #print(main_sheet)
                            cols=main_sheet.columns.to_list()
                            #print(cols)
                            pos_inst=None
                            if 'select_institution_name' in cols:
                                pos_inst=cols.index('select_institution_name')                                
                            elif 'list_of_institutions' in cols:
                                pos_inst=cols.index('list_of_institutions')  
                            elif 'institution(s)' in cols:
                                #expertise
                                pass
                            #else:
                            #    
                            #    print("DEBUG_EXIT_3")
                            #    print(cols)
                            #    #sys.exit()
                            #print(pos_inst)
                            if pos_inst is not None and pos_inst<len(cols) :
                                pos_xls=None
                                if 'upload_your_filed_xls_file' in cols:
                                    pos_xls=cols.index('upload_your_filed_xls_file')
                                #else:
                                #    print("DEBUG_EXIT_4")
                                #    #sys.exit()
                                if pos_xls is not None:
                                    #print(pos_xls)
                                    self.explore_data_from_index_sheet(d, d.google_id, cetaf_ids, excel_reader, main_sheet,pos_inst, pos_xls, d.path, version_v)
                        #else:
                        #    print("DEBUG_EXIT_2")
                        #    sys.exit()
                '''
                print("----------------------------------")
                print(pnd_coll_metadata.keys())
                key_1=list(pnd_coll_metadata.keys())[0]
                main_sheet=pnd_coll_metadata[key_1]
                print(key_1)
                print(main_sheet)
                '''
            #else:
            #    print("DEBUG_EXIT_1")
            #    sys.exit()
        #invalidate old data
        #print("current_version="+str(version_v))
        old_reply = GoogleSheetCollectionReply.objects.filter(  Q(version__lt=version_v) ).update(current=False)
        #print("olders_versions not current")
        #print("INSTITUTIONS_NOT_FOUND")
        #for ins in self.inst_not_found:
        #    print(ins)
       
    
        
        
    def explore_data_from_index_sheet(self, p_reply_obj,  p_google_src_id, p_cetaf_ids, p_excel_reader, p_sheet, p_index_inst, p_index_sheet, p_hierarch, p_version_v):
        #print(":::::::::::::::)")
        for index, row in p_sheet.iterrows():
            metadata=json.loads(row.to_json())
            metadata.pop("index")
            #print(metadata)
            if "timestamp" in metadata:
                metadata["timestamp"]=datetime.fromtimestamp(int(metadata["timestamp"])/1000).isoformat()
            #print(metadata)
            #sys.exit()
            idx_others=p_index_inst+1
            inst=row.iloc[p_index_inst]
            oth=row.iloc[idx_others]
            data_link=row.iloc[p_index_sheet]
            #print([inst, oth, data_link])
            if oth is not  None:
                inst=oth
            if inst is not None:
                inst_a=inst.split(" ")
                if len(inst_a)>0:
                    acronym=inst_a[0]
                    #print(acronym)
                    inst_uuid, inst_id=self.get_inst_from_title(p_cetaf_ids,acronym)
                    if inst_uuid is not  None:
                        if data_link is not None:                            
                            parsed_url = urlparse(data_link)
                            captured_value = parse_qs(parsed_url.query)['id'][0]
                            #print("uuid inst=")
                            #print(inst_uuid)
                            #print("uuid files=")
                            #print(captured_value)
                            self.explore_gs_file(  p_reply_obj, p_google_src_id, p_excel_reader, inst_uuid, inst_id, captured_value, metadata, p_hierarch, p_version_v)
                    else:
                        #print("DEBUG_EXIT_5")
                        self.inst_not_found.append(inst_a)
                
                    
    def explore_gs_file(self, p_reply_obj, p_google_src_id, p_excel_reader, uuid_inst, cetaf_id_inst, id_file, p_metadata_from_index,   p_hierarch, p_version_v):
        metadata = dict( id = id_file )
        #print(id_file)
        #print(cetaf_id_inst)
        #sys.exit()
        #print(metadata)
        google_file  = self.g_drive.CreateFile( metadata = metadata )
        google_file.FetchMetadata()
        google_file.FetchContent()
        metadata=google_file.metadata
        #tmp=google_file.GetContentFile( filename = id_file )
        '''
        google_file_tmp=self.list_files[id_file]
        google_file=google_file_tmp["file"]
        path=google_file_tmp["path"]
        '''
        #print(google_file)
        data=None
        sheets_str=None
        data, sheets_str=self.get_gs_data(google_file, p_excel_reader)
        #print(data)
        #print(sheets_str)
        if data is not None:           
            importation_time=datetime.now().strftime("%Y%m%d-%H%M%S")
            p_google_src_id_clone=p_google_src_id.copy()
            p_google_src_id_clone[metadata[ 'title']]=settings.GOOGLE_SHEET_URL+metadata['id']
            reply=GoogleSheetCollectionReply(fk_index_response=p_reply_obj, google_id= p_google_src_id_clone, institution_uuid=uuid_inst, institution_cetaf_acronym=cetaf_id_inst,  mime_type= metadata[ 'mimeType'], title=metadata[ 'title'], path= p_hierarch,  list_sheets=sheets_str, data=data, metadata_from_index=p_metadata_from_index,  modified_date=metadata[ 'modifiedDate'], harvesting_date=importation_time, version=p_version_v, current=True)
            reply.save()
        #else:
        #    print("DEBUG_EXIT_PB_GET_DATA")
        #    sys.exit()
    
    def explore_drive_recurs(self, p_uuid_folder, p_hierarch=[]):
        tmp = self.g_drive.ListFile({'q': "'%s' in parents and trashed=false" % p_uuid_folder}).GetList() 
        for file in tmp:
            if file['mimeType']=="application/vnd.google-apps.folder":
                p_hierarch_recurs=p_hierarch.copy()
                p_hierarch_recurs.append(file['title'])
                self.explore_drive_recurs(file['id'], p_hierarch_recurs)
            else:
                title=file[ 'title'].lower()
                if "responses" in title:
                    self.list_index_files[file['id']]={"file": file, "path": p_hierarch}
                else:                    
                    self.list_files[file['id']]={"file": file, "path": p_hierarch}
    
    def explore_drive_logic(self, p_uuid_folder):
        self.list_files={}
        self.list_index_files={}
        excel_reader=read_excel()
        version=GoogleSheetIndexResponses.objects.aggregate(Max('version'))
        version_v=(version["version__max"] or 0) +1
        #as the API can't access file didrectly but can only browse
        print('browse drive')
        print_date()
        self.explore_drive_recurs(p_uuid_folder)
        print('drive explored')
        print_date()
        #
        file_list = self.g_drive.ListFile({'q': "'%s' in parents and trashed=false" % p_uuid_folder}).GetList()
        for key, file_tmp in self.list_index_files.items():
            print(key)
            print(file_tmp)
            file=file_tmp["file"]
            path=file_tmp["path"]
            title=file[ 'title'].lower()
            data, sheets_str=self.get_gs_data( file, excel_reader)
            importation_time=datetime.now().strftime("%Y%m%d-%H%M%S")
            google_src_list={}
            google_src_list["main"]=settings.GOOGLE_SHEET_URL+file['id']
            reply=GoogleSheetIndexResponses(google_id=google_src_list, title=title, list_sheets=sheets_str, path=path, data=data, modified_date=file[ 'modifiedDate'], harvesting_date=importation_time, version=version_v, current=True )
            reply.save()
        old_reply = GoogleSheetIndexResponses.objects.filter(  Q(version__lt=version_v) ).update(current=False)
        """
        for file in file_list:            
            if file['mimeType']=="application/vnd.google-apps.folder":
                p_hierarch_recurs=p_hierarch.copy()
                p_hierarch_recurs.append(file['title'])
                self.explore_drive_recurs(file['id'], p_excel_reader, p_version_v, p_hierarch_recurs)
            else:                
                #print(file)
                title=file[ 'title'].lower()
                if "responses" in title:
                    print(title)                    
                    data, sheets_str=self.get_gs_data( file, p_excel_reader)
                    #print(data)
                    #print(sheets_str)
                    importation_time=datetime.now().strftime("%Y%m%d-%H%M%S")
                    reply=GoogleSheetIndexResponses(google_id=file['id'], title=title, list_sheets=sheets_str, path=p_hierarch, data=data, modified_date=file[ 'modifiedDate'], harvesting_date=importation_time, version=p_version_v, current=True )
                    reply.save()
        """
    
        
    
     
    def get_inst_from_title(self, cetaf_identifiers, p_title):
        returned=None, None
        #print("----------------------------->")
        #print(p_title)
        p_title=p_title.replace(' ','-').replace('_','-').replace('(','').replace(')','').upper()
        #print(p_title)
        for item in cetaf_identifiers:
            uuid=item["uuid"]
            val=item["value"]
            val2=val.replace(' ','-').replace('_','-').replace('(','').replace(')','').upper()
            #print(val2)
            if val2 in p_title:
                return uuid, val
        return returned
        
    
        
    def import_collections(self):        
        for key, code in settings.COLLECTION_ACRONYMS_MAPPING.items():
            key=key.upper()
            code=code.upper().replace(" ","")
            key=key.upper().replace(" ","")
            self.colls_code_mapper[key]=code
            if "/" in key:
                key=key.replace("/","-")
                self.colls_code_mapper[key]=code                
                if key.startswith("ZOOLOGYVERTEBRATE") or key.startswith("ZOOLOGYINVERTEBRATE"):
                    key=key.replace("ZOOLOGY","")
                    self.colls_code_mapper[key]=code
        print(self.colls_code_mapper)
        #sys.exit()
                    
        version=Collections.objects.aggregate(Max('version'))
        version_v=(version["version__max"] or 0) +1
        data=GoogleSheetCollectionReply.objects.filter(list_sheets__icontains='geographic_region', current=True).exclude(institution_uuid__isnull=True)
        #print(len(data))
        for coll in data:
            #print(coll)
            if coll.data is not None:
                metadata_from_index=coll.metadata_from_index
                #google_id_reply_index=coll.fk_index_response.google_id
                google_id_reply_index=coll.google_id
                #print("------------------------------------------------------------------------------------------------------")
                #print(coll.google_id)
                #print(coll.title)
                inst_norm_uuid=coll.institution_uuid
                inst_acronym=coll.institution_cetaf_acronym
                path=coll.path
                path_list = ast.literal_eval(path)
                #print(path_list)
                coll_acronym=None
                if len(path_list)>0:
                    coll_acronym=path_list[-1]
                    #print(coll_acronym)
                    #sys.exit()
                #print(coll.google_id+"\t"+coll.title+"\t"+"https://docs.google.com/spreadsheets/d/"+coll.google_id)
                
                pnd_coll=self.get_df_data(coll.data)
                modification_date=coll.modified_date
                #coll_agg=pnd_coll.columns.values
                #print(coll_agg)
                self.parse_df_data(pnd_coll, metadata_from_index, google_id_reply_index, inst_norm_uuid, inst_acronym, coll_acronym, modification_date, version_v)
        #pb if collection removed from survey
        old_coll = Collections.objects.filter(  Q(version__lt=version_v) ).update(current=False)
        print("COLLECTIONS_NOT_IN_GRSCICOLL")
        for coll in self.coll_grsci_coll_not_found:
            print(coll)   
        self.found_colls.sort()
        print("TYPES_OF_COLLECTIONS")
        print(self.found_colls)
        self.not_found_colls.sort()
        print("COLL_UNK_ACRONYM")
        print(self.not_found_colls)
        
    def get_df_data(self, p_data):
        #print(p_data)
        #print(p_data.keys())
        returned={}
        for key, val in p_data.items():
            df=pnd.read_json(StringIO(val))
            #print(df)
            returned[key.lower().replace(" ","_")]=df
        return returned
        #pass

    def parse_reply_index_metadata(self, p_index_metadata, p_inst_acronym, p_coll_acronym ):
        dict_general_metadata={}
        dict_identifiers=[]
        dict_general_metadata["contact"]={}
        dict_general_metadata["contact"]["curator"]={}
        dict_general_metadata["contact"]["curator"]["orcid"]=extract_field(p_index_metadata,"orcid_id_of_the_main_person_in_charge_of_the_collection")
        dict_general_metadata["contact"]["curator"]["mail"]=extract_field(p_index_metadata,"contact_email_of_the_collection")

        dict_general_metadata["description"]={}
        dict_general_metadata["description"]["abstract"]=extract_field(p_index_metadata,"abstract")
        dict_general_metadata["description"]["comments"]=extract_field(p_index_metadata,"additional_information")
        dict_general_metadata["geographic_coverage"]={}
        dict_general_metadata["geographic_coverage"]["description"]=extract_field(p_index_metadata,"geography:_free_text_description")
        dict_general_metadata["geographic_coverage"]["description"]=extract_field(p_index_metadata,"geographical_coverage_bounding_box")

        dict_identifiers.append({"type":"cetaf", "value": p_inst_acronym+' '+p_coll_acronym})
        grscicoll=extract_field(p_index_metadata,"gbif_registry_of_scientific_collections_code_s")
        if grscicoll is not None:
            grscicoll=str(grscicoll)
            grscicoll_split=grscicoll.split(" ")
            
            for c in grscicoll_split:
                if validators.url(c) and "/collection/" in c:
                    #TO DO check code of collection from grscicoll url                    
                    g_val=ExtMappingGrSciCollCollections.TestGrsciCollURL(c)
                    dict_identifiers.append({"type": "grscicoll", "url":c, "value": g_val})
                else:
                    grscicoll_code, grscicoll_url=ExtMappingGrSciCollCollections.GetUUIDFromCode(c)
                    if grscicoll_url is not None:
                        dict_identifiers.append({"type": "grscicoll", "url":grscicoll_url, "value": c})
                        #print(dict_identifiers)
                        #print("interrupt")
                        #sys.exit()
                    elif c is not None:
                        #dict_identifiers.append({"type": "grscicoll", "value": grscicoll})
                        self.coll_grsci_coll_not_found.append({"inst":p_inst_acronym, "coll":p_coll_acronym, "submitted_code": c})
                    else:
                        #print(p_inst_acronym)
                        #print(p_coll_acronym)
                        #sys.exit()
                        self.coll_grsci_coll_not_found.append({"inst":p_inst_acronym, "coll":p_coll_acronym })

        
        dict_general_metadata=del_json_none_logic(dict_general_metadata)
        return dict_general_metadata, dict_identifiers



    def parse_df_data(self, p_pnd, p_index_metadata, p_google_id_reply_index,p_institution_norm_uuid, p_inst_acronym, p_coll_acronym,  p_modified_date, version_v ):
        geo_reg=None
        geo_ctry=None    
        #print("google_id_reply="+str(p_google_id_reply_index))        
        #print(p_pnd.keys())
        #print(p_index_metadata)
        p_metadata_to_add, list_identifiers=self.parse_reply_index_metadata(p_index_metadata, p_inst_acronym, p_coll_acronym )
        print(p_coll_acronym)
        
        #print(p_metadata_to_add)
        #print(list_identifiers)
        
        #voc not standardized !
        df_main_coll={}
        df_sub_colls={}
        if "geographic_region" in  p_pnd:
            p_pnd["geographic_regions"]=p_pnd.pop("geographic_region")            
        if "geographic_regions" in p_pnd:
            geo_reg=p_pnd["geographic_regions"]            
            geo_reg=FormMappingGeoRegions.get_df(geo_reg, [0,1,2], {1:"continents", 2: "measurements" })            
            geo_reg_json, geo_reg_json_details=FormMappingGeoRegions.get_json(geo_reg)
            #print("REGIONS_MAIN=")
            #print(geo_reg_json)
            df_main_coll["geo_regions"]=geo_reg_json            
            #print("REGIONS_DETAILS=")
            #print(geo_reg_json_details)
            df_sub_colls["geo_regions"]=geo_reg_json_details
            
            #sys.exit()
            #cols=geo_reg.columns
        if "storage" in p_pnd:
            storage=p_pnd["storage"]            
            storage=FormMappingStorage.get_df(storage, [0,1,2], {0:"storage_type", 1: "definition", 2: "measurements" })            
            storage_json, storage_json_details=FormMappingStorage.get_json(storage)
            #print("STORAGE_MAIN=")
            #print(storage_json)
            df_main_coll["storage"]=storage_json
            #print("STORAGE_DETAILS=")
            #print(storage_json_details)
            df_sub_colls["storage"]=storage_json_details
        if "countries" in p_pnd:
            geo_ctry=p_pnd["countries"]
            geo_ctry=FormMappingCountries.get_df(geo_ctry, p_rename={0: "country"})            
            #for key, row in geo_ctry.iterrows():
            #    print(row)
            geo_ctry=FormMappingCountries.align_header_collection_name(geo_ctry)
            geo_ctry_json, geo_ctry_json_details=FormMappingCountries.get_json(geo_ctry)
            #print("COUNTRIES_MAIN=")
            #print(geo_ctry_json)
            df_main_coll["countries"]=geo_ctry_json
            #print("COUNTRIES_DETAILS=")
            #print(geo_ctry_json_details)
            df_sub_colls["countries"]=geo_ctry_json_details
            #sys.exit()
        self.aggregate_and_create_colls( version_v, p_modified_date, p_google_id_reply_index, p_institution_norm_uuid, p_inst_acronym, p_coll_acronym, list_identifiers, p_metadata_to_add, df_main_coll, df_sub_colls)
        #print("AGG_SUB_COLLS")
        #print(agg_sub_colls)
        #else:
        #    print("EXIT_20")
        #    sys.exit()
            

    def get_set_uuid_normalized_coll(self, dict_ids, norm_inst, list_ids, list_ids_inst):
        if "cetaf" in dict_ids:            
            existing=CollectionsNormalized.search_by_ident(norm_inst.uuid, "cetaf", dict_ids["cetaf"])
            #print(existing)
            if len(existing)==0:                
                coll=CollectionsNormalized(uuid_institution_normalized=norm_inst.uuid, fk_institution_normalized=norm_inst, local_identifier=dict_ids["cetaf"], data={"list_identifiers": list_ids, "parent_institution_list_identifiers": list_ids_inst})
                coll.save()                
                #return inst.fpk, inst.uuid
                return coll
            else:     
                
                inst=existing.first()   
                return inst            
    
    def build_obj_coll_json(self, p_metadata, p_google_sheet_url, p_source, p_cetaf_institution_acronym, p_inst_identifiers, p_coll_identifiers, p_coll, p_modified_date):
        print("--------------------------------IN_PCOLL")
        print(p_coll)
        #main_coll
        dict_coll={}
        dict_coll={}
        dict_coll["data_list"]=[]
        coll_data={}
        coll_data["main_metadata"]=p_metadata
        coll_data["collection_data"]={}
        coll_data["collection_data"]["geospatial_coverage"]={}
        if "countries" in p_coll:
             coll_data["collection_data"]["geospatial_coverage"]["country_list"]=p_coll["countries"]        
        if "geo_regions" in p_coll:
             coll_data["collection_data"]["geospatial_coverage"]["geographic_areas"]=p_coll["geo_regions"]
        if "storage" in p_coll:
            coll_data["collection_data"]["storage"]=p_coll["storage"]
        record={}
        record["source_url"]=p_google_sheet_url
        record["data"]=coll_data
        record["source"]=p_source
        #print(p_modified_date)
        record["modification_date"]=p_modified_date.isoformat()
        #local_ident=None
        #if p_cetaf_institution_acronym is not None and rework_idents["cetaf"] is not None:
        #    local_ident=rework_idents["cetaf"].replace(p_cetaf_institution_acronym,"").strip()
        dict_coll["data_list"].append(record)
        dict_coll["list_identifiers"]=p_coll_identifiers
        dict_coll["parent_institution_list_identifiers"]=p_inst_identifiers
        return dict_coll
        
    def create_links_to_parent_or_child_collections(self, cetaf_identiers):
        returned=[]
        for key, val in cetaf_identiers.items():
            code=val["code"]
            institution=val["institution"]  
            name=val["name"]               
            full_id=institution.strip()+ " "+code.strip()
            full_name=institution.strip()+ " "+name
            url=settings.INTERNAL_COLLECTION_LINK+code
            dict_coll={"type": "cetaf", "value": full_id, "uri":url, "name": full_name }
            returned.append(dict_coll)
        return returned
            
    

    
    
    def aggregate_and_create_colls(self, p_version_v, p_modified_date, p_google_id_reply_index, p_institution_norm_uuid, p_cetaf_institution_acronym, p_original_collection_name, p_coll_identifiers, p_metadata, p_main_coll, p_subcolls={}):
        norm_inst=None
        norm_inst_rs=InstitutionsNormalized.objects.filter(uuid=p_institution_norm_uuid)
        #print("insts=")
        #print(p_institution_norm_uuid)
        #print(norm_inst_rs)
        #print(len(norm_inst_rs))
        inst_identifiers=None
        if len(norm_inst_rs)>0:
            norm_inst=norm_inst_rs.first()  
            #print(norm_inst.data)
            if norm_inst.data is not None:
                if "list_identifiers" in norm_inst.data:
                    inst_identifiers=norm_inst.data["list_identifiers"]
        #print(inst_identifiers)
        rework_idents={}
        for item in p_coll_identifiers:
            rework_idents[item["type"]]=item["value"]
        #print(p_cetaf_institution_acronym)
        #print(p_coll_identifiers)
        #print(rework_idents)
        norm_coll=self.get_set_uuid_normalized_coll(rework_idents, norm_inst,p_coll_identifiers, inst_identifiers)
        #print(norm_coll)
        #sys.exit()
        
        #subcoll
        list_sub_colls={}
        list_sub_coll_idents=[]
        if "cetaf" in rework_idents:
            parent_coll_id={"cetaf": {"code": rework_idents["cetaf"], "institution": p_cetaf_institution_acronym, "name": p_original_collection_name}}
            parent_coll_id_json=self.create_links_to_parent_or_child_collections(parent_coll_id)
            print("GOOGLE_ID:")
            print(p_google_id_reply_index)
            #google_sheet_src_url=settings.GOOGLE_SHEET_URL+p_google_id_reply_index
            #google_sheet_src_url_list={}
            #google_sheet_src_url_list["main"]=google_sheet_src_url
            print(parent_coll_id_json)
            #sys.exit()
            print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
            print(rework_idents["cetaf"])
            print("p_subcolls")
            print(p_subcolls)
            for key, coll in p_subcolls.items():
                print("???????????????")
                print(key)
                print(coll)
                if key=="countries" or key=="geo_regions":
                    print("-------------------")
                    print(key)
                    for subcoll_code, subcoll_content in coll.items():
                        print("GO")
                        print("COLL_2_"+subcoll_code)
                        print(subcoll_content)
                        if not subcoll_code in list_sub_colls:
                            list_sub_colls[subcoll_code]={}
                        if not "geographic_coverage" in list_sub_colls[subcoll_code]:
                            list_sub_colls[subcoll_code]["geographic_coverage"]={}
                        if key=="countries":
                            list_sub_colls[subcoll_code]["geographic_coverage"]["country_list"]=subcoll_content
                        elif key=="geo_regions":
                            list_sub_colls[subcoll_code]["geographic_coverage"]["count_by_ecoregions"]=subcoll_content
                elif key=="storage":
                    for subcoll_code, subcoll_content in coll.items():
                        if not subcoll_code in list_sub_colls:
                            list_sub_colls[subcoll_code]={}
                        list_sub_colls[subcoll_code]["storage"] =subcoll_content
            print("AGGREGATED_SUB_COLL")
            print(list_sub_colls)
            #if rework_idents["cetaf"]=='BE-RBINS GEO':
            #    print("SYS_EXIT1")
            #    sys.exit()
            #main_coll 
            
            local_ident=None
            if p_cetaf_institution_acronym is not None and rework_idents["cetaf"] is not None:
                local_ident=rework_idents["cetaf"].replace(p_cetaf_institution_acronym,"").strip()
            dict_main_coll=self.build_obj_coll_json( p_metadata, p_google_id_reply_index, "cetaf_survey", p_cetaf_institution_acronym, inst_identifiers, p_coll_identifiers, p_main_coll, p_modified_date)
            
            
            parent_identifier=rework_idents["cetaf"]
            parent_identifier_uuid=norm_coll.uuid
            print("---------------------------------------------------------------------------------------------------------------------------")
            print(rework_idents["cetaf"])
            print(dict_main_coll)
            print("-")
            new_coll=Collections(uuid_institution_normalized=p_institution_norm_uuid, uuid_collection_normalized=norm_coll.uuid, local_identifier=local_ident, identifier= rework_idents["cetaf"],source_uri=p_google_id_reply_index,  fk_collection_normalized= norm_coll, data={}, fk_institution_normalized=norm_inst, version=p_version_v, current=True)
            new_coll.save()    
            #print(new_coll.uuid)
            uuid_main=new_coll.uuid
            print("AGG_SUB_COLL")
            for key_sub_coll, content_subcoll in list_sub_colls.items():
                key_sub_coll=key_sub_coll.replace(",","")
                #print(key_sub_coll)
                acronym_coll="? "+key_sub_coll
                if not key_sub_coll in self.found_colls:
                    self.found_colls.append(key_sub_coll)
                if key_sub_coll in self.colls_code_mapper:
                    acronym_coll=self.colls_code_mapper[key_sub_coll]
                else:
                    self.not_found_colls.append(key_sub_coll)
                
                print("===> CONTENT_SUBCOLL")
                print(parent_identifier)
                print(key_sub_coll)
                print(content_subcoll)
                sub_coll_metadata=p_metadata.copy()
                sub_coll_metadata.pop('description', None)
                sub_coll_metadata.pop('geographic_coverage', None)
                #sub_coll_metadata.pop('list_identifiers', None)
                go_sub_coll=False
                ctry_list=None
                regions_list=None
                storage_list=None
                reworked_sub_coll={}
                if "geographic_coverage" in content_subcoll:
                    if "count_by_ecoregions" in content_subcoll["geographic_coverage"]:
                        if len(content_subcoll["geographic_coverage"]["count_by_ecoregions"]  )>0:
                            reworked_sub_coll["geo_regions"]=content_subcoll["geographic_coverage"]["count_by_ecoregions"]
                            go_sub_coll=True
                    if "country_list"  in content_subcoll["geographic_coverage"]:
                        if len(content_subcoll["geographic_coverage"]["country_list"]  )>0:                            
                            reworked_sub_coll["countries"]=content_subcoll["geographic_coverage"]["country_list"]
                            go_sub_coll=True
                if "storage" in  content_subcoll:
                    if len(content_subcoll["storage"])>0:
                        if "object_quantity" in content_subcoll["storage"]:
                            if int(content_subcoll["storage"]["object_quantity"]) !=0:                            
                                reworked_sub_coll["storage"]=content_subcoll["storage"]
                                go_sub_coll=True
                        else:                            
                            reworked_sub_coll["storage"]=content_subcoll["storage"]
                            go_sub_coll=True
                if go_sub_coll:
                    print("===> SUBCOLL_METADATA")
                    print(sub_coll_metadata)
                    #replace google_sheet_src_url by list of URL of sheets
                    sub_coll_metadata["name"]={"lang": "en", "type": "CETAF", "value": key_sub_coll}
                    sub_coll=self.build_obj_coll_json( sub_coll_metadata, p_google_id_reply_index, "cetaf_survey", p_cetaf_institution_acronym, inst_identifiers, p_coll_identifiers, reworked_sub_coll, p_modified_date)
                    
                    sub_coll.pop('list_identifiers', None)
                    sub_coll["parent_collection"]=parent_coll_id_json
                    
                    #TO DOWRITE PARENT TO DATABASE
                    print("CHILD")
                    print("-----")
                    tmp_ident=[{'type': 'cetaf', 'value':p_cetaf_institution_acronym+' '+acronym_coll }]
                    sub_coll['list_identifiers']=tmp_ident
                    print(tmp_ident)
                    print(sub_coll)
                    #sys.exit()
                    rework_idents_sub_coll={}
                    for item in tmp_ident:
                        rework_idents_sub_coll[item["type"]]=item["value"]
                    local_ident_subcoll=None
                    if p_cetaf_institution_acronym is not None and rework_idents_sub_coll["cetaf"] is not None:
                        local_ident_subcoll=rework_idents_sub_coll["cetaf"].replace(p_cetaf_institution_acronym,"").strip()
                    norm_sub_coll=self.get_set_uuid_normalized_coll(rework_idents_sub_coll, norm_inst,tmp_ident, inst_identifiers)
                    sub_coll["type"]="sub_collection"
                    new_child_coll=Collections(uuid_institution_normalized=p_institution_norm_uuid, uuid_collection_normalized=norm_sub_coll.uuid, local_identifier=local_ident_subcoll, identifier= rework_idents_sub_coll["cetaf"],source_uri=p_google_id_reply_index, data=sub_coll, fk_collection_normalized= norm_sub_coll, fk_institution_normalized=norm_inst, version=p_version_v, current=True)
                    new_child_coll.save()
                    child_coll_id={"cetaf": {"code": rework_idents_sub_coll["cetaf"], "institution": "", "name": p_cetaf_institution_acronym+ '-'+key_sub_coll}}
                    subcoll_ident_tmp=self.create_links_to_parent_or_child_collections(child_coll_id)
                    list_sub_coll_idents.append(subcoll_ident_tmp)
        #if rework_idents["cetaf"]=='BE-RBINS GEO':
        #    print("SYS_EXIT2")
        #    sys.exit()
        dict_main_coll["type"]="main_collection"
        dict_main_coll["child_collections"]=list_sub_coll_idents
        new_coll.data=dict_main_coll
        new_coll.save()
        #print(p_metadata)
        #return list_sub_colls
                
        
    