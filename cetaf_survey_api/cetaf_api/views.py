import json
from django.http import JsonResponse
from django.shortcuts import render, redirect
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
#from rest_framework import permissionssudo
from django.core.cache import cache
from  urllib.parse import quote, parse_qs

import os.path
import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO
'''
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
'''
import pygsheets
import pandas as pnd
# Create your views here.
from .models import Institutions, Collections
from .serializers import InstitutionSerializer, CollectionSerializer
from django.db.models.expressions import RawSQL
from django.db import connection
from django.forms.models import model_to_dict
from urllib.parse import unquote
import math 

from .parser.json_filter_path import JSONFilterPath


#TO GET TOKEN
#https://stackoverflow.com/questions/19766912/how-do-i-authorise-an-app-web-or-installed-without-user-intervention/19766913#19766913    
class GetGoogleCloudSheet():
    refresh_token=None
    sheet_id=None
    client_id=None
    client_secret=None
    redirect_uri=None
    refresh_url=None
    
    def __init__(self, refresh_token, client_id, client_secret, redirect_uri, refresh_url):
        self.refresh_token=refresh_token
        #self.sheet_id=sheet_id
        self.client_id=client_id
        self.client_secret=client_secret
        self.redirect_uri=redirect_uri
        self.refresh_url=refresh_url
        
    def get_access_token(self):
        access_token=None
        encoded_client=quote(self.client_id, safe='~()*!.\'')
        encoded_secret=quote(self.client_secret, safe='~()*!.\'')
        encoded_token=quote(self.refresh_token, safe='~()*!.\'')
        headers={
                'Content-Type': 'Content-Type: application/x-www-form-urlencoded',
                }
        post_body = 'grant_type=refresh_token&client_id='+encoded_client+'&client_secret='+encoded_secret+'&refresh_token='+encoded_token
        resp=requests.post(self.refresh_url, params=post_body, headers=headers)
        if resp.status_code==200:
            access_token=resp.json()["access_token"]
        return access_token
        
    def get_excel_as_panda(self, sheet_id):
        returned=None
        access_token=self.get_access_token()
        if access_token is not None:
            scopes=["https://www.googleapis.com/auth/drive"]
            creds = Credentials(token=access_token, default_scopes =scopes, client_id =self.client_id, client_secret =self.client_secret  )
            if not creds or not creds.valid:
                return None
            else:
                service = build('drive', 'v3', credentials=creds)
                request = service.files().get_media(fileId=sheet_id)
                fh = BytesIO()
                downloader = MediaIoBaseDownload(fd=fh, request=request)
                done=False
                while not done:
                    status, done = downloader.next_chunk()
                df = pnd.read_excel(fh.getvalue())
                returned=df
        return returned
 

class PagerException(Exception):
    pass
    
class APIViewCetaf(APIView):

    def pager(self, rs,  p_serializer, page, page_size):
        offset=(page-1)*page_size
        limit=offset+page_size      
        data1=rs.annotate(count_all=RawSQL("COUNT(*) OVER ()", [])).all()[offset:limit]
        #list_inst=Institutions.objects.all()
        if len(data1)>0:
            count_all=getattr(data1.first(), "count_all")
        else: 
            count_all=0
        nb_pages=math.ceil(count_all/page_size)
        if page>nb_pages and count_all>0 :
            raise PagerException(Exception("page above upper limit"))
        elif page<0:
            raise PagerException(Exception("page below lower limit"))
        data2=p_serializer(data1, many=True).data
        resp={"pager":{"page":page, "page_size":len(data2), "size": count_all }, "data": data2}
        return resp
    

    def get_custom_sql(self, wrap_query, params, p_order, p_page, p_size):
        cursor = connection.cursor()
        offset=(p_page-1)*p_size
        params.append(p_order)
        params.append(str(p_size))
        params.append(str(offset))        
        params=tuple(params)  
        wrap_query=wrap_query + " ORDER BY %s LIMIT %s OFFSET %s"       
        query=wrap_query % params        
        cursor.execute(query)
        desc = cursor.description 
        data=cursor.fetchall()
        rows=[dict(zip([col[0] for col in desc], row)) for row in  data]      
        resp={"pager":{"page":p_page, "page_size":p_size, "size": len(rows) }, "data": rows}
        return resp

class WSIInstitutionsView(APIViewCetaf):

    gs_auth_file=settings.GOOGLE_AUTH_FILE
    
    def post(self, request, *args, **kwargs):
        cache.clear()
        resp={}
        resp["test"]="test"
        return  Response(resp, status=status.HTTP_200_OK)
        
    

        
    def get(self, request, *args, **kwargs):        
        data={}
        params_tmp=request.GET.urlencode()
        params=[]
        #return JsonResponse(params_tmp, safe=False)
        if len(params_tmp)>0:
            params=parse_qs(params_tmp)
            #return JsonResponse(params, safe=False)
        
        source="all"
        if "source" in params:
            if len(params["source"])>0:
                source=(params["source"][0]).lower().strip()
                
        page_size=settings.DEFAULT_QUERY_SIZE or 10
        if "size" in params:
            if len(params["size"])>0:
                page_size=int(params["size"][0])
        
        page=1
        if "page" in params:
            if len(params["page"])>0:
                page=int(params["page"][0])
                
        
         
        #current/all/number
        if not "version" in params:
            params["version"]="current"
        
            
            
        for k, v in  params.items():
            if isinstance(v, str):
                params[k]=params[k].lower()   
        #offset=(page-1)*page_size
        #limit=offset+page_size 
        operation=params["operation"] or []        
        if len(operation)>0:
            operation=operation[0]
            #return JsonResponse({"debug2":str(operation)}, safe=False)
        if operation=="list":
            if source=="all":
                q=Institutions.objects.filter(current=True)
                resp=self.pager( q,  InstitutionSerializer, page, page_size)
            else:
                resp=self.get_custom_sql("SELECT  \
 uuid, uuid_institution_normalized, identifier, \
jsonb_set(jsonb_set(data, '{data_list}', '[]'), '{data_list, 0}' , o.obj)::json data \
  FROM public.cetaf_api_institutions d \
cross join lateral jsonb_array_elements(d.data->'data_list') o(obj) \
where (o.obj->>'source')='%s'", [source],  "uuid",page, page_size )            
            return JsonResponse(resp, safe=False)
            
        if operation=="get_by_id" and "protocol" in params and "values" in params:
            if len(params["protocol"])>0 and len(params["values"])>0:
                protocol=unquote(params["protocol"][0])
                value=unquote(params["values"][0])
                if source=="all":
                    q=Institutions.objects.extra(where=["uuid in (select d.uuid  FROM cetaf_api_institutions d cross join lateral jsonb_array_elements(d.data->'list_identifiers') o(obj) where (o.obj->>'type')=%s and  (o.obj->>'value')=%s)"], params=(protocol, value))
                    resp=self.pager( q,  InstitutionSerializer, page, page_size)
                else:
                    resp=self.get_custom_sql("WITH a AS (\
                    SELECT cetaf_api_institutions.* FROM cetaf_api_institutions WHERE uuid IN (SELECT d.uuid FROM cetaf_api_institutions d CROSS JOIN LATERAL jsonb_array_elements(d.data->'list_identifiers') o(obj) WHERE (o.obj->>'type')='%s' AND  (o.obj->>'value')='%s') )\
                    SELECT  \
 uuid, uuid_institution_normalized, identifier, \
jsonb_set(jsonb_set(data, '{data_list}', '[]'), '{data_list, 0}' , o.obj)::json data \
  FROM  a \
cross join lateral jsonb_array_elements(a.data->'data_list') o(obj) \
where (o.obj->>'source')='%s'\
                    ", [ protocol, value, source],  "uuid",page, page_size ) 
                
                return JsonResponse(resp, safe=False)  
                
        if operation=="query_str" and "q" in params:
            if len(params["q"]):
                p=params["q"][0]
                q=Institutions.objects.extra(where=["LOWER(data::varchar) ~ %s"], params=(p,))
                page_size=min(page_size, len(q))
                if page_size==0:
                    return JsonResponse({"pager": {'page':page, 'page_size': page_size, 'size':0}}, safe=False)
                resp=self.pager( q,  InstitutionSerializer, page, page_size)
                return JsonResponse(resp, safe=False)
                
        else:
            return JsonResponse({"debug":type(operation)}, safe=False)
        return JsonResponse(data, safe=False)
        
    def read_sheet(self, ws_name, idx_sheet):
        client = pygsheets.authorize(service_file=self.gs_auth_file)
        sh = client.open_by_key(ws_name)
        wks=sh.worksheet('index',idx_sheet)
        df = wks.get_as_df()
        identifier = df.columns.to_series().groupby(level=0).transform('cumcount')
        df.columns = df.columns.astype('string') + identifier.astype('string')
        return df

class WSICollectionsView(APIViewCetaf):

    #gs_auth_file=settings.GOOGLE_AUTH_FILE
    
    def post(self, request, *args, **kwargs):
        cache.clear()
        resp={}
        resp["test"]="test"
        return  Response(resp, status=status.HTTP_200_OK)
        
    
    def pager(self, rs,  p_serializer, page, page_size):
        offset=page-1
        limit=offset+page_size      
        data1=rs.annotate(count_all=RawSQL("COUNT(*) OVER ()", [])).all()[offset:limit]
        #list_inst=Institutions.objects.all()
        if len(data1)>0:
            count_all=getattr(data1.first(), "count_all")
        else: 
            count_all=0
        data2=p_serializer(data1, many=True).data
        resp={"pager":{"page":page, "page_size":len(data2), "size": count_all }, "data": data2}
        return resp
        
    def filter_by_profile(self, data, profile=""):        
        if len(profile)>0:
            if profile in settings.JSON_OUTPUT_FILTER_PROFILE:                  
                returned=[]
                paths=settings.JSON_OUTPUT_FILTER_PROFILE[profile]                
                parser_filter=JSONFilterPath(data, paths)
                data=parser_filter.parse()                      
        return data
        
    def get(self, request, *args, **kwargs):        
        data={}
        params_tmp=request.GET.urlencode()
        params=[]
        #return JsonResponse(params_tmp, safe=False)
        if len(params_tmp)>0:
            params=parse_qs(params_tmp)
            #return JsonResponse(params, safe=False)
        operation=params["operation"] or []
        
        page_size=settings.DEFAULT_QUERY_SIZE or 10
        if "size" in params:
            if len(params["size"])>0:
                page_size=int(params["size"][0])
        
        page=1
        if "page" in params:
            if len(params["page"])>0:
                page=int(params["page"][0])
                
                
         #current/all/number
        if not "version" in params:
            params["version"]=["current"]
        
        #profile
        profile=""
        if "profile" in params:
            if len(params["profile"])>0:
                profile=params["profile"][0]    
            
        for k, v in  params.items():
            if isinstance(v, str):
                params[k]=params[k].lower() 
        
        
        #offset=(page-1)*page_size
        #limit=offset+page_size      
        if len(operation)>0:
            operation=operation[0]
            #return JsonResponse({"debug2":str(operation)}, safe=False)
        
        if operation=="list":
            q=Collections.objects.filter(current=True)
            resp=self.pager( q,  CollectionSerializer, page, page_size)
            return JsonResponse(resp, safe=False)
        elif operation=="get_by_id" and "protocol" in params and "values" in params:            
            if len(params["protocol"])>0 and len(params["values"])>0:
                protocol=unquote(params["protocol"][0])
                value=unquote(params["values"][0])
                version=unquote(params["version"][0])
                if version=="current":                
                    q=Collections.objects.extra(where=["uuid in (select d.uuid  FROM cetaf_api_collections d cross join lateral jsonb_array_elements(d.data->'list_identifiers') o(obj) where (o.obj->>'type')=%s and  (o.obj->>'value')=%s) AND current IS true"], params=(protocol, value))
                    resp=self.pager( q,  CollectionSerializer, page, page_size)
                    return JsonResponse(self.filter_by_profile(resp, profile), safe=False)
                elif version=="all":
                    q=Collections.objects.extra(where=["uuid in (select d.uuid  FROM cetaf_api_collections d cross join lateral jsonb_array_elements(d.data->'list_identifiers') o(obj) where (o.obj->>'type')=%s and  (o.obj->>'value')=%s)"], params=(protocol, value))
                    resp=self.pager( q,  CollectionSerializer, page, page_size)
                    return JsonResponse(self.filter_by_profile(resp, profile), safe=False)
                elif version.isnumeric():
                    q=Collections.objects.extra(where=["uuid in (select d.uuid  FROM cetaf_api_collections d cross join lateral jsonb_array_elements(d.data->'list_identifiers') o(obj) where (o.obj->>'type')=%s and  (o.obj->>'value')=%s) AND version=%s"], params=(protocol, value, int(version)))
                    resp=self.pager( q,  CollectionSerializer, page, page_size)
                    return JsonResponse(self.filter_by_profile(resp, profile), safe=False)
                    
        elif operation=="get_by_institution_id" and "protocol" in params and "values" in params:
            if len(params["protocol"])>0 and len(params["values"])>0:
                protocol=unquote(params["protocol"][0])
                value=unquote(params["values"][0])
                
                q=Collections.objects.extra(where=["uuid in (select d.uuid  FROM cetaf_api_collections d cross join lateral jsonb_array_elements(d.data->'institution_list_identifiers') o(obj) where (o.obj->>'type')=%s and  (o.obj->>'value')=%s and current=True)"], params=(protocol, value))
                obj_sum=0
                type_sum=0
                for tmp in q:
                    tmp_data=tmp.data
                    if "data" in tmp_data:
                        if "description" in tmp_data["data"]:
                            if "objects_count" in tmp_data["data"]["description"]:
                                obj_sum=obj_sum+int(tmp_data["data"]["description"]["objects_count"])
                            if "types_count" in tmp_data["data"]["description"]:
                                type_sum=type_sum+int(tmp_data["data"]["description"]["types_count"])
                resp=self.pager( q,  CollectionSerializer, page, page_size)
                result={}
                result["result"]=resp
                result["total"]={"sum_objects_count": obj_sum, "sum_types_count": type_sum}
                return JsonResponse(result, safe=False)
        elif operation=="query_str" and "q" in params:
            if len(params["q"]):
                p=unquote(params["q"][0])
                q=Collections.objects.extra(where=["LOWER(data::varchar) ~ %s"], params=(p,))
                resp=self.pager( q,  CollectionSerializer, page, page_size)
                return JsonResponse(resp, safe=False)
        else:
            return JsonResponse({"debug":type(operation)}, safe=False)
        return JsonResponse(data, safe=False)
   
class WSIGoogleSheetView(APIView):

    def get(self, request, *args, **kwargs):
        data={}
        params_tmp=request.GET.urlencode()
        if len(params_tmp)>0:
            params=parse_qs(params_tmp)
            sheet_id=params["sheet_id"] or ""
            if sheet_id !="":
                if hasattr(sheet_id, "__len__"):
                    if len(sheet_id)>0:
                        sheet_id=sheet_id[0]
                #data["sheet_id"]=sheet_id
                refresh_token = settings.GOOGLE_CLOUD_REFRESH_TOKEN
                client_id = settings.GOOGLE_CLOUD_CLIENT_ID
                client_secret = settings.GOOGLE_CLOUD_SECRET
                redirect_uri = settings.GOOGLE_CLOUD_REDIRECT_URI
                refresh_url = settings.GOOGLE_CLOUD_REFRESH_URL
                google_object=GetGoogleCloudSheet(refresh_token, client_id, client_secret, redirect_uri, refresh_url)
                df_cloud_excel=google_object.get_excel_as_panda(sheet_id)
                data=df_cloud_excel.to_json(orient="records")
        
        #return  Response(data, status=status.HTTP_200_OK)
        return JsonResponse(data, safe=False)
    
        
