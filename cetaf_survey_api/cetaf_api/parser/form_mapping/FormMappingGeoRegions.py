from  .InterfaceFormMapping import InterfaceFormMapping
import json
import sys
from ..helper import print_date, norm_str, val_not_none

class FormMappingGeoRegions(InterfaceFormMapping):

    @staticmethod
    def get_df(p_df, p_unmerge=[], p_rename={}):
        p_df= InterfaceFormMapping.clean_df( p_df, p_unmerge, p_rename)
        p_df=InterfaceFormMapping.align_header_collection_name(p_df)
        return p_df
        
    @staticmethod
    def get_json(p_df):
        returned_main={}
        returned_sub_cols={}
        json_tmp=p_df.to_json( orient='records')
        obj = json.loads(json_tmp)
        for  val in obj:            
            val.pop("index")
            geo_ori=val.pop("geographic_origin")
            continents=val.pop("continents")
            continents=continents or 'undefined continent'
            tmp_key=None
            if "measurements" in val:
                meas_tmp=val["measurements" ] or ""               
                val.pop("measurements")
                tmp_key=norm_str(meas_tmp).lower()                   
            tmp=InterfaceFormMapping.del_json_none(val, ["0", '0 Not defined'])
            if len(tmp)>0 and tmp_key is not None:                
                sum=0
                for key, item in tmp.items():
                    if str(item).isnumeric():
                        sum=sum+int(item)
                if not geo_ori in returned_main:
                    returned_main[geo_ori]={}
                if not continents in returned_main[geo_ori]:
                    returned_main[geo_ori][continents]={}
                to_add={}
                to_add["DETAIL"]=tmp
                #details sub_cols
                for key, val in tmp.items():                    
                    if not key in returned_sub_cols:
                        returned_sub_cols[key]={}
                    returned_sub_cols[key][tmp_key]=val
                if sum>0:
                    to_add["SUM"]=sum
                returned_main[geo_ori][continents][tmp_key]=to_add       
        return returned_main, returned_sub_cols