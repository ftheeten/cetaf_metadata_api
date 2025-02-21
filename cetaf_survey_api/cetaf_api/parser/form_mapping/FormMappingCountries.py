from  .InterfaceFormMapping import InterfaceFormMapping
from ..helper import print_date, norm_str, val_not_none
import json
import sys

class FormMappingCountries(InterfaceFormMapping):
    @staticmethod
    def get_df(p_df, p_unmerge=[], p_rename={}):
        #print(p_rename)
        
        p_df=FormMappingCountries.parse_countries(p_df)
        p_df=InterfaceFormMapping.clean_df( p_df, p_unmerge, p_rename)
        #for i, row in p_df.iterrows():
        #    print(row)
        #sys.exit()
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
            continent=val.pop("continent").title()
            country=val.pop("country")
            tmp=InterfaceFormMapping.del_json_none(val)
            if len(tmp)>0:
                if not continent in returned_main:
                    returned_main[continent]={}
                #sub cols
                for key, val in tmp.items():
                    key=key.replace("_", "-").upper()
                    if not key in returned_sub_cols:
                        returned_sub_cols[key]={}
                    if not continent in returned_sub_cols[key]:
                        returned_sub_cols[key][continent]=[]
                    returned_sub_cols[key][continent].append(country)
                #
                returned_main[continent][country]=tmp
        return returned_main, returned_sub_cols
        
    @staticmethod    
    def parse_countries( p_df):
        p_df["continent"] = ""
        current_continent=p_df.columns[1].title()
        #p_df.rename(columns={ p_df.columns[1]: "country" }, inplace=True)
        #print(current_continent)
        #sys.exit()
        to_delete=[]
        for i, row in p_df.iterrows():   
            #print(row)
            if val_not_none(row.iloc[1]):
                if not "-" in row.iloc[1]:
                    #print(row.iloc[1])
                    current_continent=row.iloc[1]
                    to_delete.append(i)
                    #sys.exit()
            p_df.at[i, "continent"]=current_continent
        p_df=p_df.drop(p_df.index[to_delete])
        return p_df