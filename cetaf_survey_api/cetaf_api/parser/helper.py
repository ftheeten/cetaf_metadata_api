import datetime
import re
import uuid

def extract_field(p_dict, p_field, p_default=None):
    returned=p_default
    if p_field in p_dict:
        returned=p_dict[p_field]
        return returned
        
def affect(p_target_dict, p_target_field,p_src_dict, p_src_field):
    tmp=extract_field(p_src_dict, p_src_field)
    if tmp is not None:
        p_target_dict[p_target_field]=tmp
        
def print_date():
    now = datetime.datetime.now()
    print ("Current date and time : ")
    print (now.strftime("%Y-%m-%d %H:%M:%S"))
    
def norm_str(p_str, replace_none=True):
    if p_str is not None:
        p_str=str(p_str)
        p_str=p_str.lower()
        p_str=p_str.replace(" ","_")
        p_str=p_str.replace("-","_")
        p_str=p_str.replace("\r","_")
        p_str=p_str.replace("\n","_")
        p_str=p_str.replace("/","_")
        p_str=p_str.replace("\\","_")
        p_str=p_str.replace("(","_")
        p_str=p_str.replace(")","_")
        p_str=p_str.replace("&","_and_")
        p_str=re.sub('_+', '_', p_str)
        p_str=p_str.strip("_")
    elif replace_none:
        p_str=""
    return p_str

def val_not_none(val):
    returned=False
    if val is not None:
        if len(val)>0:
            return True
    return returned
 

def del_json_none_logic(d, vals_to_replace=[]): 
        vals_to_replace=[str(x).lower() for x in vals_to_replace]
        for key, value in list(d.items()):
            if value is None:
                del d[key]
            elif len(vals_to_replace) >0:
                if str(d[key]).lower() in  vals_to_replace:
                    del d[key]
            elif isinstance(value, dict):
                del_json_none_logic(value)
        return d  
 
def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False
        
class ListAsQuerySet(list):
    def __init__(self, *args, model, **kwargs):
        self.model = model
        super().__init__(*args, **kwargs)

    def filter(self, *args, **kwargs):
        return self  # filter ignoring, but you can impl custom filter

    def order_by(self, *args, **kwargs):
        return self
        