from ..helper import extract_field, affect
import sys

class ESMappingInterface():
    @staticmethod
    def GetMapping(p_normalized_uuid, p_object, options=None):
        pass
        
    @staticmethod
    def prepare_data(p_object):
        data=p_object
        
        
        if "data_list" in p_object:
            data=p_object["data_list"]
            """
            if isinstance(data,list):
                if len(data)>0:
                    data=data[0]
            if "data" in data:
                data=data["data"]
            """       
        return data
        
    @staticmethod
    def get_by_source(p_data, p_source):
        returned=None
        if isinstance(p_data,list):
            for item in p_data:
                if item["source"] == p_source:
                    returned=item
                    if "data" in returned:
                        returned=returned["data"]
        return returned
        
    @staticmethod
    def grscicoll_mf_parser(p_data, p_key1, p_key2):
        returned={}
        if "results" in p_data:
            p_data=p_data["results"]
            for item in p_data:
                if p_key1 in item:
                    tmp1=item[p_key1]
                    for item2 in tmp1:
                        if p_key2 in item2:
                            key3=item2[p_key2]
                            if not key3 in returned:
                                returned[key3]=[]
                            item2.pop(p_key2, None)
                            returned[key3].append(item2)
        return returned
        
    @staticmethod
    def grscicoll_mf_reply_parser_logic(p_data, p_key_list):
        returned=[]
        if p_data is not None:
            for item in p_data:
                for k in p_key_list:
                    if k in item:
                        returned.append(item[k])
        return returned

    @staticmethod
    def grscicoll_mf_reply_parser(p_data, main_key, p_key_list):
        tmp=extract_field(p_data, main_key,[])
        return ESMappingInterface.grscicoll_mf_reply_parser_logic(tmp, p_key_list)
    """   
    @staticmethod    
    def parse_path( data, path, type="value"): # type : value or list
        tmp_path=ESMappingInterface.parse_array_path(path)
        print(tmp_path)
        ret=ESMappingInterface.parse_path_recurs(data, tmp_path, type)
        return ret
        
    @staticmethod
    def parse_path_recurs( data, path, type): # type : value or list
        #print("PATH")
        #print(path)
        if len(path)>0:
            old_path=path.copy()
            first=path.pop(0)            
            parse_first=first.split(":")
            #print(parse_first)
            if len(parse_first)==1:
                #print("case1")
                if isistance(data, dict):
                    if first in data:
                        return ESMappingInterface.parse_path_recurs(data[first], path, type)
            elif len(parse_first)==2 :
                #print("case3")
                field=parse_first[0]
                subpath=parse_first[1]
                if subpath.startswith("@") and len(subpath)>0 and isinstance(data, dict):
                    #print("case3.1")
                    subpath=subpath[1:]
                    subpath_3=subpath.split("|")
                    if len(subpath_3)==2:
                        subpath_2=subpath_3[0].split("=")
                        if len(subpath_2)==2:
                            if field in data:                                
                                if type=="value":
                                    print("case3.1.1")
                                    for elem in data[field]:
                                        if(subpath_2[0] in elem):
                                            if elem[subpath_2[0]]==subpath_2[1]:
                                                return ESMappingInterface.parse_path_recurs(elem[subpath_3[1]], path, type)
                                elif type=="list" and  isinstance(data, list):
                                    print("case3.1.2")
                                    return data
                                elif type=="list" and not isinstance(data, list):
                                    #print("case3.1.3")
                                    #print(data)
                                    returned=[]
                                    for elem in data[field]:   
                                        if(subpath_2[0] in elem):
                                            if elem[subpath_2[0]]==subpath_2[1]:
                                                returned.append(ESMappingInterface.parse_path_recurs(elem[subpath_3[1]], path, type))
                                    #print(returned)
                                    return returned
                elif subpath.startswith("@") and len(subpath)>0 and isinstance(data, list):
                    #print("case3.2")
                    #print(data)
                    #print('interm')
                    acc=[]
                    for item in data:
                        #print("item")
                        #print("item")
                        #print(item)
                        tmp_acc=ESMappingInterface.parse_path_recurs(item, old_path, type)
                        for tmp_acc2 in tmp_acc:
                            acc.append(tmp_acc2)
                    #print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!ACC")
                    #print(acc)
                    #sys.exit()
                    return acc
                elif subpath.isnumeric() and isinstance(data, list):
                    #print("case3.2")
                    subpath_idx=int(subpath)
                    if subpath_idx< len(data):
                        data=data[subpath_idx]
                        #print("ret 3")
                        return ESMappingInterface.parse_path_recurs(data, path, type)
                elif subpath.isnumeric() and  isinstance(data, dict):
                    #print("case3.3")
                    #print('field')
                    #print(field)
                    
                    if field in data:
                        #print('found')
                        #print(data[field])
                        #sys.exit()
                        return ESMappingInterface.parse_path_recurs(data[field], path, type)
        elif len(path)==0:
            #print("bottom")
            #print(data)
            return data
        return None

    @staticmethod           
    def parse_array_path(p_path_str):
        p_path=p_path_str.split("/")
        return  list(filter(lambda a: a or "" !="" ,p_path))
    """    
        

