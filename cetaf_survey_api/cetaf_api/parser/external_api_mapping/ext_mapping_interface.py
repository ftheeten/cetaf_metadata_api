import requests 

class ExtMappingInterface():


    @staticmethod
    def GetMapping(p_val, p_data):
        pass

    @staticmethod
    def go_for_api_logic(url):
        returned=None
        resp=requests.get(url, headers={'Content-type': 'application/json', 'Accept': 'application/json'})
        #print(resp)
        if resp.status_code==200:
            #print(resp.json())
            return resp.json()
        return returned
        
    @staticmethod    
    def parse_path( data, path, type="value"): # type : value or list
        tmp_path=ExtMappingInterface.parse_array_path(path)
        ret=ExtMappingInterface.parse_path_recurs(data, tmp_path, type)
        return ret
        
    #/element1/element2
    #/element1/element2/element3:@sibling_in_dict|value_sibling/next_elem
    @staticmethod
    def parse_path_recurs( data, path, type): # type : value or list
        if len(path)>0:
            first=path.pop(0)            
            parse_first=first.split(":")
            if len(parse_first)==1:
                if isistance(data, dict):
                    if first in data:
                        return ExtMappingInterface.parse_path_recurs(data[first], path, type)
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
                                if type=="value":
                                    for elem in data[field]:
                                        if(subpath_2[0] in elem):
                                            if elem[subpath_2[0]]==subpath_2[1]:
                                                return ExtMappingInterface.parse_path_recurs(elem[subpath_3[1]], path, type)
                                elif type=="list":
                                    returned=[]
                                    for elem in data[field]:   
                                        if(subpath_2[0] in elem):
                                            if elem[subpath_2[0]]==subpath_2[1]:
                                                returned.append(ExtMappingInterface.parse_path_recurs(elem[subpath_3[1]], path, type))
                                    return returned
                elif subpath.isnumeric() and isinstance(data, list):
                    subpath_idx=int(subpath)
                    if subpath_idx< len(data):
                        data=data[subpath_idx]
                        print("ret 3")
                        return ExtMappingInterface.parse_path_recurs(data, path, type)
        elif len(path)==0:
            return data
        return None

    @staticmethod           
    def parse_array_path(p_path_str):
        p_path=p_path_str.split("/")
        return  list(filter(lambda a: a or "" !="" ,p_path))