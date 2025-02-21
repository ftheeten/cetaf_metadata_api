from mergedeep import merge, Strategy
import json

#/element1/element2
#/element1/element2/element3/@sibling_in_dict=value_sibling

class JSONFilterPath():
    global_result_dict={}
    src_json={}
    src_path_list={}
    

    def __init__(self, p_src_json, p_src_path_list):
        self.src_json=p_src_json
        self.src_path_list=p_src_path_list
        self.parse()
   
    def parse(self):
        self.parse_logic(self.src_json, self.src_path_list, self.global_result_dict)
        #self.global_result_dict[""]
        #self.global_result_dict= {json.dumps(x, sort_keys=True):x for x in self.global_result_dict}
        #if len(self.global_result_dict)>0:
        #    self.global_result_dict=[self.global_result_dict[-1]]
        return self.global_result_dict
    
    
    #result => self.global_result_dict passed by reference    
    def parse_logic(self,  data, path, result, type="dict"): # type : value or list
        for p in path:
            #print("°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°")
            tmp_path=self.parse_array_path(p)
            #print(tmp_path)
            tmp_dict={}
            self.parse_path_recurs(data, tmp_path, tmp_dict)
            #print(tmp_dict)
            #tmp_dict["parser"]="test"+p
            if len(tmp_dict)>0:
                result=merge(result,tmp_dict) #, strategy=Strategy.ADDITIVE )
                
                #result=merge(result,tmp_dict, strategy=Strategy.ADDITIVE )
            #print("RETURNED=")
            #print(result)
            #return ret

    def parse_array_path(self, p_path_str):
        p_path=p_path_str.split("/")
        return  list(filter(lambda a: a or "" !="" ,p_path))
        
    def parse_path_recurs(self,  data, path, result, incr=0, type="dict"):
        print("=========>")
        print("CALL "+str(incr))
        print("path="+str(path))
        #print("DATA=")
        #print(data)
        #print("RESULT=")
        #print(result)
        #print("GLOBAL=")
        #print(GLOBAL_DICT)
        
        if len(path)>0:
            first=path.pop(0)
            print("FIRST="+first)
            parse_first=first.split("=")    
            if len(parse_first)==1:
                print("CASE_1")
                if isinstance(data, dict):
                    if first in data:
                        tmp=data[first]
                        if len(path)==0:
                            result[first]=tmp
                        elif isinstance(tmp, list) :
                            result[first]=[]
                            tmp2={}
                            result[first].append(tmp2)
                            self.parse_path_recurs(data[first], path, result[first][-1], incr+1 )
                        elif isinstance(tmp, dict) :
                            result[first]={}
                            self.parse_path_recurs(data[first], path, result[first], incr+1  )                        
                    else:
                        print("NOT_IN_DATA")
                elif isinstance(data, list):
                    print("IS_LIST")
                    go=False
                    path.insert(0,first)
                    for elem in data:
                        if first in elem:
                            print("111_ELEM_IN_LIST")
                            if not go:
                                result[first]=[]
                                go=True
                            if len(path)>0:
                                result[first].append({})
                                self.parse_path_recurs(elem, path, result[first][-1],  incr+1 )
                            else:
                                result[first].append(elem)
                else:
                    print("IS_VALUE")
                    result[first]=tmp
            elif len(parse_first)==2 :
                print("CASE_2")
                field=parse_first[0]
                value=parse_first[1]
                if field.startswith("@") and isinstance(data, list) and len(field)>1:
                    print("CASE_2_1") 
                    #print(data)
                    field=field[1:]
                    print(field)
                    for elem in data:
                        go=False
                        if field in elem:
                            if elem[field]==value:
                                print("found")
                                if not go:
                                    result[field]=[]
                                    go=True
                                result[field].append(elem)
        
            
