from mergedeep import merge, Strategy
import json

#/element1/element2
#/element1/element2/@/element3/@sibling_in_dict=value_sibling
#/@ only to select whole array

from copy import deepcopy

#/element1/element2
#/element1/element2/@/element3/@sibling_in_dict=value_sibling
#/@ only to select whole array

class JSONFilterPath():
    global_result_dict={}
    src_json={}
    src_path_list={}

    def __init__(self, p_src_json, p_src_path_list):
        self.src_json=p_src_json
        self.src_path_list=p_src_path_list
        self.parse()
        
    def parse(self):
        pars_p=[list(filter(None,x.split("/"))) for x in self.src_path_list]
        interm=[["@" if x2.startswith("@") else x2 for x2 in x1] for x1 in pars_p]
        tmp=deepcopy(self.src_json)
        #tmp mutable copy of a dict !
        self.parse_dict_recurs(tmp, pars_p, interm)
        return tmp
        
    def prepare_path(self, p_paths,p_cur_len ):
        #cur_len=len(p_elem)
        cut=list(filter(None,[ x[:p_cur_len]  if len(x)>=p_cur_len else None for x in p_paths]))
        #print(cut)
        return cut
       
    def fct_compare_path_indices(self, p_current, p_paths):
        returned =[]
        i=0
        for p in p_paths:
            if p==p_current:
                returned.append(i)
            i=i+1
        return returned
     
    def parse_path_elem(self, p_elem):
        p_type="path"
        p_field=p_elem
        p_val=None
        test=p_elem.split("=")
        if len(test)==2:
            p_type="sibling"
            p_field=test[0]
            if len(p_field)>0:
                p_field=p_field[1:]
            p_val=test[1]
        return p_type, p_field, p_val
        
                
        
    def parse_dict_recurs(self,p_current, p_paths,  p_paths_no_attributes,  p_current_depth=0, p_parent=[]):
        compare_path=self.prepare_path( p_paths, len(p_parent)+1)
        compare_path_test_attr=self.prepare_path( p_paths_no_attributes, len(p_parent)+1)
        if True:
            if isinstance(p_current, list):
                filter_after_loop=False
                to_keep_list=[]
                block_next=False
                to_keep=[]
                i=0
                block=False
                new_paths=[]
                narrow_paths=False
                for elem in p_current:
                    if isinstance(elem, dict):
                        original_elem=p_current[i]
                        new_path=p_parent.copy()
                        new_path.append("@")
                        candidates=self.fct_compare_path_indices(new_path, compare_path)
                        if len(candidates)>0:
                            to_keep.append(i)
                        else:
                            candidates=self.fct_compare_path_indices(new_path, compare_path_test_attr)
                            if len(candidates)>0:                        
                                for id_test in candidates:
                                    if not p_paths[id_test] in new_paths:
                                        new_paths.append(p_paths[id_test])
                                        narrow_paths=True
                                    attr_direction=p_paths[id_test][p_current_depth]
                                    if p_current_depth==(len(p_paths[id_test])-1):
                                        block=True
                                    p_type, p_field, p_val=self.parse_path_elem(attr_direction)
                                    keep_elem=False                                
                                    if p_field in elem:
                                        if str(elem[p_field])==str(p_val):
                                            to_keep.append(i)
                    i=i+1
                replacing_elems=[]
                elements=list(range(0, len(p_current)))
                to_delete=list(set(elements) - set(to_keep))
                to_delete=list(reversed(to_delete))
                for idel in to_delete:
                    del p_current[idel]
                if not block:
                    if narrow_paths:
                        p_paths= new_paths 
                        p_paths_no_attributes=[["@" if x2.startswith("@") else x2 for x2 in x1] for x1 in p_paths]
                    for elem in p_current:
                        self.parse_dict_recurs(elem,  p_paths,  p_paths_no_attributes,  p_current_depth+1, new_path)
            elif isinstance(p_current, dict):           
                keys_to_delete=[]
                for key, elem in p_current.items():
                    new_path=p_parent.copy()
                    new_path.append(key)
                    candidates=self.fct_compare_path_indices(new_path, compare_path)
                    if len(candidates)>0:
                        candidates2=self.fct_compare_path_indices(new_path, p_paths_no_attributes)
                        if len(candidates2)==0:
                            self.parse_dict_recurs(elem,  p_paths,  p_paths_no_attributes,  p_current_depth+1, new_path)
                    else:
                        keys_to_delete.append(key)
                for key in keys_to_delete:
                    p_current.pop(key)