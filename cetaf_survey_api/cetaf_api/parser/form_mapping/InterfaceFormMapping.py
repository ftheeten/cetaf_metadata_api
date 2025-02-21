from ..helper import print_date, norm_str, val_not_none, del_json_none_logic
import itertools

class InterfaceFormMapping():

    @staticmethod
    def get_df(p_df, p_unmerge=[], p_rename={}):
        return None
    
    @staticmethod
    def get_json(p_df):
        return {} 
        
    @staticmethod
    def clean_df( p_df, p_unmerge=[], p_rename={}):        
        p_df.columns = [norm_str(x)for x in p_df.columns]
        cols=p_df.columns
        #print(cols)  
        #for i in p_unmerge:
        #    print(i)
        #cols_str=[]
        for i in p_rename.keys():
            #+1 index is first column...
            #print(p_df.columns[i+1])
            #sys.exit()
            #if p_df.columns[i+1].startswith("unnamed") and i in p_rename :
            if i in p_rename :
                p_df.rename(columns={ p_df.columns[i+1]: p_rename[i] }, inplace=True)
        cols=p_df.columns        
        for i in p_unmerge:              
            p_df[cols[i]] = p_df[cols[i]].ffill()
        #print(cols_str)    
        #p_df.set_index(cols_str)
        
        """
        for i in p_unmerge:
            p_df.index = pnd.Series(p_df.index).fillna(method='ffill')
        """
        #for key, row in p_df.iterrows():
        #   print(row)
        return p_df
    
    #vals_to_replace must be strings
    @staticmethod    
    def del_json_none(d, vals_to_replace=[]): 
        return del_json_none_logic(d, vals_to_replace)
        
    @staticmethod
    def align_header_collection_name(d):
        columns=list(d.columns.values)
        cols={}
        for c in columns:
            #print(c)
            str_tmp=norm_str(c)
            tmp=c.split("_")
            
            if len(tmp)>2:
                tmp2=tmp[:-2]
                tmp2=[x.upper() for x in tmp2]
                str_tmp='-'.join(tmp2)
            cols[c]=str_tmp
        return d.rename(columns=cols)
                
        