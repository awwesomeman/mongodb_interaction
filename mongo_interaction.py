from pymongo import MongoClient
from typing import Dict
from typing import List
from  pandas import DataFrame
import pandas as pd
import json
from tabulate import tabulate
#======================================================================================================================
# connection modules
#======================================================================================================================
# mongodb+srv://ncu:ncu123@cluster0.rm9kd.mongodb.net/test 
def _get_atlasdb( db_name, collection_name, api='mongodb+srv://ncu:ncu123@cluster0.rm9kd.mongodb.net/myFirstDatabase?retryWrites=true&w=majority'):
    client = MongoClient(api)

    return client[db_name][collection_name]  

#----------------------------------------------------------------------------------------------------------------------

def _get_localdb(db_name, collection_name, host='localhost',\
    port=27017,username=None,password=None):

    if username and password:
        mongo_uri = f'mongodb://{username}:{password}@{host}/{db_name}'
        conn = MongoClient(mongo_uri)

    else:
        conn = MongoClient(host, port)

    return conn[db_name][collection_name]
#======================================================================================================================
# update operators
# https://docs.mongodb.com/manual/reference/operator/update/unset/

# advanced query
# https://docs.mongodb.com/manual/reference/operator/query/

#======================================================================================================================
#======================================================================================================================
# mmain module
#======================================================================================================================    
#======================================================================================================================

class operate_mongo():
    def __init__(self, collection_name, db_name='risk_index', atlas=True):
        if atlas:
            self.collection = _get_atlasdb(db_name, collection_name)
        else:
            self.collection = _get_localdb(db_name, collection_name)
        self.collection_name = collection_name
#======================================================================================================================
# manupilate data in db modules
#======================================================================================================================    
    
    def add_freq(self, query:Dict, new_freq:str, suffix:str , add_to_coll=False):
        """
        
        since we will add frequency from the original data, query  must include {'Suffix' : 'original'}.

        Args:
            query (Dict): [description]
            new_freq (str): [description]
            method (str): [description]
            down_sampling (bool): [description]
            add_to_coll (bool, optional): [description]. Defaults to False.
        """
        method = suffix.split('_')[-1]
        df = self.get_from_coll(query).copy()
        id_list = df.id.unique()

        if len(id_list) == 1:

            df_empty = self.get_from_coll({'id' : id_list[0], 'Freq':new_freq, 'Suffix' : suffix}).empty
            if df_empty:
                self._add_freq(df, new_freq, method, suffix, add_to_coll)
            else:
                print(f'id {id_list[0]} with the same suffix already exist.')
        
        elif  len(id_list) > 1:
            num = 0
            for id in id_list:
                df_empty = self.get_from_coll({'id' : id, 'Freq':new_freq, 'Suffix' : suffix}).empty
                if df_empty:
                    tem = df.query(f"id == '{id}'").copy()
                    self._add_freq(tem, new_freq, method, suffix, add_to_coll)
                    num += 1 
                else:
                    print(f'id {id} with the same suffix already exist, skipped.') 
            print(f'{num} data are added')               
        else:
            print('something goes wrong with your query.')

    def _add_freq(self, df:DataFrame, new_freq:str, method:str,suffix:str, add_to_coll:bool):

        tem = df.copy()
        tem = tem.dropna()
        tem.Date = pd.to_datetime(tem.Date)
        tem = tem.sort_values('Date')
        tem = tem[['Date','Value']]
        tem = tem.set_index('Date')
        new_tem = getattr(tem.resample(new_freq),method)()
        
        new_tem = new_tem.reset_index()
        new_tem['Category'] = df.Category.values[0]
        new_tem['Subcategory'] = df.Subcategory.values[0]
        new_tem['id'] = df.id.values[0]
        new_tem['Freq'] = new_freq
        new_tem['Suffix'] = suffix
        new_tem.Date = new_tem.Date.apply(lambda x:x.isoformat())
        new_tem = new_tem[['Date','Category','Subcategory','id','Value','Freq','Suffix']]

        if add_to_coll:
            self.save_to_coll(new_tem)
            print('new frequency, added successfully result : ')
            print(tabulate(new_tem,headers=new_tem.columns, tablefmt='pretty'))
            
        else: 
            print('new frequency , result : ')
            print(tabulate(new_tem,headers=new_tem.columns, tablefmt='pretty'))
           
    def update_docs(self,target_query={},update_query={}):

        before = self.get_from_coll(query=target_query)  
        result = self.collection.update_many(target_query,update_query)
        after = self.get_from_coll(query=target_query)
        print('=================================================================================================================')
        print('matched documents, shape = ', before.shape)
        print('=================================================================================================================')
        print(tabulate(before,headers=before.columns, tablefmt='pretty'))
        print('=================================================================================================================')
        print('matched documents after changes, shape = ', after.shape)
        print('=================================================================================================================')
        print(tabulate(after,headers=after.columns, tablefmt='pretty'))
        print('=================================================================================================================')
        print(result.modified_count, " documents updated")
        print('=================================================================================================================')

    def delet_docs(self,query):
        
        result = self.collection.delete_many(query)        
        print(result.deleted_count, " documents deleted")

    def clean_up(self,query):
        '''
        drop duplicates and sort by date
        better use with specific data, e.g., data with the same Freq and id
        '''
        tem = self.get_from_coll(query)
        tem = tem.dropna()
        tem.sort_values('Date')
        tem.drop_duplicates(subset=['Date','Category','Subcategory','id','Value','Freq','Suffix'],inplace=True)
        self.delet_docs(query)
        print(len(tem),' documentes re-added after cleaning')
        self.save_to_coll(tem)
#======================================================================================================================
# read & write modules
#======================================================================================================================
    def get_from_coll(self,query={}):
        df = pd.DataFrame(self.collection.find(query))

        if df.duplicated(['Date','Category','Subcategory','id','Value','Freq','Suffix']).any():
            print('duplicated value exist')
        return df

    def get_distinct(self,field_name):
        '''
        get distinct values from single field
        '''
        return self.collection.distinct(field_name)

    def get_from_coll_to_raw(self, query, suffix_criteria:List=['original','us_ffill','ds_mean']):
        
        """
        The function aims to gather all the required data wit the same frequency, so Freq is required.
        The function will retur a time series dataframe.

        Args:
            query ([type]): [description]

        Returns:
            [type]: [description]
        """
        df = self.get_from_coll(query).copy()
        id_list = df.id.unique()
        if len(id_list) == 1:
            suffix_list = df.Suffix.unique()
            id = id_list[0]
            intersection = set(suffix_list) & set(suffix_criteria)
            if intersection:
                tem = df.query(f"Suffix =='{list(intersection)[0]}'").copy()
                tem = self._get_from_coll_to_raw(tem,id)
                return tem
            else:
                print(f'id {id} might miss the suffix you sepecified.')
       
        elif len(id_list) > 1:
            tem_list=[]
            for id in id_list:
                
                tem = df.query(f"id == '{id}'").copy()
                suffix_list = tem.Suffix.unique()
                intersection = set(suffix_list) & set(suffix_criteria)
                if intersection:
                    tem2 = tem.query(f"Suffix =='{list(intersection)[0]}'").copy()
                    tem2 = self._get_from_coll_to_raw(tem2,id)
                    tem_list.append(tem2)
                else:
                    print(f'id {id} might miss the suffix you sepecified, skipped')
            
            tem3 = pd.concat(tem_list,axis=1,join='outer')
            return tem3

        else:
            print('no id found with your query, check it again.')

    def _get_from_coll_to_raw(self,df:DataFrame,id:str):
                tem = df[['Date','Value']].copy()
                tem['Date'] = tem['Date'].str[:7]+'-01'
                tem.Date = pd.to_datetime(tem.Date)
                tem=tem.sort_values('Date')
                tem.set_index('Date',inplace=True)
                tem.columns=[id]
                tem = tem.dropna()
                return tem

    def save_to_coll(self,df):

        tem = df.copy()
        tem = tem.dropna()

        if '_id' in tem.columns:
            tem.drop('_id',axis=1,inplace=True)

        tem=tem.sort_values('Date')
        tem['Category'] = self.collection_name
        tem['Subcategory'] = tem.Subcategory
        tem['Freq'] = tem.Freq    
        tem = tem[['Date','Category','Subcategory','id','Value','Freq','Suffix']]
        jtext = tem.to_json(orient='records')
        self.collection.insert_many(json.loads(jtext)) 
        
        print("df added successfully, result : ")
        print(tabulate(tem,headers=tem.columns, tablefmt='pretty'))

    def save_raw_to_coll(self, df, subcategory, data_freq): 
        '''
        input df form : Index|Date|Var1_Name|Var2_Name|
                        -------------------------------
                        xxx  |xxx | xxxxxxxx|xxxxxxxxx|
                        xxx  |xxx | xxxxxxxx|xxxxxxxxx|
                        xxx  |xxx | xxxxxxxx|xxxxxxxxx|
        
        Index -> use the default setting
        Date -> must be able to be turnn into datetime object
        Var_Name -> dtype must be int or float
        
        variables must be in the same subcategory and with the same data frequency.

        '''
        tem = df.copy()
        ids = tem.drop('Date',axis=1).columns
        for id in ids:
            tem2 = tem[['Date',id]].copy()
            self._save_raw_to_coll(tem2, id, subcategory, data_freq)
    
    def _save_raw_to_coll(self,df:DataFrame, id:str, subcategory:str, data_freq:str):
        tem = df.dropna()
        if id in self.get_distinct('id'):
            print(f'id {id} already exists, please delete it and try again.')

        else:
            tem['Category'] = self.collection_name
            tem['Subcategory'] = subcategory
            tem['id'] = id
            tem['Value']= tem[id]
            tem['Freq'] = data_freq
            tem['Suffix'] = 'original'
            tem.Date = pd.to_datetime(tem.Date)
            tem=tem.sort_values('Date')
            tem.Date = tem.Date.apply(lambda x:x.isoformat())
            tem.drop(id,axis=1,inplace=True)
            
            jtext = tem.to_json(orient='records')
            self.collection.insert_many(json.loads(jtext))
            
            print("raw df added and transformed successfully, result : ")
            print(tabulate(tem,headers=tem.columns, tablefmt='pretty'))