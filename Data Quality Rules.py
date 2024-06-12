from calendar import c
#import os
#from collections import Counter
#from glob import glob
#from unittest import removeResult
import pandas as pd
#import numpy as np
import pytd
from collections import OrderedDict
#import operator
from datetime import datetime
import time
import json
import requests



class DQR:

    validationsRowCountsReportAlgoritmDic  = {
        #--------------------validation with daily report count , column_name = validation_row_counts -------------------------
    "valid_hash_sha256" : "sum(if(regexp_like(param_column_name, '^[0-9a-f]{64}$'),1,0)) as param_column_name_valid_hash_sha256 ", 
    "valid_hash_sha1"   : "sum(if(regexp_like(param_column_name, '^^[0-9a-f]{40}$'),1,0)) as param_column_name_valid_hash_sha1 " ,
    "valid_hash_sha1_digits_appended" : "sum(if(regexp_like(param_column_name, '^[A-Fa-f0-9]{{40}}\_[0-9]{{5,6}}$'),1,0)) as param_column_name_valid_hash_sha1_digits_appended ",
    "valid_email"   : "sum(if( REGEXP_LIKE( param_column_name, '^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$') ,1,0)) as param_column_name_valid_email ",
    "valid_is_not_null" : "sum(if(param_column_name is null,1,0)) as param_column_name_valid_is_not_null ",
    "valid_numbers_only" :"sum(if(REGEXP_LIKE(param_column_name, '^[0-9]+$'),1,0)) as param_column_name_valid_numbers_only",
    "valid_length"  : "if( LENGTH(param_column_name) > param_length ,1,0) as param_column_name_valid_length",
    "distinct_values" : "count(distinct param_column_name) as param_column_name_distinct_values ",
    "valid_text_only" :"sum(if( not REGEXP_LIKE(param_column_name, '[^A-Za-z]'), 1,0)) as param_column_name_valid_text_only ",
    "valid_date_format" :" count(TRY_CAST(param_column_name as TIMESTAMP)) as param_column_name_valid_date_format "
    }
    
    validationsIssuedRowsDescriptionReportAlgoritmDic  = {
    #--------------------validation wit issued rows description column_name = validation_issued_rows_desc -------------------------
    "valid_hash_sha256" : "case regexp_like(param_column_name, '^[0-9a-f]{64}$') when false then   'valid_hash_sha256(param_column_name);' else '' end  ",
    "valid_hash_sha1"   : "case regexp_like(param_column_name, '^[0-9a-f]{40}$') when false then  'valid_hash_sha1(param_column_name);' else '' end " ,
    "valid_hash_sha1_digits_appended"   : "case regexp_like(param_column_name, '^[A-Fa-f0-9]{{40}}\_[0-9]{{5,6}}$') when false then  'valid_hash_sha1_digits_appended(param_column_name);' else '' end " ,
    "valid_email"   : "case  regexp_like(param_column_name, '^[\w-\.]+@([\w-]+\.)+[\w-]{2,4}$') when false then 'valid_email(param_column_name);'  else  '' end ",
    "valid_is_not_null" : "if(param_column_name is not null,'','valid_is_not_null(param_column_name);') ",    
    "valid_numbers_only"   : "case  regexp_like(param_column_name,  '^[0-9]+$') when false then 'valid_numbers_only(param_column_name);'  else  '' end ",
    "valid_length"  : "case when param_column_name is not null then if(LENGTH(param_column_name) > param_length ,'','valid_length(param_column_name);') else '' end ",
    "distinct_values" : "case when param_column_name is not null then  if( count(param_column_name) over(partition by  param_column_name ) =1, '', 'distinct_values(param_column_name);' ) else '' end ",
    "valid_text_only"   : "case not regexp_like(param_column_name, '[^A-Za-z]') when false then 'valid_text_only(param_column_name);'  else  '' end ",
    "valid_date_format": " case when  param_column_name is not null and   TRY_CAST(param_column_name as TIMESTAMP) is null then 'valid_date_format(param_column_name);' else '' end  "       
    }

    #global variables
    TD_API_SERVER = 'https://api.eu01.treasuredata.com/'
    TD_API_KEY ='dddxxxxxx # prod
    client = pytd.Client(database='db_raw',apikey=TD_API_KEY,endpoint= TD_API_SERVER)

    db_validated_name =  'db_clean'
    db_nonvalidated_name =  'db_dirty'
    configuation_validations_table = pd.DataFrame()

    def initRowCountReportOperations(self):
        # assing validations algoritms 
        self.configuation_validations_table["validation_row_counts"] = self.configuation_validations_table["apply_algoritm"].map(self.validationsRowCountsReportAlgoritmDic)
        # replace column names with validation_algoritms tamplate
        self.configuation_validations_table["validation_row_counts"] = self.configuation_validations_table.apply(lambda row :  row["validation_row_counts"].replace("param_column_name",row["column_name"]), axis=1) 
        
    def initIssueRowsDescriptionOperations(self):
        # assing validations algoritms 
        self.configuation_validations_table["validation_issued_rows_desc"] = self.configuation_validations_table["apply_algoritm"].map(self.validationsIssuedRowsDescriptionReportAlgoritmDic)
        # replace column names with validation_algoritms tamplate
        self.configuation_validations_table["validation_issued_rows_desc"] = self.configuation_validations_table.apply(lambda row :  row["validation_issued_rows_desc"].replace("param_column_name",row["column_name"]), axis=1) 

    def initAggregateGenericSQL(self):
        #cfgv_tables = configuation_validations_table["table_name"].unique()
        self.cfgv_report_sql_list_aggr_algo = self.configuation_validations_table.groupby(["table_name","db"]).agg(
            {
            'validation_row_counts': [('unique', lambda x: ','.join(map(str, x.unique())))],
            'validation_issued_rows_desc': [('unique', lambda x: ' || '.join(map(str, x.unique())))]
            }
        )

        # clean column names
        self.cfgv_report_sql_list_aggr_algo.columns = self.cfgv_report_sql_list_aggr_algo.columns.map('_'.join)
        # rename column names
        self.cfgv_report_sql_list_aggr_algo =self.cfgv_report_sql_list_aggr_algo.rename(  columns={
                'db_min': 'db_name', 
                'validation_row_counts_unique': 'reportRowCountAlgoritm',
                'validation_issued_rows_desc_unique': 'issueFilterDescAlgoritm'
                  }          
          )
        
    def __init__(self):
         
         self.configuation_validations_table = self.executeSQLTD("select db, table_name, column_name, apply_algoritm from db_golden.configuation_validations order by table_name")
       
         self.initRowCountReportOperations()
         self.initIssueRowsDescriptionOperations()
         self.initAggregateGenericSQL()
         
    def executeSQLTD(self, query):
        #engine='hive'
        return pd.DataFrame(**self.client.query(query))

    ## takes executed sql_validation result then applies transpose operation for reporting.
    def transformResultToTabular(self, table_name, sqlQueryResult:pd.DataFrame):

        columnsNameList = sqlQueryResult.columns.to_list()
        resultDic = {}

        for columnName in columnsNameList: 
            resultDic[columnName] =  int(sqlQueryResult[columnName][0])
       
        resultSet1  = self.configuation_validations_table[self.configuation_validations_table.table_name == table_name][['db','table_name','column_name', 'apply_algoritm']]
        resultSet1["column_name"]  = resultSet1.apply(lambda row : row["column_name"] + '_' + row["apply_algoritm"]   , axis=1)

        resultSet1["validated_rows_count"]  = resultSet1["column_name"].map(resultDic)
        resultSet1 = resultSet1.assign(total_rows_count=resultDic["total_row_count"])

        resultSet1["diff"] =resultSet1.apply(lambda row : row["total_rows_count"] - row["validated_rows_count"]   , axis=1)
        resultSet1 = resultSet1.assign(time=  time.mktime(datetime.now().date().timetuple()))  

        return resultSet1

    def writeDataTD(self, data:pd.DataFrame):
           
        self.client.load_table_from_dataframe(
            dataframe=data, 
            destination= 'validation_results',
            #writer= ('bulk_import' if(bulkWriterMode)  else  'insert_into' ),
            writer ='bulk_import',
            if_exists='append'
            )

    def startReportingRowCountProcess(self):
        allvalidationResults = pd.DataFrame()

        for index, row in self.cfgv_report_sql_list_aggr_algo.iterrows():
            db_name =  index[1]
            table_name = index[0]
            reportRowCountAlgoritm = row["reportRowCountAlgoritm"] 
            sql_query =  f'select  {reportRowCountAlgoritm} , count(1) total_row_count from  {db_name}.{table_name} '
            sqlResult = self.executeSQLTD(sql_query)
            transformationResult = self.transformResultToTabular(table_name,sqlResult)
            allvalidationResults = pd.concat([transformationResult,allvalidationResults])
                
             
        self.writeDataTD(allvalidationResults)

    def startIssueRowsDetectionProcess(self):
    
        for index, row in self.cfgv_report_sql_list_aggr_algo.iterrows():
            source_db_name =  index[1]
            table_name =   index[0]
            issueFilterDescAlgoritm = row["issueFilterDescAlgoritm"]

            sql_query= f" DROP TABLE IF EXISTS  {self.db_nonvalidated_name}.{table_name}"
            self.executeSQLTD(sql_query)
            sql_query= f"CREATE  TABLE {self.db_nonvalidated_name}.{table_name} AS  with v_map as (select  t.*, {issueFilterDescAlgoritm} as validation_descriptions from   {source_db_name}.{table_name}  t ) select * from  v_map where LENGTH(validation_descriptions ) > 1"
            self.executeSQLTD(sql_query)

    def dropTable(self,tableName):
        sql_query= f" DROP TABLE IF EXISTS  {self.db_validated_name}.{tableName}"
        self.executeSQLTD(sql_query)

    def startCleanRowsDetectionProcess(self): 

        for index, row in self.cfgv_report_sql_list_aggr_algo.iterrows():
            source_db_name =  index[1]
            table_name =   index[0]
            issueFilterDescAlgoritm = row["issueFilterDescAlgoritm"]

            self.dropTable(tableName=table_name)
            #sql_query= f"CREATE  TABLE {self.db_validated_name}.{table_name} AS  select  t.* from   {source_db_name}.{table_name}  t where LENGTH( {issueFilterDescAlgoritm} )  =0"
            sql_query= f"CREATE  TABLE {self.db_validated_name}.{table_name} AS  with v_map as (select  t.*, {issueFilterDescAlgoritm} as validation_descriptions from   {source_db_name}.{table_name}  t ) select * from  v_map where not REGEXP_LIKE(validation_descriptions, ';')"
        
            self.executeSQLTD(sql_query)
             
    def sentEmail(self, dataContentDf:pd.DataFrame):
        
        url = "https://api.postmarkapp.com/email"

        dataContentDf["validated_rows_count"] = dataContentDf["validated_rows_count"].apply(lambda val : f"{val:,}" )
        dataContentDf["total_rows_count"] = dataContentDf["total_rows_count"].apply(lambda val : f"{val:,}" )
        dataContentDf["diff"] = dataContentDf["diff"].apply(lambda val : f"{val:,}" )
        print(dataContentDf["column_name"])
        dataContentDf["column_name2"] = dataContentDf.apply(lambda row : str(row["column_name"])  )

        payload = json.dumps({
        "From": "gokhan@wearesilverbullet.com",
        "To": "gokhan@wearesilverbullet.com",
        "Subject": "Hello from Postmark",
        "HtmlBody": dataContentDf.to_html(),
        "MessageStream": "notofications"
        })

        headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'X-Postmark-Server-Token': '5ab19400-053e-4da8-a209-047eca8481d9'
        }

        response = requests.request("POST", url, headers=headers, data=payload)
        print(response.text)

    def startDailyReportProcess(self):
        uniTime = time.mktime(datetime.now().date().timetuple())
        sqlQuery= f'select db, table_name, column_name, apply_algoritm, validated_rows_count, total_rows_count, diff from db_raw.validation_results where time = {uniTime}'
        resultDF = self.executeSQLTD(sqlQuery)
        self.sentEmail(resultDF) 

    def startProcess(self):
        self.startReportingRowCountProcess()
        self.startDailyReportProcess()
        self.startIssueRowsDetectionProcess()
        self.startCleanRowsDetectionProcess()


def InitProcess():
    r:DQR = DQR()
    r.startProcess()
