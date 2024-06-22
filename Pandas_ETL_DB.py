from dotenv import load_dotenv
import os
import pytd
import pandas as pd
import re
from datetime import datetime, date
from collections import defaultdict
import tdclient
import json
import boto3
import gzip
from pprint import pprint
import dictdiffer
import distutils
from tqdm.notebook import trange, tqdm
from time import sleep


apikey = os.getenv("TD_API_KEY")
api_client = tdclient.Client(apikey)


client = pytd.Client(database='db_raw')


s3_conn = boto3.client("s3", aws_access_key_id=os.environ.get('S_S3_TD_KEY'), aws_secret_access_key=os.environ.get('S_S3_TD_SECRET'))

bucket = os.environ.get('S_S3_TD_BUCKET')
s3_conn.head_bucket(Bucket=bucket)

s3_resource = boto3.resource('s3', aws_access_key_id=os.environ.get('S_S3_TD_KEY'), aws_secret_access_key=os.environ.get('S_S3_TD_SECRET'))

connectors = api_client.api.connector_list()



prod_files = []
for key in keys:
    # fpath = os.path.normpath(os.path.join(root, fnm))
    fpath = key['Key']
    matches = re.search(r'Treasuredata/sss\_(?P<source>[A-Z]+)\_(?P<environment>[a-z]+)\_Treasuredata/(SEU\_)?(?P<table>[A-Z\_]+)?\_((?P<month>[0-9]{1,2})\_)?((?P<year>[0-9]{4})\_)?((?P<extractionday>[0-9]{8})\_)?(?P<batch>[0-9]{3})?(?P<file_extension>\..{3,6})?', fpath)
    #matches = re.search(r'Treasuredata\\sss\_(?P<source>[A-Z]+)\_(?P<environment>[a-z]+)\_Treasuredata\\(?P<filename>.+)', fpath)
    table_name = None
    initial_load = False
    if matches:
        finfo = matches.groupdict()
        if finfo['environment'] == 'prod':
            if finfo['table'].endswith('_IL'):
                initial_load = True
            table_name = finfo['table'].replace('_IL', '').lower()
            src = finfo['source']
        else:
            print(f'skipping non-prod file {fpath}')
            continue
    else:
        dir_matches = re.search(r'Treasuredata/sss\_(?P<source>[A-Z]+)\_(?P<environment>[a-z]+)\_Treasuredata/(?P<filename>.+)', fpath)
        if dir_matches:
            dinfo = dir_matches.groupdict()
            if dinfo['environment'] == 'prod':
                src = dinfo['source']
                match src:
                    case 'BW':
                        table_name = 'bw_price'
                    case 'RE':
                        table_name = 'redemption_engine_claims'
                    case 'RAMEN':
                        table_name = 'ramen'
                    case 'IE':
                        table_name = '_'.join(dinfo['filename'].split('_')[:-1])
                    case 'POWERBI':
                        print(f'skipping PowerBI file {fpath}')
                        continue
                    case _:
                        print(f'unmatched source {src} in file {fpath}')
                        continue
            else:
                print(f'skipping non-prod file {fpath}')
                continue
        else:
            print(f'unhandled directory path {fpath}')
            continue
    if table_name:
        prod_files.append({
            'filepath': fpath,
            'local_filepath': os.path.normpath(f'D:/sss/{fpath}'),
            'source': src,
            'table_name': f'db_raw.{table_name}',
            'initial_load': initial_load,
            'size_in_mb': key['Size'] / (1024**2),
            'last_modified': key['LastModified']
        })
df_files = pd.DataFrame(prod_files)


 
dfs = df_sources.merge(
    df_tables,
    how = 'left',
    left_on = 'table',
    right_on = 'table_name'
    
).merge(
    df_s3_summary.rename({
        'db_raw.bw_price': 'db_raw.bw_price_in',
        'db_raw.camera_lens_data': 'db_raw.ie_camera_lens_data',
        'db_raw.mdcim_user_optins': 'db_raw.ie_mdcim_user_optins',
        'db_raw.q1_answers': 'db_raw.ie_q1_answers',
        'db_raw.q2_answers': 'db_raw.ie_q2_answers',
        'db_raw.user_data': 'db_raw.ie_user_data',
        'db_raw.production_mdcim_users_optins_iem_eu': 'db_raw.ie_mdcim_user_optins'
    }),
    how = 'left',
    left_on = 'table',
    right_on = 'table_name'
).merge(
    df_files[['filepath','initial_load','linecount']].rename({'linecount': 'last_file_linecount'}, axis=1),
    how = 'left',
    left_on = 'last_path',
    right_on = 'filepath'
).merge(
    df_files[['source','table_name']].rename({'source': 'sss_source_system'}, axis=1).drop_duplicates().groupby('table_name')['sss_source_system'].apply(lambda x: ','.join(x)).rename({
        'db_raw.bw_price': 'db_raw.bw_price_in',
        'db_raw.camera_lens_data': 'db_raw.ie_camera_lens_data',
        'db_raw.mdcim_user_optins': 'db_raw.ie_mdcim_user_optins',
        'db_raw.q1_answers': 'db_raw.ie_q1_answers',
        'db_raw.q2_answers': 'db_raw.ie_q2_answers',
        'db_raw.user_data': 'db_raw.ie_user_data',
        'db_raw.production_mdcim_users_optins_iem_eu': 'db_raw.ie_mdcim_user_optins'
    }),
    how = 'left',
    left_on = 'table',
    right_index = True
)

dfs['header_rows'] = dfs.apply(lambda row: row['file_count'] if row['table_name'] != 'db_raw.bw_price_in' else 0 , axis=1)
dfs['last_file_header_rows'] = dfs.apply(lambda row: 1 if row['table_name'] != 'db_raw.bw_price_in' else 0 , axis=1)
dfs['fraction_of_total_lines_loaded'] = dfs['rows'] / (dfs['linecount'] - dfs['header_rows'])
dfs['fraction_of_last_lines_loaded'] = dfs['rows'] / (dfs['last_file_linecount'] - dfs['last_file_header_rows'])
dfs['fraction_of_lines_loaded'] = dfs.apply(lambda row: row['fraction_of_total_lines_loaded'] if row['mode']=='append' else row['fraction_of_last_lines_loaded'], axis=1)


tables_cleared = set()
for row in dfs.sort_values('table').to_dict(orient='index').values():
    #print(row['table'], row['fraction_of_lines_loaded'])
    #if row['sss_source_system'] == 'CDWH,SFMC':
    #if row['table'].startswith('db_raw.in_emv_non_deliverables'):
    #if 'bound' in row['table']:
    if row['table'] == 'db_raw.fact_customer_redemption':
        #print(row)
        connector = [connector for connector in connectors if connector['name'] == row['source_name']][0]
        connector['config']['in']['access_key_id'] = os.environ.get('sss_S3_TD_KEY')
        connector['config']['in']['secret_access_key'] = os.environ.get('sss_S3_TD_SECRET')
        
        mode_asis = row['mode']
        mode_tobe = row['mode_tobe']
        incremental_asis = row['incremental']
        incremental_tobe = row['incremental_tobe']
        if (incremental_asis!=incremental_tobe or
        mode_asis!=mode_tobe or
        row['fraction_of_lines_loaded']<0.98 or
        row['fraction_of_lines_loaded']>1.0 or
        (incremental_tobe and mode_tobe == 'replace_on_new_data' and row['files_to_process'] != 1) or
        (incremental_tobe == False and mode_tobe == 'replace_on_new_data')):
            print (row['table'])
            
            # update source
            connector['config']['in']['incremental'] = incremental_tobe
            connector['config']['out']['mode'] = mode_tobe
            if incremental_tobe and mode_tobe == 'replace_on_new_data' or 'bound' in row['table']:
                connector['config']['in']['total_file_count_limit'] = 1
                row['files_to_process'] = 1
            connector['config_diff']['in']['last_path'] = ''
            api_client.api.connector_update(name=connector['name'], job=connector)
            
            # rename files
            """
            for fpath_in in df_files[df_files.table_name==row['table']]['filepath'].to_list():
                if '_IL' in fpath_in:
                    fpath_out = fpath_in.replace('_IL', '_0IL')
                    print(f'{fpath_in} > {fpath_out}')
                    # Copy object A as object B
                    try:
                        s3_resource.Object(bucket, fpath_out).copy_from(CopySource=bucket+'/'+fpath_in)

                        sleep(3)
                        # Delete the former object A
                        s3_resource.Object(bucket, fpath_in).delete()            
                    except:
                        pass
            """
            
            # clear table
            if row['table'] not in tables_cleared:
                client.query(f"delete from {row['table']} where time>0")
                tables_cleared.add(row['table'])
                
            # reingest data, once per file if incremental replacements in use, to check each in turn and ensure brought up to the latest last_path, last run should do nothing
            if row['files_to_process'] == 1:
                runs = row['file_count']+1
            else:
                runs = 2
            #if 'bound' in row['table']:
            #    runs=1
            print(row['table'], runs, 'runs')
            #continue
            for i in range(runs):
                print(f'{datetime.now()} run {i+1}/{runs}')
                job = api_client.api.connector_run(connector['name'])
                job_id = job['job_id']
                while api_client.api.job_status(job_id)!='success':
                    sleep(5)
