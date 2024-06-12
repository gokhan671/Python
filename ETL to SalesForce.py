import requests
import logging
import pandas as pd
import pytd
from requests.sessions import Session
import json
import sqlite3

session = requests.Session()
dbSQLiteConnection = sqlite3.connect('store_db.db',isolation_level=None)


def InsertValue2DB(x_value, record_time ):
    try:
        dbSQLiteConnection.execute(f"INSERT INTO data_list (x_value,record_time) VALUES ( '{x_value}', '{record_time}' )")
    except Exception as ex:
        print(str(ex))

def create_request_post(url, payload, headers):
    req = requests.Request('POST', url, data=payload, headers=headers)
    prepped = session.prepare_request(req)
    response = session.send(prepped)

    return response

def checkExistsInDB(x_value, record_time):
    isExists = False
    select_query =f"SELECT count(*) as 'exists'  from data_list where x_value = '{x_value}' and record_time >= '{record_time}'"
    result = dbSQLiteConnection.execute(select_query).fetchone()
    isExists = True  if  result[0] >  0 else False
    print(f'already in db  x_value = {x_value} and record_time {record_time}' if isExists else 'new records')
    return isExists


def call_klaviyo_api(profile_data):
    url = "https://a.klaviyo.com/api/profile-subscription-bulk-create-jobs/"

    headers = {
        "accept": "application/json",
        "revision": "2023-10-15",
        "content-type": "application/json",
        "Authorization": "Klaviyo-API-Key xxx"
    }

    payload = {
        "data": {
            "type": "profile-subscription-bulk-create-job",
            "attributes": {
                "custom_source": "Marketing Event",
                "profiles": {"data": profile_data},
            },
            "relationships": {"list": {"data": {"type": "list", "id": "TNxnvs"}}},
        }
    }

    json_payload = json.dumps(payload)

    response = create_request_post(url, payload=json_payload, headers=headers)
    print(response)


def get_query_result():
    td_client = pytd.Client(apikey='xxxx',
                            endpoint='https://api.treasuredata.com/', database='src', default_engine='presto')

    query = '''  
    with v_main as
    (
    select
        DISTINCT coalesce(trim(lower(email)),'') as email,
        COALESCE(case when SUBSTR(phone_number,1,2) = '+1' and LENGTH(phone_number) = 12 then phone_number ELSE NULL  END ,'') as phone,
        CASE
        WHEN TRIM(LOWER(marketing_consent)) IN ('true', 'yes', '1') then '1'
        WHEN TRiM(LOWER(marketing_consent)) IN ('false', 'no', '0') then '0'
        else null
    end as  email_consent_flag,
        sms_opt_in as sms_consent_flag,
    time 
    FROM src.js_form_fill_quiz 
    WHERE
        email IS NOT NULL
        AND email NOT IN ('', ' ')
        AND REGEXP_LIKE(email, '^[a-zA-Z0-9][a-zA-Z0-9._+-]*[a-zA-Z0-9_\-]@[a-zA-Z0-9][a-zA-Z0-9._+-]*[a-zA-Z0-9]\.[a-zA-Z]+$')
        --AND is_unsubscribed = 0
        --and TD_INTERVAL(time, '-10m/now')
    ),
    v_union_all as
    (
      select 'phone' as x_key, phone as x_value, email as other_x_value, time  from v_main where phone not in ('') AND sms_consent_flag  IN ('1')
    )
    select * from v_union_all
    order by time desc
  '''
    result = td_client.query(query)
    return result


def startProcess():
    result = get_query_result()
    df = pd.DataFrame(result['data'], columns=result['columns'])
    iter_no = 0
    profiles_list_for_sms = []

    for x, row in df.iterrows():
        iter_no = iter_no + 1
        processing_key = str(row["x_key"])
        x_value = str(row["x_value"])
        other_x_value = str(row["other_x_value"])
        record_time = int(row["time"])

        print(f'processing key is {processing_key} value is {x_value} and email is {other_x_value} time is {record_time}')

        if( not checkExistsInDB( x_value = x_value , record_time= record_time)):
            InsertValue2DB( x_value= x_value, record_time= record_time)
        
        # if x_key is phone
        if processing_key == 'phone':
            profile = {'type': 'profile', 'id': '', 'attributes': {'email': other_x_value,'phone_number': x_value,'subscriptions': {"sms": {"marketing": {"consent": "SUBSCRIBED"}}}}}

            profiles_list_for_sms.append(profile)

        if (len(profiles_list_for_sms) == 50):
            call_klaviyo_api(profiles_list_for_sms)
            profiles_list_for_sms.clear()

    if (len(profiles_list_for_sms) >0):
        call_klaviyo_api(profiles_list_for_sms)

startProcess()