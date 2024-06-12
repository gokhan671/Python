import requests
from requests.sessions import Session
import time
from threading import Thread,local
from queue import Queue
import sys
import os
import requests
import json
import pytd
import pandas as pd
import uuid
import hashlib
import logging
import time
import datetime as dt 
import threading


def hash_email_to_uuid(email):
    # Hash the email using MD5 algorithm
    hashed_email = hashlib.md5(email.encode()).hexdigest()

    # Convert the hashed value to a UUID
    uuid_email = str(uuid.UUID(hashed_email))
    return uuid_email

# Function to get access token
def get_access_token():
    url = "https://xxx.auth.marketingcloudapis.com/v2/token"
    payload = json.dumps({
        "grant_type": "client_credentials",
        "client_id": "xxx",
        "client_secret": "xxx"
    })
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response)
    return json.loads(response.text)['access_token']

# function to retrieve data using Presto
def get_query():
    
    td_client = pytd.Client(apikey='xxxx',
                            endpoint='https://api.treasuredata.com/', database='src', default_engine='presto')
    
    query = td_client.query('''
                            
        SELECT
         LOWER(TRIM(email)) AS email,
         LOWER(TRIM(first_name)) AS FirstName,
         LOWER(TRIM(last_name)) AS LastName,
         CASE WHEN marketing_optin_2 = '1' THEN 'True' ELSE 'False'   END AS OptIN,
         DATE_FORMAT(FROM_UNIXTIME(time, 'America/New_York'), '%Y-%m-%d %H:%i') AS SubmissionDate
        FROM src.js_event_mw_form_fill
        WHERE 
        AND REGEXP_LIKE(email,'^[a-zA-Z0-9][a-zA-Z0-9._+-]*[a-zA-Z0-9_\-]@[a-zA-Z0-9][a-zA-Z0-9._+-]*[a-zA-Z0-9]\.[a-zA-Z]+$')
        and TD_INTERVAL(time, '-5m/now')
        order by time desc    
    ''')
    return query


# API call to send email message
def send_email(row, access_token):
    email = row['email']
    messageKey = hash_email_to_uuid(email)

    messageKey = email
    
    url = f"https://xxx.rest.marketingcloudapis.com/messaging/v1/email/messages/{messageKey}"
    
    payload = json.dumps({
        "definitionKey": "US_MW_Snickers_HP_2024_v2",
        "recipient": {
            "contactKey": row['email'],
            "to": row['email'],
            "attributes": {
                "FirstName": row['FirstName'],
                "LastName": row['LastName'],
                "SubscriberKey": row['email'],
                "EmailAddress": row['email'],
                "OptIN": row['OptIN'],
                "SubmissionDate": row['SubmissionDate'],
                "BrandName": "Snickers",
                "MID": "100011631",
                "ConsentLanguage": "Yes! would like to receive future marketing communication from SNICKERS® and other Mars Wrigley brands via the email provided.",
                "CampaignName": "US_MW_Snickers_HP_2024"
            }
        }
    })
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {access_token}'
    }
    #time.sleep(10)
    response = requests.request("POST", url, headers=headers, data=payload)
    print(f'processed for mail ={messageKey} time = {str(dt.datetime.now())} ')


# Function to authenticate with SFMC API
def authenticate_sfmc():
    try:
        access_token = get_access_token()
        if access_token is None:
            return None
        return access_token
    except Exception as e:
        print(f"Authentication error: {str(e)}")
        return None


# Function to send data to SFMC
def send_to_sfmc():
    #while(True):
        #time.sleep(60)
        try:
            #print(  f'started  at { str(dt.datetime.now())}') 
            access_token = authenticate_sfmc()
            if access_token is None:
                return

            #print("Authentication successful")

            result = get_query()
            #print("Query executed")

            df = pd.DataFrame(result['data'], columns=result['columns'])

            for index, row in df.iterrows():
                t1 = threading.Thread(target=send_email, args=(row, access_token))
                t1.start()
                #t1.join(timeout=1)
                #print(f"Processing email: {row['email']}")  # Print the email before processing
                #send_email(row, access_token)
                #print(f"success email: {row['email']}")  # Print the email before processing
                
            print(  f'process finished success at { str(dt.datetime.now())}') 
                
        except Exception as e:
            print(  f'error at { str(dt.datetime.now())} , {str(e)} ') 
            #logging.error(f"An error occurred: {str(e)}")

 
