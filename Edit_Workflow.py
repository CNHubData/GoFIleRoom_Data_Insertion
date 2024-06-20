# %%
import pandas as pd
import pyodbc
from sqlalchemy import create_engine, MetaData, Table, Column, VARCHAR, DATETIME, SMALLINT, PrimaryKeyConstraint, text
import requests
import configparser
import time
from datetime import timedelta
from datetime import datetime
import json
import os
from pandas import json_normalize
import uuid
import pytz
from sqlalchemy.exc import IntegrityError
from sqlalchemy.exc import SQLAlchemyError
from flask import Flask, request, jsonify
import pandas as pd
from io import StringIO

# %%
# print("Current Working Directory:", os.getcwd())
# config_path = os.path.join(os.getcwd(), 'config.ini')
# print("Expected config.ini path:", config_path)
# print("Does the config.ini exist at the expected path?", os.path.exists(config_path))

# %%
# Load the config file
config = configparser.ConfigParser()
config.read(r'C:\Users\jarp\Documents\Data Analytics\Workflows\config.ini')

# %%
# Access the database credentials
db_username = config['Database']['DBUsername']
db_password = config['Database']['DBPassword']

# %%
# Access the API credentials
api_login_name = config['API']['LoginName']
api_password = config['API']['Password']
api_key = config['API']['APIKey']

# %%
def authenticate(api_key, api_login_name, api_password):
    """Authenticate via username/password. Returns json token object."""
    uri_path = 'https://api.thomsonreuters.com/gofileroom/api/v1/user/login'
    headers = {
               'X-TR-API-APP-ID': api_key
               }
    body = {
        "LoginName": api_login_name,
        "Password": api_password,
        "Language": "en",
        "Capcha": "",
        "CapchaType":0
    }

    # Send POST request
    response = requests.post(uri_path, headers=headers, json=body)

    # Check for a successful response
    if response.status_code == 200:
        # Parse and return the token from the response
        token = response.json().get('token')
        return token
    else:
        # Print error message and return None if authentication fails
        print(f'Failed to authenticate: {response.status_code} - {response.text}')
        print(f'Response headers: {response.headers}')
        return None

# %%
token = authenticate(api_key, api_login_name, api_password)
if token:
    print('Authentication successful')
else:
    print('Authentication failed')

# %%
# Path to Excel
invoices = pd.read_excel(r'C:/Users/jarp/Documents/Data Analytics/Workflows/Tax Invoice History and propsed 2023 TR.xlsx', sheet_name='Proposed UT Amount')

rename_dict = {
    invoices.columns[0]: 'clientNumber',
}

invoices = invoices.rename(columns=rename_dict)

invoices.head()

# %%
# Path to Database
conn_str = (
    f'mssql+pyodbc://{db_username}:{db_password}@'
    f'CN-ANALYTICS\\/FIRMFLOWAPI?driver=ODBC Driver 17 for SQL Server'
    )
engine = create_engine(conn_str)

def get_related_groups(engine):
    query = text("""
       SELECT 
        DISTINCT 
            clientNumber, 
            filingID, 
            deliverables
       FROM TAX
       WHERE year = 2023
                 """)

    with engine.connect() as conn:
        result = conn.execute(query)
        records = result.fetchall()
        df = pd.DataFrame(records, columns=['clientNumber', 'filingID', 'deliverables'])
    return df

# Get the data from the database
db_data = get_related_groups(engine)

db_data.head()

# %%
invoices.shape

# %%
invoices['clientNumber'] = invoices['clientNumber'].astype(str)
db_data['clientNumber'] = db_data['clientNumber'].astype(str)
df = pd.merge(db_data, invoices, how='left', on='clientNumber')
df

# %%
def edit_workflow(api_key, token, filing_id, deliverables, invoice_value):
    '''Construct the payload with the page number and the last updated timestamp'''
    api_url = 'https://api.thomsonreuters.com/gofileroom/firmflow/api/V1/Workflow/EditWorkflow'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Basic ' + token,
        'X-TR-API-APP-ID': api_key
    }

    payload = {
        "filingId": int(filing_id),  # Ensure filingId is an integer
        "deliverables": deliverables,
        "informationFields": {
            "name": "2024 TR Est. Invoice",
            "value": str(invoice_value)  # Ensure invoice_value is a string
        }
    }

    try:
        response = requests.post(api_url, headers=headers, json=payload)
        response_data = response.json()

        if response.status_code == 200 and response_data.get('status', False):
            print(f"Successfully updated workflow for filing ID: {filing_id}")
        else:
            print(f"Failed to update workflow for filing ID: {filing_id}")
            print(f"Response: {response_data}")

    except Exception as e:
        print(f"An error occurred while updating filing ID: {filing_id}")
        print(f"Error: {str(e)}")

def process_data(invoices, db_data, api_key, token):
    for index, row in invoices.iterrows():
        client_id = row['clientNumber']
        client_name = row['Client Name']
        invoice_value = row['2023 TR']

        # Find the corresponding clientNumber in the database data
        client_data = db_data[db_data['clientNumber'] == client_id]

        for _, client_row in client_data.iterrows():
            filing_id = client_row['filingID']
            deliverables = client_row['deliverables']
            # Print the details
            print(f"Client Name: {client_name}, Filing ID: {filing_id}, Deliverables: {deliverables}, Invoice: {invoice_value}")
            # Call the edit_workflow function with the required parameters
            edit_workflow(api_key, token, filing_id, deliverables, invoice_value)
            # Add a short delay to avoid hitting the rate limit
            time.sleep(1)

# Process the data
process_data(invoices, db_data, api_key, token)
