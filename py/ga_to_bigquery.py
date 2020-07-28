#!/usr/bin/env python3
from googleapiclient.discovery import build
import sys
from time import sleep
import pandas as pd
import copy

'''
Extract Google Analytics API report data and insert into BigQuery.

Usage:
  python ga_to_bigquery.py 2020-06-01 2020-06-30

You can specify

- date range programatically in line 122-123
- report query in line 119-
- authentication method between user-based and service account-based in line 50-101

Authentication method:

1. service account based  
    This service account processes extracting from Analytics and inserting to Bigquery.
    This account must have both GCP(BigQuery) IAM permission and Google Analytics(Read & Analyze) permission.
2. user based 1: Use previously issued credential  
    When you have a credential json file, paste the content into `authorized_user_info`.
3. user based 2: Auto-authentication  
    When the credential does not exist, authentication process starts automatically; else the credential file is used
    Generated credential file is stored in the current directory as `pydata_google_credentials.json`.

Choose one of the three and comment out the others.
'''

################################################################################
# Logging
################################################################################
from logger import getLogger, StreamHandler, Formatter, INFO
logger = getLogger(__name__)
logger.setLevel(INFO)
handler = StreamHandler()
handler.setLevel(INFO)
formatter = Formatter('%(asctime)s - %(filename)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.propagate = False

################################################################################
# Authentication
################################################################################
# 1. By service account (Paste key file content in `service_account_key` definition.)
from google.oauth2 import service_account
service_account_key = {
  "type": "service_account",
  "project_id": "my-project",
  "private_key_id": "aaaaaaaaaabbbbbbbbbbccccccccccdddddddddd",
  "private_key": "-----BEGIN PRIVATE KEY-----\nMIIE...1X9dS\n-----END PRIVATE KEY-----\n",
  "client_email": "user1@my-project.iam.gserviceaccount.com",
  "client_id": "999999999999999999999",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/user1%40my-project.iam.gserviceaccount.com"
}
credentials = service_account.Credentials.from_service_account_info(service_account_key)
scoped_credentials = credentials.with_scopes(
  [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/analytics.readonly'
  ])

# 2. Auth by user account (info: Paste credential file content in `authorized_user_info` definition.)
'''
from google.oauth2 import credentials
authorized_user_info = {"refresh_token": "1//aaaaa-bbbbbbb", "id_token": null, "token_uri": "https://accounts.google.com/o/oauth2/token", "client_id": "99999999999-999999aaaaaaaaaa9999aaaaaaaaaaaa.apps.googleusercontent.com", "client_secret": "aaaaaaaaaaaaaaaaaaaaaaaa", "scopes": ["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/analytics.readonly"]}
scoped_credentials = credentials.Credentials.from_authorized_user_info(
  authorized_user_info,
  [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/analytics.readonly'
  ]
)
'''

# 3. Auth by user account (save/read auth file in current directory)
'''
import pydata_google_auth, os
CLIENT_ID = '99999999999-999999aaaaaaaaaa9999aaaaaaaaaaaa.apps.googleusercontent.com'
CLIENT_SECRET = 'aaaaaaaaaaaaaaaaaaaaaaaa'
scoped_credentials = pydata_google_auth.get_user_credentials(
  [
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/analytics.readonly'
  ],
  client_id=CLIENT_ID,
  client_secret=CLIENT_SECRET,
  credentials_cache=pydata_google_auth.cache.ReadWriteCredentialsCache(dirname=os.path.abspath('.'), filename='pydata_google_credentials.json')
)
'''

################################################################################
# Date
################################################################################
#from datetime import datetime, timedelta, timezone
#JST = timezone(timedelta(hours=+9), 'JST')
#now = datetime.now(JST)
#_7days_ago = now-timedelta(days=7)
#str_7days_ago = _7days_ago.strftime('%Y-%m-%d')
#_3days_ago = now-timedelta(days=3)
#str_3days_ago = _3days_ago.strftime('%Y-%m-%d')

################################################################################
# Config
################################################################################
# https://developers.google.com/analytics/devguides/reporting/core/v4/rest/v4/reports/batchGet?ReportRequest
request_body = {
  'reportRequests': [{
    'viewId':'ga:99999999',
    'dateRanges':[{
      'startDate':sys.argv[1], # '2019-03-17' ; You can specify date string defined above
      'endDate':sys.argv[2] # '2019-03-19' ; You can specify date string defined above
    }],
    'samplingLevel': 'LARGE',
    'metrics':[
      { 'expression':'ga:pageviews' },
      { 'expression':'ga:entrances' }
    ],
    'dimensions': [
      { 'name':'ga:sourceMedium' },
      { 'name':'ga:campaign' },
      { 'name':'ga:deviceCategory' },
      { 'name':'ga:pagePath' },
      { 'name':'ga:clientId' },
      { 'name':'ga:dimension2' },
      { 'name':'ga:sessionCount' }
    ]
  }]
}

################################################################################
# Retrive data
################################################################################
def get_google_analytics(request_body):
  analytics = build('analyticsreporting', 'v4', credentials=scoped_credentials, cache_discovery=False)
  request_body_ = copy.deepcopy(request_body)
  n_requests = len(request_body_['reportRequests'])
  results = [{}] * n_requests
  finished = [False] * n_requests
  pages = [1] * n_requests
  for request in request_body_['reportRequests']:
    request.update({'pageSize': 10000})
  while True:
    try:
      response = analytics.reports().batchGet(body=request_body_).execute()
      reports = [i for i in range(n_requests) if finished[i] == False]
      if len(reports) != len(response.get('reports', [])):
        raise Exception('Number of requests differs number of reports.')
      for i in reversed(range(len(reports))):
        iReport = reports[i]
        report = response.get('reports', [])[i]
        if pages[iReport] == 1:
          dimensionHeaders = report.get('columnHeader', {}).get('dimensions', [])
          metricHeaders = report.get('columnHeader', {}).get('metricHeader', {}).get('metricHeaderEntries', [])
          sampled = report.get('data', {}).get('samplesReadCounts') is not None
          results[iReport] = {
            'sampled': sampled,
            'rows': []
          }
        for row in report.get('data', {}).get('rows', []):
          lRow = row.get('dimensions', [])
          dRow = dict(zip([x.replace('ga:', '') for x in dimensionHeaders], row.get('dimensions', [])))
          j = 0
          for x in metricHeaders:
            k = x.get('name').replace('ga:', '')
            v = row.get('metrics', [])[0].get('values', '')[j]
            if x.get('type') == 'INTEGER':
              dRow.update({k: int(v)})
            elif x.get('type') == 'FLOAT' or x.get('type') == 'CURRENCY' or x.get('type') == 'PERCENT':
              dRow.update({k: float(v)})
            else:
              dRow.update({k: v})
            j += 1
          results[iReport]['rows'].append(dRow)
        
        if report.get('nextPageToken') is not None:
          request_body_['reportRequests'][i].update({'pageToken': report.get('nextPageToken')})
          pages[iReport] += 1
        else:
          request_body_['reportRequests'].pop(i)
          finished[iReport] = True
      if n_requests == sum(finished):
        break
    
    except Exception as e:
      raise
  return(results)

################################################################################
# Process data
################################################################################
logger.info('Started.')

# Retrive the data
results = get_google_analytics(request_body)

# Import to BigQuery
df = pd.DataFrame.from_dict(results[0]['rows'])
logger.info("DataFrame size: %s" % (df.shape,))
df.to_gbq('ga.pvlog', project_id='my-project', credentials=scoped_credentials, chunksize=100000, if_exists='append')

# Execute a query.
query = '''
create or replace table `ga.enriched_pvlog`
partition by date
as
select
  clientId, ...
from ga.pvlog
;
'''[1:-1]
from google.cloud import bigquery
bigquery_client = bigquery.Client(credentials=scoped_credentials, project='my-project')
query_job = bigquery_client.query(query)
query_job.result()
logger.info('Finished.')
