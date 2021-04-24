#!/usr/bin/python
# -*- coding: utf-8 -*-
# Version 0.2b, 2021/04/24
# token: Version: 0.2b
# token: Libs and config needed:
# token: make venv
# token: pip install apiclient
# token: pip install oauth2client
# token: pip install pyodbc
# token: get right measures and dims
# token: DONE: ga account list

# token: DONE 2021/04/24 v0.2b: Add minimums, maximums, totals from GA JSON
# token: TODO: utf-8 signs in pageTitle
# token: TODO: anti_sample=TRUE
# token: TODO: auto-repair process
# token: TODO: old data insert process (from start_date)
# token: TODO: other sets (like adsense) with same dimensions
# token: TODO: password to SQL Database stored in external file
# token: TODO: JSON Output with double-quotas
# token: TODO: Error handling
# token: TODO: Do data frame output, export to csv
# token: TODO: transform code to PEP8
# token: TODO: Do documentation on wiki-based web
# token: TODO: Do documentation: add DNS and NON-DNS connect to SQL DB
# token: TODO: Variables for destination RDS Table



"""Hello Analytics Reporting API V4."""

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import json
import pyodbc
from datetime import datetime, timedelta
import time

# tomkenig: variables
#DATE_ID = str(datetime.strptime(str(datetime.now() - timedelta(days=3))[0:10], "%Y-%m-%d"))[0:10]
VIEW_ID_LIST = []
DATE_ID_LIST = []
SQL_DB_CONN_STRING = {}

# Number of days to download from GA
DAYS_TO_GET = 3

# tomkenig: google analytics connection
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'google_service_account.json'

# tomkenig: get ODBC connection string from stored file
with open('sql_db_connection_string.json') as json_sql_con:
    SQL_DB_CONN_STRING = (json.load(json_sql_con))["connection_string"]
    print(SQL_DB_CONN_STRING)

# ODBC connecion
    # Specifying the ODBC driver, server name, database, etc. directly
conn = pyodbc.connect(SQL_DB_CONN_STRING)
    # Using a DSN, but providing a password as well
    # conn = pyodbc.connect('DSN=test;PWD=password')
    # Create a cursor from the connection
# tomkenig: cursors for data and ga list ov views numbers
cursor = conn.cursor()


# tomkenig: Returns list of dates to check with GA v4 API. Get right date_id. Can get yesterday dt, 3days ago date, ...
# tomkenig: Params: 1- yesterday, 3 - 3days ago date up to today, x- x days before up to today.

def get_date_id_list(days_ago):
    date_list = []
    for i in range(days_ago):
       date_list.append(str(datetime.strptime(str(datetime.now() - timedelta(days=i+1))[0:10], "%Y-%m-%d"))[0:10])
    return date_list[0:days_ago]


# tomkenig: Get views list from your internal DB (DWHLITE)
def get_views_ids():
    cursor.execute("select d_ga_view_id from dwhlite.dim.d_ga_views ")
    view_list=[]
    for row in cursor.fetchall():
       view_list.append(row[0])
    return view_list


def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics


def get_report(analytics, VIEW_ID, DATE_ID):
  """Queries the Analytics Reporting API V4.

  Args:
    analytics: An authorized Analytics Reporting API V4 service object.
  Returns:
    The Analytics Reporting API V4 response.
  """
  #tomkenig: convert VIEW_ID to string is needed
  VIEW_ID = str(VIEW_ID)
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': DATE_ID, 'endDate': DATE_ID}],
          'metrics': [{'expression': 'ga:sessions'},
                     {'expression': 'ga:pageviews'},
                     {'expression': 'ga:users'},
                     {'expression': 'ga:newUsers'},
                     {'expression': 'ga:bounces'},
                     {'expression': 'ga:timeOnPage'},
                     {'expression': 'ga:sessionDuration'},
                     {'expression': 'ga:uniquePageviews'}],
          'dimensions': [{'name': 'ga:country'},
                         {'name': 'ga:pagePath'},
                         {'name': 'ga:dateHour'},
                         {'name': 'ga:pageTitle'},
                         {'name': 'ga:sourceMedium'},
                         {'name': 'ga:deviceCategory'},
                         {'name': 'ga:fullReferrer'},
                         {'name': 'ga:landingPagePath'},
                         {'name': 'ga:exitPagePath'}
                         ]
        }]
      }
  ).execute()



def print_response(response):
  """Parses and prints the Analytics Reporting API V4 response.

  Args:
    response: An Analytics Reporting API V4 response.
  """
  data_json_output = []
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    # print(columnHeader)
    # print(dimensionHeaders)
    # print(metricHeaders)
    maximums = report.get('data', {}).get('maximums', [])
    minimums = report.get('data', {}).get('minimums', [])
    totals = report.get('data', {}).get('totals', [])
    isDataGolden = report.get('data', {}).get('isDataGolden', [])

    for row in report.get('data', {}).get('rows', []):
      dimensions = row.get('dimensions', [])
      dateRangeValues = row.get('metrics', [])
      for i in enumerate(dateRangeValues):
          data_json_output = data_json_output + [{"dimensions":dimensions, "measures":(i[1]['values'])}]

  return({'data': data_json_output,
          'dimensionHeaders': dimensionHeaders,
          'metricHeaders': metricHeaders,
          'maximums': maximums,
          'minimums': minimums,
          'totals': totals,
          'isDataGolden': isDataGolden})


def main():
   # tomkenig: dates 1- yesterday, 20 - list of 20 days ago since yesterday
   DATE_ID_LIST = get_date_id_list(DAYS_TO_GET)
   DATE_ID_LIST.reverse()

   for j in DATE_ID_LIST:
      print(j)

   #tomkenig: views list
   VIEW_ID_LIST = get_views_ids()
   for i in VIEW_ID_LIST:
      print(i)

   #tomkenig: delete and insert data into SQL DB
   for j in DATE_ID_LIST:
      for i in VIEW_ID_LIST:
         analytics = initialize_analyticsreporting()
         response = get_report(analytics, i, j)

         # SQL OVERWRITE: delete and insert
         cursor.execute("delete from dwhlite.ga.dat_google_analytics where d_date_id=? and d_ga_view_id=?", int(j.replace('-', '')), i)
         print('sql delete done' +' ga view:' + str(i) + ' date:' + str(j))
         cursor.execute("insert into dwhlite.ga.dat_google_analytics(d_ga_view_id, ga_dat, d_date_id) values (?,?,?)", i, str(json.dumps(print_response(response))), int(j.replace('-', '')))
         print('sql insert done' +' ga view:' + str(i) + ' date:' + str(j))
         conn.commit()

         # tomkenig: x seconds sleep before next for loop run
         # time.sleep(1)

print('all done')

if __name__ == '__main__':
   main()
