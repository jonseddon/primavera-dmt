#!/usr/bin/env python
"""
add_spinup.py

Based on populate_data_request.py and adds to the PRIMAVERA-DMT DataRequest 
table the spinup-1950 experiment from the PRIMAVERA data request spreadsheet.

Uses the Google Sheets API:
https://developers.google.com/sheets/quickstart/python
To load the spreadsheet at:
https://docs.google.com/spreadsheets/d/1ewKkyuaUq99HUefWIdb3JzqUwwnPLNUGJxbQyqa-10U/
"""
from __future__ import (unicode_literals, division, absolute_import,
                        print_function)
import os
import re

from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools


import django
django.setup()
from pdata_app.models import (DataRequest, Institute, Project, Settings,
                              ClimateModel, Experiment, VariableRequest)
from pdata_app.utils.dbapi import match_one, get_or_create

# The ID of the Google Speadsheet (taken from the sheet's URL)
SPREADSHEET_ID = '1fKslvfeXiKcUYpis4BK3z2ceHzF6cO_ivf5FmjfvBWw'


def main():
    """
    Run the processing.
    """
    SCOPES = 'https://www.googleapis.com/auth/spreadsheets'
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)
    service = build('sheets', 'v4', http=creds.authorize(Http()))


    # Loop through each sheet
    range_name = 'HighResMIP!A2:P'
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID, range=range_name).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        for i, row in enumerate(values):
            # Print columns A and I, which correspond to indices 0 and 8.
            print('%s, %s, %s' % (row[0], row[8], len(row)))
            # values[i][15] = row[8]
            if i > 10:
                break



if __name__ == '__main__':
    main()
