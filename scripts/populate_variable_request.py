#!/usr/bin/env python2.7
"""
populate_variable_request.py

Populates the PRIMAVERA-DMT VariableRequest table with details from the PRIMAVERA
data request.

Uses the Google Sheets API: https://developers.google.com/sheets/quickstart/python
To load the spreadsheet at: https://docs.google.com/spreadsheets/d/1RyAtRixFpC5eMz94TyJE8QLbp1jxhrrKWGV_op5ch5Q
Which is copied from Matthew Mizielinski's originhal at: https://docs.google.com/spreadsheets/d/12xidWhF2t1jZ4m_wGfZVIw5GHzckNWuhSvPLCS2BOQo
"""
import httplib2
import os

from apiclient import discovery
import oauth2client
from oauth2client import client
from oauth2client import tools

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None

import django
django.setup()
from pdata_app.models import VariableRequest
from vocabs.vocabs import FREQUENCY_VALUES, VARIABLE_TYPES
from pdata_app.utils.dbapi import get_or_create

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'
#TODO: improve the path on the next line
CLIENT_SECRET_FILE = 'etc/client_secret_920707869718-bjp97l2ikhi0qdqi4ibb5ivcpmnml7n8.apps.googleusercontent.com.json'
APPLICATION_NAME = 'PRIMAVERA-DMT'


def get_credentials():
    """
    Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    home_dir = os.path.expanduser('~')
    credential_dir = os.path.join(home_dir, '.credentials')
    if not os.path.exists(credential_dir):
        os.makedirs(credential_dir)
    credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

    store = oauth2client.file.Storage(credential_path)
    credentials = store.get()
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
        flow.user_agent = APPLICATION_NAME
        if flags:
            credentials = tools.run_flow(flow, store, flags)
        else:  # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials


def main():
    """
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discovery_url = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discovery_url)

    spreadsheet_id = '1RyAtRixFpC5eMz94TyJE8QLbp1jxhrrKWGV_op5ch5Q'

    sheet_names = ['Amon', 'LImon', 'Lmon', 'Omon', 'SImon', 'aero', 'cfMon',
        'emMon', 'emMonZ', 'primMon', 'primOmon', 'Oday', 'cfDay', 'day',
        'emDay', 'emDayZ', 'emDaypt', 'primDay', 'primOday', 'primSIday',
        '6hrPlev', '6hrPlevpt', 'primO6hr', 'prim6hr', 'prim6hrpt', '3hr',
        'em3hr', 'em3hrpt', 'prim3hr', 'prim3hrpt', 'em1hr', 'emSubhr',
        'prim1hrpt', 'fx']

    for sheet in sheet_names:
        range_name = '{}!A2:AD'.format(sheet)
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_name).execute()
        values = result.get('values', [])

        if not values:
            print('No data found.')
        else:
            for row in values:
                try:
                    _vr = get_or_create(VariableRequest, table_name=sheet,
                        long_name=row[1], units=row[2], var_name=row[5],
                        standard_name=row[6], cell_methods=row[7], positive=row[8],
                        variable_type=VARIABLE_TYPES[row[9]], dimensions=row[10],
                        cmor_name=row[11], modeling_realm=row[12],
                        frequency=FREQUENCY_VALUES[row[13]],
                        cell_measures=row[14], uid=row[21])
                except KeyError:
                    # display some information to work out where the error
                    # happened and then re-raise the exception to crash out
                    print 'cmor_name: {} table: {}'.format(row[11], sheet)
                    print row
                    raise

if __name__ == '__main__':
    main()
