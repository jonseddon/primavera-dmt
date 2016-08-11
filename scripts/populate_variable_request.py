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
CLIENT_SECRET_FILE = '../etc/client_secret_920707869718-bjp97l2ikhi0qdqi4ibb5ivcpmnml7n8.apps.googleusercontent.com.json'
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
        else: # Needed only for compatibility with Python 2.6
            credentials = tools.run(flow, store)
        print('Storing credentials to ' + credential_path)
    return credentials

def main():
    """
    """
    credentials = get_credentials()
    http = credentials.authorize(httplib2.Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

    spreadsheetId = '1RyAtRixFpC5eMz94TyJE8QLbp1jxhrrKWGV_op5ch5Q'
    rangeName = '3hr!A2:AD'
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheetId, range=rangeName).execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
    else:
        # print('Name, Major:')
        # for row in values:
        #     # Print columns A and E, which correspond to indices 0 and 4.
        #     print('%s, %s' % (row[0], row[4]))
        for row in values:
            _vr = get_or_create(VariableRequest, long_name=row[1], units=row[2],
                var_name=row[5], standard_name=row[6], cell_methods=row[7],
                positive=row[8], variable_type=VARIABLE_TYPES[row[9]],
                dimensions=row[10], cmor_name=row[11], modeling_realm=row[12],
                frequency=row[13], cell_measures=row[14], uid=row[21])


if __name__ == '__main__':
    main()
