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

from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

import django
django.setup()
from django.db.models import Sum  # NOPEP8
from pdata_app.models import DataRequest, DataFile  # NOPEP8


# The ID of the Google Speadsheet (taken from the sheet's URL)
SPREADSHEET_ID = '1fKslvfeXiKcUYpis4BK3z2ceHzF6cO_ivf5FmjfvBWw'

LAST_LINE = 673


def calc_item_size(path):
    """
    Calculate the size in bytes of all datasets below the specified `path`.

    :param str path: the path to the item
    :returns: the size in bytes
    """
    filter_terms = {
        'datafile__isnull': False,
        'project__short_name': 'CMIP6'
    }
    components = path.split('/')
    index_to_str = {
        0: 'climate_model__short_name',
        1: 'experiment__short_name',
        2: 'rip_code',
        3: 'variable_request__table_name',
        4: 'variable_request__cmor_name'
    }
    for index, cmpn in enumerate(components[7:]):
        filter_terms[index_to_str[index]] = cmpn

    data_reqs = DataRequest.objects.filter(**filter_terms).distinct()
    total_file_size = (DataFile.objects.filter(data_request__in=data_reqs).
                       distinct().aggregate(Sum('size'))['size__sum'])

    return total_file_size


def main():
    """
    Run the processing.
    """
    scopes = 'https://www.googleapis.com/auth/spreadsheets'
    store = file.Storage('token.json')
    creds = store.get()
    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', scopes)
        creds = tools.run_flow(flow, store)

    for row_index in range(2, LAST_LINE + 1):
        try:
            range_name = f'HighResMIP!{row_index}:{row_index}'
            service = build('sheets', 'v4', http=creds.authorize(Http()))
            result = service.spreadsheets().values().get(
                spreadsheetId=SPREADSHEET_ID, range=range_name).execute()
            values = result.get('values', [])

            if not values:
                print(f'No data found for row_index {row_index}.')
            else:
                # We're only getting one row, but the API's returning a 2D set
                # and so we need to loop over this to get each row
                for row in values:
                    # Add extra columns if required so that we can add an
                    # item later
                    new_column_index = 15
                    num_blanks_required = new_column_index + 1 - len(row)
                    for blank_num in range(num_blanks_required):
                        row.append('')
                    item_size = calc_item_size(row[0])
                    row[15] = item_size / 1024**4

                body = {
                    'values': values
                }
                result = service.spreadsheets().values().update(
                    spreadsheetId=SPREADSHEET_ID, range=range_name,
                    valueInputOption='USER_ENTERED', body=body).execute()
        except Exception:
            print(f'Exception on line {row_index}')
        # if row_index > 10:
        #     break


if __name__ == '__main__':
    main()
