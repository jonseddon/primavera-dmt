#!/usr/bin/env python2.7
"""
NetCDF inventory Elastic Tape database updater

Fetches the web page describing all of the elastic tape holdings and updates
each data file with the information available in that.
"""
import argparse
import requests
from lxml import html
from datetime import datetime as dt
import logging
import pytz
from django.utils.timezone import make_aware

from ncdf_indexer import NetCDFIndexedFile, NoDatafileError, FileSizeError


ET_URL_FORMAT = "http://et-monitor.fds.rl.ac.uk/et_user/ET_Holdings_Summary.php?workspace={0};caller={1};level=files"
ET_TIME_TO_ARCHIVE_FORMAT = "%Y-%m-%d %H:%M:%S"

ET_SERVER_TIME_ZONE = 'Europe/London'

def process_row(row, **kwargs):
    '''
    process a single row from one of the HTML tables
    Arguments:
     * row:
        list of table row elements. Only the first four are used

    all other keywords are passed on to netcdf_indexer.NetCDFIndexedFile.update_from_et
    '''

    filename, size, state, time_to_archive = row[:4]
    size = int(size)
    date_archived = dt.strptime(time_to_archive, ET_TIME_TO_ARCHIVE_FORMAT)

    server_tz = pytz.timezone(ET_SERVER_TIME_ZONE)
    date_archived = make_aware(date_archived, server_tz)

    df = NetCDFIndexedFile(filename)
    try:
        df.retrieve()
    except NoDatafileError as err:
        logging.debug("NoDatafileError %s" % err)
        return

    messages = df.update_from_et(file_size=size,
                                 state=state,
                                 date_archived=date_archived,
                                 **kwargs)
    for m in messages:
        logging.debug(m)


def get_tables(workspace, caller):
    '''
    Retrieve information from the Elastic Tape inventory (html interface)
    Arguments:
     * workspace:
        workspace to access
     * caller:
        user id of the owner of the workspace
    '''

    et_url = ET_URL_FORMAT.format(workspace, caller)
    page = requests.get(et_url)
    tree = html.fromstring(page.text)
    tables = tree.xpath('//@id')

    batch_tables = {}

    for table_name in tables:
        if "batch_file_list" not in table_name:
            continue
        batch_number = int(table_name.split("_")[-1])
        t = tree.get_element_by_id(table_name)
        tlist = [[i.text for i in tr.getchildren()] for tr in t.getchildren()]
        batch_tables[batch_number] = tlist

    return batch_tables

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="NetCDF inventory Elastic Tape database updater")
    parser.add_argument("workspace", type=str, help="Workspace name")
    parser.add_argument("caller", type=str, help="Workspace owner")
    parser.add_argument("--overwrite", action="store_true",
                        help="overwrite file size (USE WITH CARE)")

    args = parser.parse_args()

    batch_tables = get_tables(args.workspace, args.caller)

    logging.basicConfig(level=logging.DEBUG)

    for batch_id, batch_table in batch_tables.iteritems():
        for row in batch_table[2:]:
            try:
                process_row(row, batch_id=batch_id,
                            workspace=args.workspace,
                            overwrite=args.overwrite)
            except NoDatafileError as err:
                print err, "... Skipping"
            except FileSizeError as err:
                print err, "... Continuing"
