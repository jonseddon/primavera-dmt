#!/usr/bin/env python2.7
"""
delete_usage.py

This script is run by the admin to delete previously retrieved data from the
file structure. All other retrievals are checked for files that are still
required online.
"""
import argparse
import datetime
from itertools import chain
import logging.config
import os
import sys

import cf_units

import django
django.setup()
from django.db.models.query import QuerySet

from pdata_app.models import RetrievalRequest
from pdata_app.utils.dbapi import match_one


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

def _date_filter_retrieval_files(retrieval_req, data_req):
    """
    Filter the files belonging to a data request in a retrieval
    request and return only the files that fall between the retrieval's
    start and end dates.

    :param pdata_app.models.RetrievalRequest retrieval_req: The
        retrieval request.
    :param pdata_app.models.DataRequest data_req: The data request
        that we're interested in that belongs to the retrieval.
    :returns: A tuple of the QuerySet of the timeless and QuerySet of
        timed files.
    :rtype: tuple
    """
    all_files = data_req.datafile_set.filter(online=True,
                                             directory__isnull=False)
    if not all_files:
        logger.warning('No online files with a directory found for data '
                       'request {}'.format(data_req))
        return None, None
    time_units = all_files[0].time_units
    calendar = all_files[0].calendar
    start_float = None
    if retrieval_req.start_year is not None and time_units and calendar:
        start_float = cf_units.date2num(
            datetime.datetime(retrieval_req.start_year, 1, 1), time_units,
            calendar
        )
    end_float = None
    if retrieval_req.end_year is not None and time_units and calendar:
        end_float = cf_units.date2num(
            datetime.datetime(retrieval_req.end_year + 1, 1, 1), time_units,
            calendar
        )

    timeless_files = all_files.filter(start_time__isnull=True)

    timed_files = (all_files.exclude(start_time__isnull=True).
        filter(start_time__gte=start_float,
               end_time__lt=end_float))

    return timeless_files, timed_files


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Delete previously restored data files from their current '
                    'location for a specified retrieval request.')
    parser.add_argument('retrieval_id', help='the id of the retrieval request '
        'to carry out.', type=int)
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
        'debug, info, warn (the default), or error')
    parser.add_argument('--version', action='version',
        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    deletion_retrieval = match_one(RetrievalRequest, id=args.retrieval_id)
    if not deletion_retrieval:
        logger.error('Unable to find retrieval id {}'.format(
            args.retrieval_id))
        sys.exit(1)

    if deletion_retrieval.date_deleted:
        logger.error('Retrieval {} was already deleted, at {}.'.
                     format(deletion_retrieval.id,
                            deletion_retrieval.date_complete.strftime(
                                '%Y-%m-%d %H:%M')))
        sys.exit(1)

    if not deletion_retrieval.data_finished:
        logger.error('Retrieval {} is not marked as finished.'.
                     format(deletion_retrieval.id))
        sys.exit(1)

    # loop through all of the data requests in this retrieval
    for data_req in deletion_retrieval.data_request.all():
        timeless_files, timed_files = _date_filter_retrieval_files(
            deletion_retrieval,
            data_req
        )
        if timeless_files is None and timed_files is None:
            continue

        # this is all of the files to delete from this data request of this
        # retrieval request
        files_to_delete = QuerySet.union(timeless_files, timed_files)

        # find any other retrieval requests that still need this data
        other_retrievals = RetrievalRequest.objects.filter(
            data_request=data_req, data_finished=False
        )
        # loop through the retrieval requests that still need this data request
        for ret_req in other_retrievals:
            ret_timeless_files, ret_timed_files = _date_filter_retrieval_files(
                ret_req, data_req
            )

            # remove from the list of files to delete the ones that we have
            # just found are still needed
            files_to_delete = (files_to_delete.difference(ret_timeless_files).
                difference(ret_timed_files))
            # list the parts of the data request that are still required
            logger.debug("{} {} to {} won't be deleted".format(
                data_req, ret_req.start_year, ret_req.end_year))

        # TODO the actual deleting!!!!
        logger.debug('{} {} files will be deleted.'.format(data_req,
                                    files_to_delete.distinct().count()))


if __name__ == "__main__":
    cmd_args = parse_args()

    # determine the log level
    if cmd_args.log_level:
        try:
            log_level = getattr(logging, cmd_args.log_level.upper())
        except AttributeError:
            logger.setLevel(logging.WARNING)
            logger.error('log-level must be one of: debug, info, warn or error')
            sys.exit(1)
    else:
        log_level = DEFAULT_LOG_LEVEL

    # configure the logger
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'standard': {
                'format': DEFAULT_LOG_FORMAT,
            },
        },
        'handlers': {
            'default': {
                'level': log_level,
                'class': 'logging.StreamHandler',
                'formatter': 'standard'
            },
        },
        'loggers': {
            '': {
                'handlers': ['default'],
                'level': log_level,
                'propagate': True
            }
        }
    })

    # run the code
    main(cmd_args)
