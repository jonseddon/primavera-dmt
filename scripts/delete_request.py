#!/usr/bin/env python
"""
delete_request.py

This script is run by the admin to delete previously retrieved data from the
file structure. All other retrievals are checked for files that are still
required online.
"""
from __future__ import unicode_literals, division, absolute_import

import argparse
import logging.config
import os
import sys

import django
django.setup()
from django.utils import timezone

from pdata_app.models import RetrievalRequest, Settings
from pdata_app.utils.common import (delete_drs_dir, construct_drs_path,
                                    date_filter_files)
from pdata_app.utils.dbapi import match_one


__version__ = '0.2.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

# the path to the CEDA archive
CEDA_ARCHIVE = '/badc'

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
    parser.add_argument('-n', '--dryrun', help="see how many files can be "
       "deleted but don't delete any.", action='store_true')
    parser.add_argument('-f', '--force', help="Force the deletion of all "
       "files from this  retrieval even if they are still required by other "
       "retrievals.", action='store_true')
    parser.add_argument('--version', action='version',
        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    logger.debug('Starting delete_request.py for retrieval {}'.format(
        args.retrieval_id))

    deletion_retrieval = match_one(RetrievalRequest, id=args.retrieval_id)
    if not deletion_retrieval:
        logger.error('Unable to find retrieval id {}'.format(
            args.retrieval_id))
        sys.exit(1)

    if deletion_retrieval.date_deleted:
        logger.error('Retrieval {} was already deleted, at {}.'.
                     format(deletion_retrieval.id,
                            deletion_retrieval.date_deleted.strftime(
                                '%Y-%m-%d %H:%M')))
        sys.exit(1)

    if not deletion_retrieval.data_finished:
        logger.error('Retrieval {} is not marked as finished.'.
                     format(deletion_retrieval.id))
        sys.exit(1)

    problems_encountered = False
    directories_found = []
    base_output_dir = Settings.get_solo().base_output_dir

    # loop through all of the data requests in this retrieval
    for data_req in deletion_retrieval.data_request.all():
        online_req_files = data_req.datafile_set.filter(
            online=True, directory__isnull=False
        )
        files_to_delete = date_filter_files(online_req_files,
                                            deletion_retrieval.start_year,
                                            deletion_retrieval.end_year)

        if files_to_delete is None:
            continue

        if not args.force:
            # find any other retrieval requests that still need this data
            other_retrievals = RetrievalRequest.objects.filter(
                data_request=data_req, data_finished=False
            )
            # loop through the retrieval requests that still need this data
            # request
            for ret_req in other_retrievals:
                ret_online_files = data_req.datafile_set.filter(
                    online=True, directory__isnull=False
                )
                ret_filtered_files = date_filter_files(
                    ret_online_files,
                    ret_req.start_year,
                    ret_req.end_year
                )
                if ret_filtered_files is None:
                    continue
                # remove from the list of files to delete the ones that we have
                # just found are still needed
                files_to_delete = files_to_delete.difference(ret_filtered_files)
                # list the parts of the data request that are still required
                logger.debug("{} {} to {} won't be deleted".format(
                    data_req, ret_req.start_year, ret_req.end_year))

        # don't (try to) delete anything that's in the CEDA archive
        files_to_delete.exclude(directory__startswith=CEDA_ARCHIVE)

        # do the deleting
        if args.dryrun:
            logger.debug('{} {} files can be deleted.'.format(data_req,
                files_to_delete.distinct().count()))
        else:
            logger.debug('{} {} files will be deleted.'.format(data_req,
                files_to_delete.distinct().count()))
            for data_file in files_to_delete:
                old_file_dir = data_file.directory
                try:
                    os.remove(os.path.join(data_file.directory,
                                           data_file.name))
                except OSError as exc:
                    logger.error(str(exc))
                    problems_encountered = True
                else:
                    if data_file.directory not in directories_found:
                        directories_found.append(data_file.directory)
                    data_file.online = False
                    data_file.directory = None
                    data_file.save()

                # if a symbolic link exists from the base output directory
                # then delete this too
                if not old_file_dir.startswith(base_output_dir):
                    sym_link_dir = os.path.join(base_output_dir,
                                                construct_drs_path(data_file))
                    sym_link = os.path.join(sym_link_dir, data_file.name)
                    if not os.path.islink(sym_link):
                        logger.error("Expected {} to be a link but it isn't. "
                                     "Leaving this file in place.")
                        problems_encountered = True
                    else:
                        try:
                            os.remove(sym_link)
                        except OSError as exc:
                            logger.error(str(exc))
                            problems_encountered = True
                        else:
                            if sym_link_dir not in directories_found:
                                directories_found.append(sym_link_dir)

    if not args.dryrun:
        # delete any empty directories
        for directory in directories_found:
            if not os.listdir(directory):
                delete_drs_dir(directory)

        # set date_deleted in the db
        if not problems_encountered:
            deletion_retrieval.date_deleted = timezone.now()
            deletion_retrieval.save()
        else:
            logger.error('Errors were encountered and so retrieval {} has not '
                         'been marked as deleted. All possible files have been '
                         'deleted.'.format(args.retrieval_id))

    logger.debug('Completed delete_request.py for retrieval {}'.format(
        args.retrieval_id))


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
