#!/usr/bin/env python2.7
"""
delete_request.py

This script is run by the admin to delete previously retrieved data from the
file structure.
"""
import argparse
import logging.config
import os
import sys

import django
django.setup()
from django.utils import timezone

from pdata_app.models import RetrievalRequest
from pdata_app.utils.dbapi import match_one


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

# The possible values of mip_era at the top of the DRS structure. Directories
# above these values are not deleted.
MIP_ERAS = ('PRIMAVERA', 'CMIP6')


def delete_dir(directory):
    """
    Delete the directory specified and any empty parent directories until one
    of the MIP_ERAS values is found at the top of the DRS structure. It is
    assumed that the directory has already been confirmed as being empty.

    :param str directory: The directory to delete.
    """
    try:
        os.rmdir(directory)
    except OSError as exc:
        logger.error('Unable to delete directory {}. {}.'.format(directory,
                                                                 exc.strerror))
        return

    parent_dir = os.path.dirname(directory)
    if (not os.listdir(parent_dir) and
            not directory.endswith(MIP_ERAS)):
        # parent is empty so delete it
        delete_dir(parent_dir)


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
    logger.debug('Starting delete_request.py')

    # check retrieval
    retrieval = match_one(RetrievalRequest, id=args.retrieval_id)
    if not retrieval:
        logger.error('Unable to find retrieval id {}'.format(
            args.retrieval_id))
        sys.exit(1)

    if retrieval.date_deleted:
        logger.error('Retrieval {} was already deleted, at {}.'.
                     format(retrieval.id,
                            retrieval.date_complete.strftime('%Y-%m-%d %H:%M')))
        sys.exit(1)

    if not retrieval.data_finished:
        logger.error('Retrieval {} is not marked as finished.'.
                     format(retrieval.id))
        sys.exit(1)

    problems_encountered = False
    directories_found = []

    # delete each file
    for data_req in retrieval.data_request.all():
        for data_file in data_req.datafile_set.filter(online=True,
                                                      directory__isnull=False):
            try:
                os.remove(os.path.join(data_file.directory, data_file.name))
            except OSError as exc:
                logger.error(str(exc))
                problems_encountered = True
            else:
                if data_file.directory not in directories_found:
                    directories_found.append(data_file.directory)
                data_file.online = False
                data_file.directory = None
                data_file.save()

    # delete any empty directories
    for directory in directories_found:
        if not os.listdir(directory):
            delete_dir(directory)

    # set date_complete in the db
    if not problems_encountered:
        retrieval.date_deleted = timezone.now()
        retrieval.save()
    else:
        logger.error('Errors were encountered and so retrieval {} has not '
                     'been marked as deleted. All possible files have been '
                     'deleted.'.format(retrieval.id))

    logger.debug('Completed delete_request.py')


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
