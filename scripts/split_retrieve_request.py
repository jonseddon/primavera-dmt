#!/usr/bin/env python2.7
"""
split_retrieve_request.py

This script is run by a cron job to separate retrieve_requests into ones
on elastic tape and ones in MASS.
"""
import argparse
import datetime
import glob
from itertools import chain
import logging.config
import os
import shutil
import subprocess
import sys

import cf_units

import django
django.setup()
from django.contrib.auth.models import User
from django.utils import timezone

from pdata_app.models import Settings, RetrievalRequest, DataFile, EmailQueue
from pdata_app.utils.common import (md5, sha256, adler32, construct_drs_path,
                                    get_temp_filename)
from pdata_app.utils.dbapi import match_one


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def _email_user_split(orig_retrieval, new_retrieval):
    """
    Send an email to request's creator advising them that their request's been
    split.

    :param pdata_app.models.RetrievalRequest orig_retrieval: the retrieval object
    """
    contact_user_id = Settings.get_solo().contact_user_id
    contact_user = User.objects.get(username=contact_user_id)

    msg = (
        'Dear {},\n'
        '\n'
        'Thank you for your recent data retrieval request.\n'
        '\n'
        'Due to temporary restrictions on the tape servers at JASMIN '
        'Your request has been split into two separate requests, so that the '
        'Met Office data is in a request of its own.\n'
        '\n'
        'Therefore your data will not be fully restored until you have '
        'received emails to say that requests {} and {} have both been '
        'restored.\n'
        '\n'
        'Thanks,\n'
        '\n'
        '{}'.format(orig_retrieval.requester.first_name, orig_retrieval.id,
                    new_retrieval.id, contact_user.first_name)
    )

    _email = EmailQueue.objects.create(
        recipient=orig_retrieval.requester,
        subject=('[PRIMAVERA_DMT] Retrieval Request {}'.
                 format(orig_retrieval.id)),
        message=msg
    )


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Perform a PRIMAVERA '
                                                 'retrieval request.')
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
    logger.debug('Starting split_retrieve_request.py')

    ret_reqs = RetrievalRequest.objecs.filter(date_complete__isnull=True,
                                              date_deleted__isnull=True)

    for ret_req in ret_reqs:
        # find retrieval requests with both MASS and non-MASS data
        if (ret_req.data_request.
                    filter(institute__short_name='MOHC').count() and
             ret_req.data_request.
                    exclude(institute__short_name='MOHC').count()):
            logger.debug('Splitting retrieve request {}'.format(ret_req.id))
            original_id = ret_req.id
            # copy the retrieval request
            ret_req.pk = None
            ret_req.save()

            # get the original retrieval request
            orig_req = RetrievalRequest.object.get(id=original_id)

            # add the MASS data requests to the new request
            for data_req in orig_req.data_request.filter(
                    institute__short_name='MOHC'):
                ret_req.data_request.add(data_req)
                ret_req.save()

            # remove the MASS data requests from the old request
            for data_req in ret_req.data_request.all():
                orig_req.data_request.remove(data_req)
                orig_req.save()

            _email_user_split(orig_req, ret_req)

    logger.debug('Completed split_retrieve_request.py')


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
