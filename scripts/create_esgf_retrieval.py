#!/usr/bin/env python
"""
create_esgf_retrieval.py

This script is used to create a retrieval request from the
`rose_task_names_new.json` file generated by `make_rose_task_names.py`.
"""
import argparse
import datetime
import json
import logging.config
import sys

import django
django.setup()

from django.contrib.auth.models import User
from pdata_app.models import RetrievalRequest, Settings
from pdata_app.utils.esgf_utils import add_data_request, parse_rose_stream_name

__version__ = '0.1.0b'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)



def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(
        description='Create a retrieval request from a JSON list of the Rose '
                    'task names that should be submitted to CREPP.'
    )
    parser.add_argument('json_file', help='the path to the JSON file to read')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
                                                  'debug, info, warn (the '
                                                  'default), or error')
    parser.add_argument('--version', action='version',
                        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point

    Task names in the JSON file are in the form:

    <climate-model>_<experiment>_<variant-label>_<table>_<variable>

    e.g.:

    HadGEM3-GC31-LM_highresSST-present_r1i1p1f1_Amon_psl
    """
    with open(args.json_file) as fh:
        task_names = json.load(fh)

    logger.debug('{} task names loaded'.format(len(task_names)))

    system_user = User.objects.get(username=Settings.get_solo().contact_user_id)
    ret_req = RetrievalRequest.objects.create(requester=system_user,
                                         start_year=1900, end_year=2100)
    time_zone = datetime.timezone(datetime.timedelta())
    ret_req.date_created = datetime.datetime(2000, 1, 1, 0, 0, tzinfo=time_zone)
    ret_req.save()

    for task_name in task_names:
        task_cmpts = parse_rose_stream_name(task_name)
        add_data_request(task_cmpts, debug_req_found=False)
        ret_req.data_request.add(task_cmpts['data_req'])

    logger.debug('Request id {} created'.format(ret_req.id))


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
