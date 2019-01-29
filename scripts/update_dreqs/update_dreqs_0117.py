#!/usr/bin/env python
"""
update_dreqs_0117.py

Update the paths and sym links that changed as a result of the move of the
group workspaces to new storage.
"""
import argparse
import logging.config
import os
import re
import sys


import django
django.setup()

from pdata_app.models import DataFile, Settings
from pdata_app.utils.common import is_same_gws, construct_drs_path


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Update GWS')
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
    base_output_dir = Settings.get_solo().base_output_dir

    for data_file in DataFile.objects.filter(
            online=True,
            climate_model__short_name='HadGEM3-GC31-LM',
            experiment__short_name='highresSST-present',
            rip_code__in=['r1i2p1f1', 'r1i3p1f1'],
            variable_request__table_name='Amon',
            variable_request__cmor_name__in=['tas', 'pr', 'psl']
    ):
        gws_pattern = r'^/group_workspaces/jasmin2/primavera(\d)/(\S*)'
        gws = re.match(gws_pattern, data_file.directory)
        if not gws:
            logger.error('No GWS match for {}'.format(data_file.name))
            continue
        new_gws = '/gws/nopw/j04/primavera' + gws.group(1)
        new_dir = os.path.join(new_gws, gws.group(2))
        new_path = os.path.join(new_dir, data_file.name)
        if not os.path.exists(new_path):
            logger.error('Cannot find {}'.format(new_path))
            continue
        data_file.directory = new_dir
        data_file.save()

        if not is_same_gws(data_file.directory, base_output_dir):
            link_path = os.path.join(base_output_dir,
                                     construct_drs_path(data_file),
                                     data_file.name)
            # it's got to be a link but check anyway
            if os.path.islink(link_path):
                # os.remove(link_path)
                logger.debug("os.remove('{}')".format(link_path))
                # os.symlink(os.path.join(data_file.directory, data_file.name),
                #            link_path)
                logger.debug("os.symlink('{}', '{}')".format(os.path.join(data_file.directory, 
                                                                          data_file.name),
                                                             link_path))
            else:
                logger.error('Expected a link but found a file at {}'.
                             format(link_path))


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
