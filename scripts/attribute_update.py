#!/usr/bin/env python
"""
attribute_update.py

This script is called by the pre-proc tool to update metadata in the
Data Management Tool.
"""
from abc import ABCMeta, abstractmethod
import argparse
import json
import logging
import os
import sys

import django
django.setup()

from pdata_site.settings import DATABASES

if DATABASES['default']['HOST'] != '':
    raise ValueError('Do nut run attribute_update.py on the live database!!!')

from pdata_app.models import ClimateModel, DataFile
from pdata_app.utils.common import (construct_drs_path, construct_filename,
                                    get_gws)

__version__ = '0.1.0b'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


class DmtUpdate(object, metaclass=ABCMeta):
    """
    Abstract base class for all DMT metadata updates.
    """
    def __init__(self, datafile, new_value):
        """
        Initialise the class

        :param pdata_apps.models.DataFile datafile: the file to update
        :param str new_value: the new value to apply
        """
        self.datafile = datafile
        self.new_value = new_value
        self.new_filename = None
        self.new_directory = None

    def update(self):
        """
        Update everything.
        """
        self._update_attribute()
        self.datafile.refresh_from_db()
        self._update_filename()
        self._update_directory()
        self._report_results()

    @abstractmethod
    def _update_attribute(self):
        """
        Update the attribute in the database.
        """
        pass

    def _update_filename(self):
        """
        Update the file's name in the database.
        """
        self.new_filename = construct_filename(self.datafile)
        self.datafile.name = self.new_filename
        self.datafile.save()

    def _update_directory(self):
        """
        Update the file's directory.
        """
        self.new_directory = os.path.join(get_gws(self.datafile.directory),
                                          construct_drs_path(self.datafile))
        self.datafile.directory = self.new_directory
        self.datafile.save()

    def _report_results(self):
        """
        Report the new filename and directory on stdout
        """
        print(json.dumps({'filename': self.new_filename,
                          'directory': self.new_directory}))


class SourceIdUpdate(DmtUpdate):
    """
    Update a DataFile's source_id (climate model).
    """
    def __init__(self, datafile, new_value):
        """
        Initialise the class
        """
        super().__init__(datafile, new_value)

    def _update_attribute(self):
        """
        Update the source_id
        """
        new_source_id = ClimateModel.objects.get(short_name=self.new_value)
        self.datafile.climate_model = new_source_id
        self.datafile.save()


class VariantLabelUpdate(DmtUpdate):
    """
    Update a DataFile's variant_label (rip_code).
    """
    def __init__(self, datafile, new_value):
        """
        Initialise the class
        """
        super().__init__(datafile, new_value)

    def _update_attribute(self):
        """
        Update the variant label
        """
        self.datafile.rip_code = self.new_value
        self.datafile.save()


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Update PRIMAVERA metadata')
    parser.add_argument('file_path', help='the full path of the file to update')
    parser.add_argument('attribute_name', help='the name of the attribute to '
                                               'update')
    parser.add_argument('new_value', help='the new value for the attribute')
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
    """
    file_dir = os.path.dirname(args.file_path)
    file_name = os.path.basename(args.file_path)
    try:
        data_file = DataFile.objects.get(directory=file_dir, name=file_name)
    except django.core.exceptions.ObjectDoesNotExist:
        msg = 'Unable to find data file with name {}'.format(file_name)
        logger.error(msg)
        sys.exit(1)
    except django.core.exceptions.MultipleObjectsReturned:
        msg = 'Found multiple data files with name {}'.format(file_name)
        logger.error(msg)
        sys.exit(1)

    updaters = {
        'variant_label': VariantLabelUpdate,
        'source_id': SourceIdUpdate
    }

    updater_class = updaters.get(args.attribute_name)
    if not updater_class:
        msg = 'Unable to find updater for attribute name {}'.format(
            args.attribute_name)
        logger.error(msg)
        sys.exit(1)

    updater = updater_class(data_file, args.new_value)
    updater.update()


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
