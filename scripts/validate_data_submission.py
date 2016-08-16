#!/usr/bin/env python2.7
"""
validate_data_submission.py

This script is run by users to validate submitted data files and to create a
data submission in the Data Management Tool.
"""
import argparse
import datetime
import logging
import os
import sys

import iris
import django
django.setup()
import pytz

from vocabs.vocabs import FREQUENCY_VALUES

__version__ = '0.1.0b'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


class FileValidationFails(Exception):
    def __init__(self, message=''):
        """
        :param str message: The error message text.
        """
        Exception.__init__(self)
        self.message = message


def list_files(directory, suffix='.nc'):
    """
    Return a list of all the files with the specified suffix in the submission
    directory structure and sub-directories.

    :param str directory: The root directory of the submission
    :param str suffix: The suffix of the files of interest
    :returns: A list of absolute filepaths
    """
    nc_files = []

    dir_files = os.listdir(directory)
    for filename in dir_files:
        file_path = os.path.join(directory, filename)
        if os.path.isdir(file_path):
            nc_files.extend(list_files(file_path))
        elif file_path.endswith(suffix):
            nc_files.append(file_path)

    return nc_files


def identify_and_validate(files, project):
    """
    Loop through a list of file names, identify each file's metadata and then
    validate it.

    clt_Amon_HadGEM2-ES_historical_r1i1p1_185912-188411.nc

    :param list files: The files to process
    :param str project: The name of the project
    :returns:
    """
    for filename in files:
        metadata = identify_filename_metadata(filename, project)
        metadata.update(identify_contents_metadata(filename))

        print '*** {}'.format(metadata)



def identify_filename_metadata(filename, project):
    """
    Identify all of the required metadata from the filename and file contents

    :param str filename: The file's complete path
    :param str project: the name of the project that the file is part of
    :returns: A dictionary containing the identified metadata
    """
    components = ['cmor_name', 'table', 'climate_model', 'experiment', 'rip_code',
        'date_string']

    basename = os.path.basename(filename)
    directory = os.path.dirname(filename)
    metadata = {'basename': basename, 'directory': directory,
                'project': project}
    # deduce as much as possible from the filename
    for cmpt_name, cmpt in zip(components, basename.rstrip('.nc').split('_')):
        if cmpt_name == 'date_string':
            start_date, end_date = cmpt.split('-')
            start_datetime = datetime.datetime(int(start_date[0:4]),
                int(start_date[5:6]), 1, 0, 0, 0, 0, pytz.UTC)
            end_datetime = datetime.datetime(int(end_date[0:4]),
                int(end_date[5:6]), 30, 23, 59, 59, 999999, pytz.UTC)
            metadata['start_date'] = start_datetime
            metadata['end_date'] = end_datetime
        else:
            metadata[cmpt_name] = cmpt

    metadata['filesize'] = os.path.getsize(filename)
    for freq in FREQUENCY_VALUES:
        if freq in metadata['table'].lower():
            metadata['frequency'] = freq
            break
    if 'frequency' not in metadata:
        # set a blank frequency if one hasn't been found
        metadata['frequency'] = ''

    return metadata


def identify_contents_metadata(filename):
    """
    Uses Iris to get additional metadata from the files contents

    :param str filename: The path of the file to work with
    :returns: A dictionary of the identified metadata
    """
    metadata = {}

    cubes = iris.load(filename)
    if len(cubes) != 1:
        msg = "Filename '{}' does not load to a single variable"
        logger.warning(msg)
        raise FileValidationFails(msg)
    else:
        cube = cubes[0]

    # This could be None if cube.var_name isn't defined
    metadata['var_name'] = cube.var_name
    metadata['units'] = str(cube.units)
    metadata['long_name'] = cube.long_name
    metadata['standard_name'] = cube.standard_name

    return metadata



def create_database_submission():
    """
    Create an entry in the database for this submission
    """
    pass


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Validate and create a '
        'PRIMAVERA data submission')
    parser.add_argument('directory', help="the submission's top-level directory")
    parser.add_argument('--project', help='the project that data is ultimately '
        'being submitted to (default: %(default)s)', default='CMIP6')
    parser.add_argument('--log-level', help='set logging level to one of '
        'debug, info, warn (the default), or error')
    parser.add_argument('--version', action='version',
        version='%(prog)s {}'.format(__version__))
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    logger.debug('Submission directory: %s', args.directory)
    logger.debug('Project: %s', args.project)

    data_files = list_files(args.directory)

    logger.debug('%s files identified', len(data_files))

    identify_and_validate(data_files, args.project)

    create_database_submission()


if __name__ == "__main__":
    cmd_args = parse_args()

    # set-up the logger
    console = logging.StreamHandler(stream=sys.stdout)
    fmtr = logging.Formatter(fmt=DEFAULT_LOG_FORMAT)
    console.setFormatter(fmtr)
    logger.addHandler(console)
    if cmd_args.log_level:
        try:
            logger.setLevel(getattr(logging, cmd_args.log_level.upper()))
        except AttributeError:
            logger.setLevel(logging.WARNING)
            logger.error('log-level must be one of: debug, info, warn or error')
            sys.exit(1)
    else:
        logger.setLevel(DEFAULT_LOG_LEVEL)

    # run the code
    main(cmd_args)
