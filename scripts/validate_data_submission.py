#!/usr/bin/env python2.7
"""
validate_data_submission.py

This script is run by users to validate submitted data files and to create a
data submission in the Data Management Tool.
"""
import argparse
import logging
import os
import random
import sys

import iris
from iris.time import PartialDateTime
import django
django.setup()

from vocabs.vocabs import FREQUENCY_VALUES

__version__ = '0.1.0b'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


class FileValidationError(Exception):
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
        metadata = identify_filename_metadata(filename)

        # TODO: what action should be taken if a file fails validation
        try:
            cube = load_cube(filename)
            metadata.update(identify_contents_metadata(cube))
            metadata['project'] = project
            validate_file_contents(cube, metadata)
        except FileValidationError:
            msg = 'File failed validation: {}'.format(filename)
            logger.warning(msg)

        print '*** {}'.format(metadata)


def identify_filename_metadata(filename):
    """
    Identify all of the required metadata from the filename and file contents

    :param str filename: The file's complete path
    :returns: A dictionary containing the identified metadata
    """
    components = ['cmor_name', 'table', 'climate_model', 'experiment', 'rip_code',
        'date_string']

    basename = os.path.basename(filename)
    directory = os.path.dirname(filename)
    metadata = {'basename': basename, 'directory': directory}
    # deduce as much as possible from the filename
    for cmpt_name, cmpt in zip(components, basename.rstrip('.nc').split('_')):
        if cmpt_name == 'date_string':
            start_date, end_date = cmpt.split('-')
            start_datetime = PartialDateTime(year=int(start_date[0:4]),
                month=int(start_date[4:6]))
            end_datetime = PartialDateTime(year=int(end_date[0:4]),
                month=int(end_date[4:6]))
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


def identify_contents_metadata(cube):
    """
    Uses Iris to get additional metadata from the files contents

    :param iris.cube.Cube cube: The loaded file to check
    :returns: A dictionary of the identified metadata
    """
    metadata = {}

    # This could be None if cube.var_name isn't defined
    metadata['var_name'] = cube.var_name
    metadata['units'] = str(cube.units)
    metadata['long_name'] = cube.long_name
    metadata['standard_name'] = cube.standard_name

    return metadata


def load_cube(filename):
    """
    Loads the specified file into a single Iris cube

    :param str filename: The path of the file to load
    :returns: An Iris cube containing the loaded file
    :raises FileValidationError: If the file generates more than a single cube
    """
    iris.FUTURE.netcdf_promote = True

    try:
        cubes = iris.load(filename)
    except Exception:
        msg = 'Unable to load cube: {}'.format(filename)
        logger.warning(msg)
        raise FileValidationError(msg)
    if len(cubes) != 1:
        msg = "Filename '{}' does not load to a single variable"
        logger.warning(msg)
        raise FileValidationError(msg)

    return cubes[0]


def validate_file_contents(cube, metadata):
    """
    Check whether the contents of the cube loaded from a file are valid

    :param iris.cube.Cube cube: The loaded file to check
    :param dict metadata: Metadata obtained from the file
    :returns: A boolean
    """
    _check_start_end_times(cube, metadata)
    _check_contiguity(cube, metadata)
    _check_data_point(cube, metadata)


def _check_start_end_times(cube, metadata):
    """
    Check whether the start and end dates match those in the metadata

    :param iris.cube.Cube cube: The loaded file to check
    :param dict metadata: Metadata obtained from the file
    :returns: True if the times match
    :raises FileValidationError: If the times don't match
    """
    file_start_date = metadata['start_date']
    file_end_date = metadata['end_date']

    time = cube.coord('time')
    data_start = time.units.num2date(time.points[0])
    data_end = time.units.num2date(time.points[-1])

    if not _compare_dates(file_start_date, data_start):
        msg = ('Start date in filename does not match the first time in the '
            'file ({}): {}'.format(str(data_start), metadata['basename']))
        logger.warning(msg)
        raise FileValidationError(msg)
    elif not _compare_dates(file_end_date, data_end):
        msg = ('End date in filename does not match the last time in the '
            'file ({}): {}'.format(str(data_end), metadata['basename']))
        logger.warning(msg)
        raise FileValidationError(msg)
    else:
        return True


def _compare_dates(date_1, date_2):
    """
    Compare the year and month of two datetime like objects

    :param date_1: The first date to compare
    :param date_2: The second date to compare
    :returns: True if the dates match
    """
    if date_1.year != date_2.year or date_1.month != date_2.month:
        return False
    else:
        return True


def _check_contiguity(cube, metadata):
    """
    Check whether the time coordinate is contiguous

    :param iris.cube.Cube cube: The loaded file to check
    :param dict metadata: Metadata obtained from the file
    :returns: True if the data is contiguous
    :raises FileValidationError: If the data isn't contiguous
    """
    time_coord = cube.coord('time')

    if not time_coord.is_contiguous():
        msg = ('The points in the time dimension in the file are not '
            'contiguous: {}'.format(metadata['basename']))
        logger.warning(msg)
        raise FileValidationError(msg)
    else:
        return True


def _check_data_point(cube, metadata):
    """
    Check whether a data point can be loaded

    :param iris.cube.Cube cube: The loaded file to check
    :param dict metadata: Metadata obtained from the file
    :returns: True if a data point was read without any exceptions being raised
    :raises FileValidationError: If there was a problem reading the data point
    """
    point_index = []

    for dim_length in cube.shape:
        point_index.append(int(random.random() * dim_length))

    point_index = tuple(point_index)

    try:
        data_point = cube.data[point_index]
    except Exception:
        msg = 'Unable to extract data point {} from file: {}'.format(
            point_index, metadata['basename'])
        logger.warning(msg)
        raise FileValidationError(msg)
    else:
        return True


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
