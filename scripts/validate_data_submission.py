#!/usr/bin/env python2.7
"""
validate_data_submission.py

This script is run by users to validate submitted data files and to create a
data submission in the Data Management Tool.
"""
import argparse
import datetime
import itertools
import logging
from multiprocessing import Process, Manager
import os
import random
import sys

import iris
from iris.time import PartialDateTime
import django
django.setup()
import pytz

from pdata_app.models import (Project, ClimateModel, Experiment, DataSubmission,
    DataFile, Variable, Checksum)
from pdata_app.utils.dbapi import get_or_create, match_one
from pdata_app.utils.common import adler32
from vocabs.vocabs import FREQUENCY_VALUES, STATUS_VALUES, CHECKSUM_TYPES

__version__ = '0.1.0b'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


class FileValidationError(Exception):
    def __init__(self, message=''):
        """
        An exception to indicate that a data file has failed validation.

        :param str message: The error message text.
        """
        Exception.__init__(self)
        self.message = message


class SubmissionError(Exception):
    def __init__(self, message=''):
        """
        An exception to indicate that there has been an error that means that
        the data submission cannot continue.

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


def identify_and_validate(filenames, project, num_processes):
    """
    Loop through a list of file names, identify each file's metadata and then
    validate it. The looping is done in parallel using the multiprocessing
    library module.

    clt_Amon_HadGEM2-ES_historical_r1i1p1_185912-188411.nc

    :param list filenames: The files to process
    :param str project: The name of the project
    :param int num_processes: The number of parallel processes to use
    :returns: A list containing the metadata dictionary generated for each file
    :rtype: multiprocessing.Manager.list
    """
    jobs = []
    manager = Manager()
    params = manager.Queue(num_processes)
    result_list = manager.list()
    error_event = manager.Event()
    for i in range(num_processes):
        p = Process(target=identify_and_validate_file, args=(params,
            result_list, error_event))
        jobs.append(p)
        p.start()

    func_input_pair = zip(filenames, (project,) * len(filenames))
    blank_pair = (None, None)

    iters = itertools.chain(func_input_pair, (blank_pair,) * num_processes)
    for item in iters:
        params.put(item)

    for j in jobs:
        j.join()

    if error_event.set():
        raise SubmissionError()

    return result_list


def identify_and_validate_file(params, output, error_event):
    """
    Identify `filename`'s metadata and then validate the file. The function
    continues getting items to process from the parameter queue until a None
    is received.

    :param multiprocessing.Manager.Queue params: A queue, with each item being a
        tuple of the filename to load and the name of the project
    :param multiprocessing.Manager.list output: A list containing the output
        metadata dictionaries for each file
    :param multiprocessing.Manager.Event error_event: If set then a catastrophic
        error has occurred in another process and processing should end
    """
    while True:
        if error_event.is_set():
            return

        filename, project = params.get()

        if filename is None:
            return

        try:
            metadata = identify_filename_metadata(filename)
            metadata['project'] = project

            verify_fk_relationships(metadata)

            cube = load_cube(filename)
            metadata.update(identify_contents_metadata(cube))

            validate_file_contents(cube, metadata)
        except SubmissionError:
            msg = ('A serious file error means the submission cannot continue: '
                  '{}'.format(filename))
            logger.error(msg)
            error_event.set()
            return
        except FileValidationError:
            msg = 'File failed validation: {}'.format(filename)
            logger.warning(msg)
        else:
            output.append(metadata)


def identify_filename_metadata(filename):
    """
    Identify all of the required metadata from the filename and file contents

    :param str filename: The file's complete path
    :returns: A dictionary containing the identified metadata
    :raises FileValidationError: If unable to parse the date string
    """
    components = ['cmor_name', 'table', 'climate_model', 'experiment',
        'rip_code', 'date_string']

    basename = os.path.basename(filename)
    directory = os.path.dirname(filename)
    metadata = {'basename': basename, 'directory': directory}

    # split the filename into sections
    filename_sects = basename.rstrip('.nc').split('_')

    # but if experiment present_day was in the filename, join these sections
    # back together
    if filename_sects[3] == 'present' and filename_sects[4] == 'day':
        filename_sects[3] += '_' + filename_sects.pop(4)

    # deduce as much as possible from the filename
    for cmpt_name, cmpt in zip(components, filename_sects):
        if cmpt_name == 'date_string':
            start_date, end_date = cmpt.split('-')
            try:
                metadata['start_date'] = _make_partial_date_time(start_date)
                metadata['end_date'] = _make_partial_date_time(end_date)
            except ValueError:
                msg = 'Unknown date format in filename: {}'.format(filename)
                logger.debug(msg)
                raise FileValidationError(msg)
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


def verify_fk_relationships(metadata):
    """
    Check that entries already exist in the database for `Project`,
    `ClimateModel` and `Experiment`.

    :param dict metadata: Metadata identified for this file.
    :returns: True if objects exist.
    :raises SubmissionError: If there are no existing entries in the
        database for `Project`, `ClimateModel` or `Experiment`.
    """
    checks = [
        (Project, 'project'),
        (ClimateModel, 'climate_model'),
        (Experiment, 'experiment')]

    for check_type, check_str in checks:
        results = match_one(check_type, short_name=metadata[check_str])
        if not results:
            msg = ('There is no existing entry for {}: {} in file: {}'.format(
                check_str, metadata[check_str], metadata['basename']))
            logger.warning(msg)
            raise SubmissionError(msg)

    return True


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
        msg = 'Unable to load data from file: {}'.format(filename)
        logger.debug(msg)
        raise FileValidationError(msg)
    if len(cubes) != 1:
        msg = "Filename '{}' does not load to a single variable"
        logger.debug(msg)
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


def create_database_submission(validated_metadata, directory):
    """
    Create an entry in the database for this submission

    :param multiprocessing.Manager.list validated_metadata: A list containing
        the metadata dictionary generated for each file
    :param str directory: The directory that the files were uploaded to
    :returns:
    """
    data_sub = get_or_create(DataSubmission, status=STATUS_VALUES['ARRIVED'],
        incoming_directory=directory, directory=directory)

    for data_file in validated_metadata:
        create_database_file_object(data_file, data_sub)

    data_sub.status = STATUS_VALUES['VALIDATED']
    data_sub.save()


def create_database_file_object(metadata, data_submission):
    """
    Create a database entry for a data file

    :param dict metadata: This file's metadata
    :param pdata_app.models.DataSubmission data_submission: The parent data
        submission
    :returns:
    """
    foreign_key_types = [
        (Project, 'project'),
        (ClimateModel, 'climate_model'),
        (Experiment, 'experiment')]

    metadata_objs = {}

    for object_type, object_str in foreign_key_types:
        result = match_one(object_type, short_name=metadata[object_str])
        if result:
            metadata_objs[object_str] = result
        else:
            msg = ("No {} '{}' found for file: {}. Please create this object "
                "and resubmit.".format(object_str.replace('_', ' '),
                metadata['project'], metadata['basename']))
            logger.error(msg)
            raise SubmissionError(msg)

    variable = get_or_create(Variable, var_id=metadata['var_name'],
        units=metadata['units'], long_name=metadata['long_name'],
        standard_name=metadata['standard_name'])

    # create a data file. If the file already exists in the database with
    # identical metadata then nothing happens. If the file exists but with
    # slighly different metadata then django.db.utils.IntegrityError is
    # raised which may or may not be what we want to happen
    # TODO: sort the above
    try:
        data_file = get_or_create(DataFile, name=metadata['basename'],
            incoming_directory=metadata['directory'],
            directory=metadata['directory'], size=metadata['filesize'],
            project=metadata_objs['project'],
            climate_model=metadata_objs['climate_model'],
            experiment=metadata_objs['experiment'],
            variable=variable, frequency=metadata['frequency'],
            rip_code=metadata['rip_code'],
            start_time=_pdt_to_datetime(metadata['start_date']),
            end_time=_pdt_to_datetime(metadata['end_date'], start_of_period=False),
            data_submission=data_submission, online=True)
    except django.db.utils.IntegrityError:
        msg = ('Unable to submit file because it already exists in the '
            'database with different metadata: {} ({}) .'.format(
            metadata['basename'], metadata['directory']))
        logger.error(msg)
        raise SubmissionError(msg)

    checksum_value = adler32(os.path.join(metadata['directory'],metadata['basename'] ))
    checksum = get_or_create(Checksum, data_file=data_file,
        checksum_value=checksum_value, checksum_type=CHECKSUM_TYPES['ADLER32'])


def _pdt_to_datetime(pdt, start_of_period=True):
    """
    Convert an Iris PartialDateTime object into a Python datetime object. If
    the day of the month is not specified in `pdt` then it is set as 1 in the
    output unless `start_of_period` isn't True, when 30 is used as the day of the
    month (because a 360 day calendar is assumed).

    :param iris.time.PartialDateTime pdt: The partial date time to convert
    :param bool start_of_period: If true and no day is specified then day is
        set to 1, otherwise day is set to 30.
    :returns: A Python datetime representation of `pdt`
    """
    datetime_attrs = {}

    compulsory_attrs = ['year', 'month']

    for attr in compulsory_attrs:
        attr_value = getattr(pdt, attr)
        if attr_value:
            datetime_attrs[attr] = attr_value
        else:
            msg = '{} must be defined in: {}'.format(attr, pdt)
            logger.error(msg)
            raise ValueError(msg)

    if pdt.day:
        datetime_attrs['day'] = pdt.day
    else:
        if start_of_period:
            datetime_attrs['day'] = 1
        else:
            datetime_attrs['day'] = 30

    optional_attrs = ['hour', 'minute', 'second', 'microsecond']
    for attr in optional_attrs:
        attr_value = getattr(pdt, attr)
        if attr_value:
            datetime_attrs[attr] = attr_value

    datetime_attrs['tzinfo'] = pytz.UTC

    return datetime.datetime(**datetime_attrs)


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

    if file_start_date != data_start:
        msg = ('Start date in filename does not match the first time in the '
            'file ({}): {}'.format(str(data_start), metadata['basename']))
        logger.debug(msg)
        raise FileValidationError(msg)
    elif file_end_date != data_end:
        msg = ('End date in filename does not match the last time in the '
            'file ({}): {}'.format(str(data_end), metadata['basename']))
        logger.debug(msg)
        raise FileValidationError(msg)
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
        logger.debug(msg)
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
        logger.debug(msg)
        raise FileValidationError(msg)
    else:
        return True


def _make_partial_date_time(date_string):
    """
    Convert the fields in `date_string` into a PartialDateTime object. Formats
    that are known about are:

    YYYMM
    YYYYMMDD

    :param str date_string: The date string to process
    :returns: An Iris PartialDateTime object containing as much information as
        could be deduced from date_string
    :rtype: iris.time.PartialDateTime
    :raises ValueError: If the string is not in a known format.
    """
    if len(date_string) == 6:
        pdt_str = PartialDateTime(year=int(date_string[0:4]),
            month=int(date_string[4:6]))
    elif len(date_string) == 8:
        pdt_str = PartialDateTime(year=int(date_string[0:4]),
            month=int(date_string[4:6]), day=int(date_string[6:8]))
    else:
        raise ValueError('Unknown date string format')

    return pdt_str


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Validate and create a '
        'PRIMAVERA data submission')
    parser.add_argument('directory', help="the submission's top-level directory")
    parser.add_argument('-j', '--project', help='the project that data is ultimately '
        'being submitted to (default: %(default)s)', default='CMIP6')
    parser.add_argument('-l', '--log-level', help='set logging level to one of '
        'debug, info, warn (the default), or error')
    parser.add_argument('-p', '--processes', help='the number of parallel processes '
        'to use (default: %(default)s)', default=8, type=int)
    parser.add_argument('-r', '--relaxed', help='create a submission from '
        'validated files, ignoring failed files (default behaviour is to only '
        'create a submission when all files pass validation)', action='store_true')
    parser.add_argument('-v', '--validate_only', help='only validate the input, '
        'do not create a data submission', action='store_true')
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
    logger.debug('Processes requested: %s', args.processes)

    data_files = list_files(args.directory)

    logger.debug('%s files identified', len(data_files))

    try:
        validated_metadata = identify_and_validate(data_files, args.project,
            args.processes)

        logger.debug('%s files validated successfully', len(validated_metadata))

        if args.validate_only:
            logger.debug('Data submission not run (-v option specified)')
            logger.debug('Processing complete')
            sys.exit(0)

        if not args.relaxed and len(validated_metadata) != len(data_files):
            msg = ('Not all files passed validation. Please fix these errors '
                'and then re-run this script to create a data submission.')
            logger.error(msg)
            raise SubmissionError(msg)

        create_database_submission(validated_metadata, args.directory)

        logger.debug('%s files submitted successfully',
            match_one(DataSubmission, incoming_directory=args.directory).get_data_files().count())
    except SubmissionError:
        sys.exit(1)

    logger.debug('Processing complete')


if __name__ == "__main__":
    cmd_args = parse_args()

    # Disable propagation and discard any existing handlers.
    logger.propagate = False
    if len(logger.handlers):
        logger.handlers = []

    # set-up the logger
    console = logging.StreamHandler(stream=sys.stdout)
    fmtr = logging.Formatter(fmt=DEFAULT_LOG_FORMAT)
    if cmd_args.log_level:
        try:
            logger.setLevel(getattr(logging, cmd_args.log_level.upper()))
        except AttributeError:
            logger.setLevel(logging.WARNING)
            logger.error('log-level must be one of: debug, info, warn or error')
            sys.exit(1)
    else:
        logger.setLevel(DEFAULT_LOG_LEVEL)
    console.setFormatter(fmtr)
    logger.addHandler(console)

    # run the code
    main(cmd_args)
