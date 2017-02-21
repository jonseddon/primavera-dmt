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
import re
import shutil
import sys

from primavera_val import (identify_filename_metadata, validate_file_contents,
                           identify_contents_metadata, load_cube)

import django
django.setup()

from pdata_app.models import (Project, ClimateModel, Experiment, DataSubmission,
    DataFile, VariableRequest, DataRequest, Checksum, Settings, Institute,
    ActivityId, EmailQueue)
from pdata_app.utils.dbapi import get_or_create, match_one
from pdata_app.utils.common import adler32, list_files, pdt2num
from vocabs.vocabs import STATUS_VALUES, CHECKSUM_TYPES

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


def identify_and_validate(filenames, project, num_processes, file_format):
    """
    Loop through a list of file names, identify each file's metadata and then
    validate it. The looping is done in parallel using the multiprocessing
    library module.

    clt_Amon_HadGEM2-ES_historical_r1i1p1_185912-188411.nc

    :param list filenames: The files to process
    :param str project: The name of the project
    :param int num_processes: The number of parallel processes to use
    :param str file_format: The CMOR version of the netCDF files, one out of-
        CMIP5 or CMIP6
    :returns: A list containing the metadata dictionary generated for each file
    :rtype: multiprocessing.Manager.list
    """
    jobs = []
    manager = Manager()
    params = manager.Queue()
    result_list = manager.list()
    error_event = manager.Event()
    for i in range(num_processes):
        p = Process(target=identify_and_validate_file, args=(params,
            result_list, error_event))
        jobs.append(p)
        p.start()

    func_input_pair = zip(filenames,
                          (project,) * len(filenames),
                          (file_format,) * len(filenames))
    blank_pair = (None, None, None)

    iters = itertools.chain(func_input_pair, (blank_pair,) * num_processes)
    for item in iters:
        params.put(item)

    for j in jobs:
        j.join()

    if error_event.is_set():
        raise SubmissionError()

    return result_list


def identify_and_validate_file(params, output, error_event):
    """
    Identify `filename`'s metadata and then validate the file. The function
    continues getting items to process from the parameter queue until a None
    is received.

    :param multiprocessing.Manager.Queue params: A queue, with each item being a
        tuple of the filename to load, the name of the project and the netCDF
        file CMOR version
    :param multiprocessing.Manager.list output: A list containing the output
        metadata dictionaries for each file
    :param multiprocessing.Manager.Event error_event: If set then a catastrophic
        error has occurred in another process and processing should end
    """
    while True:
        if error_event.is_set():
            return

        filename, project, file_format = params.get()

        if filename is None:
            return

        try:
            metadata = identify_filename_metadata(filename, file_format)
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
        except FileValidationError:
            msg = 'File failed validation: {}'.format(filename)
            logger.warning(msg)
        else:
            output.append(metadata)




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
            msg = ('There is no existing database entry for {}: {} in file: {}'.
                format(check_str, metadata[check_str], metadata['basename']))
            logger.warning(msg)
            raise SubmissionError(msg)

    return True


def update_database_submission(validated_metadata, data_sub):
    """
    Create entries in the database for the files in this submission.

    :param multiprocessing.Manager.list validated_metadata: A list containing
        the metadata dictionary generated for each file
    :param pdata_app.models.DataSubmission data_sub: The data submission object
        to update.
    :returns:
    """
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
        (Experiment, 'experiment'),
        (Institute, 'institute'),
        (ActivityId, 'activity_id')]

    metadata_objs = {}

    # get values for each of the foreign key types
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

    # find the variable request
    var_match = match_one(VariableRequest, cmor_name=metadata['var_name'],
        table_name=metadata['table'])
    if var_match:
        variable = var_match
    else:
        msg = ('No variable request found for file: {}. Please create a '
            'variable request and resubmit.'.format(metadata['basename']))
        logger.error(msg)
        raise SubmissionError(msg)

    # find the data request
    dreq_match = match_one(DataRequest, project=metadata_objs['project'],
                           institute=metadata_objs['institute'],
                           climate_model=metadata_objs['climate_model'],
                           experiment=metadata_objs['experiment'],
                           variable_request=variable)
    if dreq_match:
        data_request = dreq_match
    else:
        msg = ('No data request found for file: {}. Please create a '
            'data request and resubmit.'.format(metadata['basename']))
        logger.error(msg)
        raise SubmissionError(msg)

    time_units = Settings.get_solo().standard_time_units

    # find the version number from the date in the submission directory path
    date_string = re.search(r'(?<=/incoming/)(\d{8})', metadata['directory'])
    if date_string:
        date_string = date_string.group(0)
        version_string = 'v' + date_string
    else:
        today = datetime.datetime.utcnow()
        version_string = today.strftime('v%Y%m%d')

    # create a data file. If the file already exists in the database with
    # identical metadata then nothing happens. If the file exists but with
    # slightly different metadata then django.db.utils.IntegrityError is
    # raised
    try:
        data_file = DataFile.objects.create(name=metadata['basename'],
            incoming_directory=metadata['directory'],
            directory=metadata['directory'], size=metadata['filesize'],
            project=metadata_objs['project'],
            institute=metadata_objs['institute'],
            climate_model=metadata_objs['climate_model'],
            activity_id=metadata_objs['activity_id'],
            experiment=metadata_objs['experiment'],
            variable_request=variable, data_request=data_request,
            frequency=metadata['frequency'], rip_code=metadata['rip_code'],
            start_time=pdt2num(metadata['start_date'], time_units,
                                metadata['calendar']),
            end_time=pdt2num(metadata['end_date'], time_units,
                              metadata['calendar'], start_of_period=False),
            time_units=time_units, calendar=metadata['calendar'],
            version=version_string,
            data_submission=data_submission, online=True,
            grid=metadata['grid'] if 'grid' in metadata else None
        )
    except django.db.utils.IntegrityError as exc:
        msg = ('Unable to submit file {}: {}'.format(metadata['basename'],
                                                       exc.message))
        logger.error(msg)
        raise SubmissionError(msg)

    checksum_value = adler32(os.path.join(metadata['directory'],
                                          metadata['basename']))
    if checksum_value:
        checksum = get_or_create(Checksum, data_file=data_file,
                                 checksum_value=checksum_value,
                                 checksum_type=CHECKSUM_TYPES['ADLER32'])
    else:
        msg = ('Unable to calculate checksum for file: {}'.
               format(metadata['basename']))
        logger.warning(msg)


def move_rejected_files(submission_dir):
    """
    Move the entire submission to a rejected directory two levels up from the
    submission directory.

    :param str submission_dir:
    :returns: The path to the submission after the function has run.
    """
    rejected_dir = os.path.normpath(os.path.join(submission_dir, '..',
                                                 '..', 'rejected'))
    try:
        if not os.path.exists(rejected_dir):
            os.mkdir(rejected_dir)

        shutil.move(submission_dir, rejected_dir)
    except (IOError, OSError):
        msg = ("Unable to move the directory. Leaving it in it's current "
               "location")
        logger.error(msg)
        return submission_dir

    new_rejected_dir = os.path.join(rejected_dir,
        os.path.basename(os.path.abspath(submission_dir)))

    msg = 'Data submission moved to {}'.format(new_rejected_dir)
    logger.error(msg)

    return rejected_dir


def send_rejection_email(data_sub, rejection_dir):
    """
    Send an email to the submission's creator warning them of validation
    failure.

    :param pdata_app.models.DataSubmission data_sub:
    :param str rejection_dir:
    """
    # TODO consider how much information to include.
    # Can it include enough to allow users to identify why it failed and what
    # they need to do to correct the data. If it's due to a missing data
    # request then the data request list needs to be updated and it's not a
    # user problem.
    msg = ('Dear {first_name} {surname},\n'
           '\n'
           'Your data submission in {incoming_dir} has failed validation and '
           'has been moved to {rejected_dir}.')



def set_status_rejected(data_sub, rejected_dir):
    """
    Set the data submission's status to be rejected and update the path to
    point to where the data now lives.

    :param pdata_app.models.DataSubmission data_sub: The data submission object.
    """
    data_sub.status = STATUS_VALUES['REJECTED']
    data_sub.directory = rejected_dir
    data_sub.save()


def _get_submission_object(submission_dir):
    """
    :param str submission_dir: The path of the submission's top level
    directory.
    :returns: The object corresponding to the submission.
    :rtype: pdata_app.models.DataSubmission
    """
    try:
        data_sub = DataSubmission.objects.get(incoming_directory=submission_dir)
    except django.core.exceptions.MultipleObjectsReturned:
        msg = 'Multiple DataSubmissions found for directory: {}'.format(
            submission_dir)
        logger.error(msg)
        raise SubmissionError(msg)
    except django.core.exceptions.ObjectDoesNotExist:
        msg = ('No DataSubmissions have been found in the database for '
               'directory: {}. Please create a submission through the web '
               'interface.'.format(submission_dir))
        logger.error(msg)
        raise SubmissionError(msg)

    return data_sub




def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Validate and create a '
        'PRIMAVERA data submission')
    parser.add_argument('directory', help="the submission's top-level "
                                          "directory")
    parser.add_argument('-j', '--project', help='the project that data is '
                                                'ultimately being submitted to '
                                                '(default: %(default)s)',
                        default='CMIP6')
    parser.add_argument('-f', '--file-format', help='the CMOR version of the '
                                                    'input netCDF files being '
                                                    'submitted (CMIP5 or CMIP6)'
                                                    ' (default: %(default)s)',
                        default='CMIP6')
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
    submission_dir = os.path.normpath(args.directory)
    logger.debug('Submission directory: %s', submission_dir)
    logger.debug('Project: %s', args.project)
    logger.debug('Processes requested: %s', args.processes)

    data_files = list_files(submission_dir)

    logger.debug('%s files identified', len(data_files))

    try:
        if not args.validate_only:
            data_sub = _get_submission_object(submission_dir)
            if data_sub.status != 'ARRIVED':
                msg = "The submission's status is not ARRIVED."
                logger.error(msg)
                raise SubmissionError(msg)

        validated_metadata = identify_and_validate(data_files, args.project,
            args.processes, args.file_format)

        logger.debug('%s files validated successfully', len(validated_metadata))

        if args.validate_only:
            logger.debug('Data submission not run (-v option specified)')
            logger.debug('Processing complete')
            sys.exit(0)

        if not args.relaxed and len(validated_metadata) != len(data_files):
            rejected_dir = move_rejected_files(submission_dir)
            send_rejection_email(data_sub, rejected_dir)
            set_status_rejected(data_sub, rejected_dir)
            msg = ('Not all files passed validation. Please fix these errors '
                'and then re-run this script.')
            logger.error(msg)
            raise SubmissionError(msg)

        update_database_submission(validated_metadata, data_sub)

        logger.debug('%s files submitted successfully',
            match_one(DataSubmission, incoming_directory=submission_dir).get_data_files().count())
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
