#!/usr/bin/env python2.7
"""
retrieve_request.py

This script is run by the admin to perform a retrieval request.
"""
import argparse
import datetime
import glob
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
from pdata_app.utils.common import md5, sha256, adler32, check_same_gws
from pdata_app.utils.dbapi import match_one


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

# The top-level directory to initially restore files to
BASE_RETRIEVAL_DIR = '/group_workspaces/jasmin2/primavera5/.et_retrievals'
# The top-level directory to write output data to
BASE_OUTPUT_DIR = Settings.get_solo().base_output_dir
# The name of the directory to store et_get.py log files in
LOG_FILE_DIR = '/group_workspaces/jasmin2/primavera5/.et_logs/'
# The prefix to use on et_get.py log files
LOG_PREFIX = 'et_get'
# The number of processes that et_get.py should use.
# Between 5 and 10 are recommended
MAX_ET_GET_PROC = 5


class ChecksumError(Exception):
    def __init__(self, message=''):
        """
        An exception to indicate that a data file's checksum does not match
        the value recorded in the database.

        :param str message: The error message text.
        """
        Exception.__init__(self)
        self.message = message


def get_tape_url(tape_url, retrieval):
    """
    Get all of the data from `tape_url`.

    :param str tape_url: The URL of the tape data to fetch.
    :param pdata_app.models.RetrievalRequest retrieval: The retrieval object
    """
    if tape_url.startswith('et:'):
        get_et_url(tape_url)
    elif tape_url.startswith('moose:'):
        get_moose_url(tape_url, retrieval)
    else:
        msg = ('Tape url {} is not a currently supported type of tape.'.
               format(tape_url))
        logger.error(msg)
        raise NotImplementedError(msg)


def get_et_url(tape_url):
    """
    Get all of the data from `tape_url`, which is already known to be an ET url.

    :param str tape_url: The url to fetch
    """
    logger.debug('Starting restoring {}'.format(tape_url))

    batch_id = tape_url.split(':')[1]

    retrieval_dir = _make_tape_url_dir(tape_url, skip_creation=True)
    if os.path.exists(retrieval_dir):
        msg = ('Elastic tape retrieval destination directory {} already '
               'exists. Please delete this directory to restore from tape '
               'again, or run this script with the -n option to extract files '
               'from this existing directory.'.format(retrieval_dir))
        logger.error(msg)
        sys.exit(1)
    else:
        retrieval_dir = _make_tape_url_dir(tape_url)

    logger.debug('Restoring to {}'.format(retrieval_dir))

    cmd = 'et_get.py -v -l {} -b {} -r {} -t {}'.format(
        _make_logfile_name(LOG_FILE_DIR), batch_id, retrieval_dir,
        MAX_ET_GET_PROC)

    logger.debug('et_get.py command is:\n{}'.format(cmd))

    try:
        cmd_out = _run_command(cmd)
        pass
    except RuntimeError as exc:
        logger.error('et_get.py command for batch id {} failed\n{}'.
                     format(batch_id, exc.message))
        sys.exit(1)

    logger.debug('Restored {}'.format(tape_url))


def get_moose_url(tape_url, retrieval):
    """
    Get all of the data from `tape_url`, which is already known to be a MOOSE
    url. Data is not cached and is instead copied directly into the destination
    directory.

    :param str tape_url: The url to fetch
    :param pdata_app.models.RetrievalRequest retrieval: The retrieval object
    """
    logger.debug('Starting restoring {}'.format(tape_url))

    for data_req in retrieval.data_request.all():
        all_files = data_req.datafile_set.filter(tape_url=tape_url,
                                                 online=False)
        if not all_files:
            # There may not be any files for this data request at this URL that
            # are not online
            continue
        time_units = all_files[0].time_units
        calendar = all_files[0].calendar
        start_float = cf_units.date2num(
            datetime.datetime(retrieval.start_year, 1, 1), time_units,
            calendar
        )
        end_float = cf_units.date2num(
            datetime.datetime(retrieval.end_year + 1, 1, 1), time_units,
            calendar
        )
        data_files = all_files.filter(start_time__gte=start_float,
                                      end_time__lt=end_float)

        if not data_files:
            # There may not be any files for this data request at this time
            # period at this URL
            continue

        # because the PRIMAVERA data that has been stored in MASS is in a DRS
        # directory structure already then all files that have an identical
        # tape_url will be placed in the same output directory
        data_file = data_files.first()
        drs_path = construct_drs_path(data_file)
        if not cmd_args.alternative:
            drs_dir = os.path.join(BASE_OUTPUT_DIR, drs_path)
        else:
            drs_dir = os.path.join(cmd_args.alternative, drs_path)

        # create the path if it doesn't exist
        if not os.path.exists(drs_dir):
            os.makedirs(drs_dir)

        moose_urls = ['{}/{}'.format(tape_url, df.name) for df in data_files]
        cmd = 'moo get {} {}'.format(' '.join(moose_urls), drs_dir)

        logger.debug('MOOSE command is:\n{}'.format(cmd))

        try:
            cmd_out = _run_command(cmd)
        except RuntimeError as exc:
            logger.error('MOOSE command failed\n{}'.
                         format(exc.message))
            sys.exit(1)

        logger.debug('Restored {}'.format(tape_url))

        _remove_data_license_files(drs_dir)

        for data_file in data_files:
            try:
                _check_file_checksum(data_file, os.path.join(drs_dir,
                                                             data_file.name))
            except ChecksumError:
                # warning message has already been displayed and so take no
                # further action
                pass

            data_file.directory = drs_dir
            data_file.online = True
            data_file.save()


def copy_files_into_drs(retrieval, tape_url, args):
    """
    Copy files from the restored data cache into the DRS structure.

    :param pdata_app.models.RetrievalRequest retrieval: The retrieval object.
    :param str tape_url: The portion of the data now available on disk.
    :param argparse.Namespace args: The parsed command line arguments
        namespace.
    """
    logger.debug('Copying files from tape url {}'.format(tape_url))

    url_dir = _make_tape_url_dir(tape_url, skip_creation=True)

    for data_req in retrieval.data_request.all():
        first_file = data_req.datafile_set.first()
        time_units = first_file.time_units
        calendar = first_file.calendar
        start_float = cf_units.date2num(
            datetime.datetime(retrieval.start_year, 1, 1), time_units,
            calendar
        )
        end_float = cf_units.date2num(
            datetime.datetime(retrieval.end_year + 1, 1, 1), time_units,
            calendar
        )

        data_files = data_req.datafile_set.filter(tape_url=tape_url,
                                                  online=False,
                                                  start_time__gte=start_float,
                                                  end_time__lt=end_float)

        for data_file in data_files:
            file_submission_dir = data_file.incoming_directory
            extracted_file_path = os.path.join(url_dir,
                                               file_submission_dir.lstrip('/'),
                                               data_file.name)
            if not os.path.exists(extracted_file_path):
                msg = ('Unable to find file {} in the extracted data at {}. The '
                       'expected path was {}'.format(data_file.name, url_dir,
                                                     extracted_file_path))
                logger.error(msg)
                sys.exit(1)

            drs_path = construct_drs_path(data_file)
            if not args.alternative:
                drs_dir = os.path.join(BASE_OUTPUT_DIR, drs_path)
            else:
                drs_dir = os.path.join(args.alternative, drs_path)
            dest_file_path = os.path.join(drs_dir, data_file.name)

            # create the path if it doesn't exist
            if not os.path.exists(drs_dir):
                os.makedirs(drs_dir)

            if os.path.exists(dest_file_path):
                msg = 'File already exists on disk: {}'.format(dest_file_path)
                logger.warning(msg)
            else:
                if check_same_gws(extracted_file_path, drs_dir):
                    # if src and destination are on the same GWS then create a
                    # hard link, which will be faster and use less disk space
                    os.link(extracted_file_path, dest_file_path)
                    logger.debug('Created link to:\n{}\nat:\n{}'.format(
                        extracted_file_path, dest_file_path))
                else:
                    # if on different GWS then will have to copy
                    shutil.copyfile(extracted_file_path, dest_file_path)
                    logger.debug('Copied:\n{}\nto:\n{}'.format(
                        extracted_file_path, dest_file_path))

            if not args.skip_checksums:
                try:
                    _check_file_checksum(data_file, dest_file_path)
                except ChecksumError:
                    # warning message has already been displayed and so take no
                    # further action
                    pass

            # create symbolic link from main directory if storing data in an
            # alternative directory
            if args.alternative:
                primary_path = os.path.join(BASE_OUTPUT_DIR, drs_path)
                if not os.path.exists(primary_path):
                    os.makedirs(primary_path)
                os.symlink(dest_file_path,
                           os.path.join(primary_path, data_file.name))

            # set directory and set status as being online
            data_file.directory = drs_dir
            data_file.online = True
            data_file.save()

    logger.debug('Finished copying files from tape url {}'.format(tape_url))


def construct_drs_path(data_file):
    """
    Make the CMIP6 DRS directory path for the specified file.

    :param pdata_app.models.DataFile data_file:
    :returns: A string containing the DRS directory structure
    """
    return os.path.join(
        data_file.project.short_name,
        data_file.activity_id.short_name,
        data_file.institute.short_name,
        data_file.climate_model.short_name,
        data_file.experiment.short_name,
        data_file.rip_code,
        data_file.variable_request.table_name,
        data_file.variable_request.cmor_name,
        data_file.grid,
        data_file.version
    )


def _check_file_checksum(data_file, file_path):
    """
    Check that a restored file's checksum matches the value in the database.

    :param pdata_app.models.DataFile data_file: the database file object
    :param str file_path: the path to the restored file
    :raises ChecksumError: if the checksums don't match.
    """
    checksum_methods = {'ADLER32': adler32,
                        'MD5': md5,
                        'SHA256': sha256}

    # there is only likely to be one checksum and so chose the last one
    checksum_obj = data_file.checksum_set.last()

    if not checksum_obj:
        msg = ('No checksum exists in the database. Skipping check for {}'.
               format(file_path))
        logger.warning(msg)
        return

    file_checksum = checksum_methods[checksum_obj.checksum_type](file_path)

    if file_checksum != checksum_obj.checksum_value:
        msg = ('Checksum for restored file does not match its value in the '
               'database.\n {}: {}:{}\nDatabase: {}:{}'.format(file_path,
               checksum_obj.checksum_type, file_checksum,
               checksum_obj.checksum_type, checksum_obj.checksum_value))
        logger.warning(msg)
        raise ChecksumError(msg)


def _make_logfile_name(directory=None):
    """
    From the current date and time make a filename for a log-file.

    :param str directory: The optional directory path to prepend to the
        generated filename
    :returns: The name of a log file to use
    """
    time_str = datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S')
    filename = '{}_{}.txt'.format(LOG_PREFIX, time_str)
    if directory:
        return os.path.join(directory, filename)
    else:
        return filename


def _make_tape_url_dir(tape_url, skip_creation=False):
    """
    Make the directory to store the specified URL in and return its path

    :param str tape_url: The url to construct a directory for.
    :param bool skip_creation: If true then don't try to create the directory
        and just return its path.
    :returns: The directory path to store this tape_url in.
    """
    dir_name = os.path.join(BASE_RETRIEVAL_DIR, tape_url.replace(':', '_'))

    if not skip_creation:
        if not os.path.exists(dir_name):
            os.mkdir(dir_name)

    return dir_name


def _run_command(command):
    """
    Run the command specified and return any output to stdout or stderr as
    a list of strings.
    :param str command: The complete command to run.
    :returns: Any output from the command as a list of strings.
    :raises RuntimeError: If the command did not complete successfully.
    """
    cmd_out = None
    try:
        cmd_out = subprocess.check_output(command, stderr=subprocess.STDOUT,
                                          shell=True)
    except subprocess.CalledProcessError as exc:
        msg = ('Command did not complete sucessfully.\ncommmand:\n{}\n'
               'produced error:\n{}'.format(command, exc.output))
        logger.warning(msg)
        if exc.returncode != 0:
            raise RuntimeError(msg)

    return cmd_out.rstrip().split('\n')


def _email_user_success(retrieval):
    """
    Send an email to request's creator advising them that their data's been
    successfully restored.

    :param pdata_app.models.RetrievalRequest retrieval: the retrieval object
    """
    contact_user_id = Settings.get_solo().contact_user_id
    contact_user = User.objects.get(username=contact_user_id)

    msg = (
        'Dear {},\n'
        '\n'
        'Your retrieval request number {} has now been restored from elastic '
        'tape to group workspace. The data will be available in the DRS '
        'directory structure at {}.\n'
        '\n'
        'To free up disk space on the group workspaces we would be grateful '
        'if this data could be deleted as soon as you have finished analysing '
        'it.\n'
        '\n'
        'Thanks,\n'
        '\n'
        '{}'.format(retrieval.requester.first_name, retrieval.id,
                    BASE_OUTPUT_DIR, contact_user.first_name)
    )

    _email = EmailQueue.objects.create(
        recipient=retrieval.requester,
        subject=('[PRIMAVERA_DMT] Retrieval Request {} Complete'.
                 format(retrieval.id)),
        message=msg
    )


def _remove_data_license_files(dir_path):
    """
    Delete any Met Office Data License files from the directory specified.

    :param str dir_path: The directory to remove files from.
    """
    license_file_glob = 'MetOffice_data_licence.*'

    for lic_file in glob.iglob(os.path.join(dir_path, license_file_glob)):
        try:
            os.remove(lic_file)
        except OSError:
            logger.warning('Unable to delete license file {}'.format(lic_file))


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Perform a PRIMAVERA '
                                                 'retrieval request.')
    parser.add_argument('retrieval_id', help='the id of the retrieval request '
        'to carry out.', type=int)
    parser.add_argument('-a', '--alternative', help="store data in alternative "
        "directory and create a symbolic link to each file from the main "
        "retrieval directory")
    parser.add_argument('-n', '--no_restore', help="don't restore data from "
        "tape. Assume that it already has been and extract files from the "
        "restoration directory. This will only work for files retrieved from "
        "Elastic Tape; no files will be restored if this option is used for "
        "files in MASS.", action='store_true')
    parser.add_argument('-s', '--skip_checksums', help="don't check the "
        "checksums on restored files.", action='store_true')
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
    logger.debug('Starting retrieve_request.py')

    # check retrieval
    retrieval = match_one(RetrievalRequest, id=args.retrieval_id)
    if not retrieval:
        logger.error('Unable to find retrieval id {}'.format(
            args.retrieval_id))
        sys.exit(1)

    if retrieval.date_complete:
        logger.error('Retrieval {} was already completed, at {}.'.
                     format(retrieval.id,
                            retrieval.date_complete.strftime('%Y-%m-%d %H:%M')))
        sys.exit(1)

    tape_urls = []
    for data_req in retrieval.data_request.all():
        all_files = data_req.datafile_set.all()
        time_units = all_files[0].time_units
        calendar = all_files[0].calendar
        start_float = cf_units.date2num(
            datetime.datetime(retrieval.start_year, 1, 1), time_units,
            calendar
        )
        end_float = cf_units.date2num(
            datetime.datetime(retrieval.end_year + 1, 1, 1), time_units,
            calendar
        )
        data_files = all_files.filter(start_time__gte=start_float,
                                      end_time__lt=end_float)

        tape_urls += [qs['tape_url'] for qs in data_files.values('tape_url')]

    tape_urls = list(set(tape_urls))
    tape_urls.sort()


    for tape_url in tape_urls:
        if not args.no_restore:
            get_tape_url(tape_url, retrieval)

        if tape_url.startswith('et:'):
            copy_files_into_drs(retrieval, tape_url, args)

    # set date_complete in the db
    retrieval.date_complete = timezone.now()
    retrieval.save()

    # send an email to advise the user that their data's been restored
    _email_user_success(retrieval)

    logger.debug('Completed retrieve_request.py')


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
