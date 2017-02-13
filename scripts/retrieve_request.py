#!/usr/bin/env python2.7
"""
retrieve_request.py

This script is run by the admin to perform a retrieval request.
"""
import argparse
import datetime
import logging
import os
import re
import shutil
import subprocess
import sys

import django
django.setup()
from django.utils import timezone

from pdata_app.models import RetrievalRequest, DataFile
from pdata_app.utils.dbapi import match_one


__version__ = '0.1.0b1'

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)

# The top-level directory to initially restore files to
BASE_RETRIEVAL_DIR = '/group_workspaces/jasmin2/primavera4/.et_retrievals'
# The top-level directory to write output data to
BASE_OUTPUT_DIR = '/group_workspaces/jasmin2/primavera4/stream1'
# The name of the directory to store et_get.py log files in
LOG_FILE_DIR = '/group_workspaces/jasmin2/primavera4/.et_logs/'
# The prefix to use on et_get.py log files
LOG_PREFIX = 'et_get'
# The number of processes that et_get.py should use.
# Between 5 and 10 are recommended
MAX_ET_GET_PROC = 5


def get_tape_url(tape_url):
    """
    Get all of the data from `tape_url`.

    :param str tape_url: The URL of the tape data to fetch.
    """
    if tape_url.startswith('et:'):
        get_et_url(tape_url)
    else:
        msg = ('Only ET is currently supported. Tape url {} is not '
               'understood.'.format(tape_url))
        logger.error(msg)
        raise NotImplementedError(msg)


def get_et_url(tape_url):
    """
    Get all of the data from `tape_url`, which is already known to be an ET url.

    :param str tape_url: The url to fetch
    """
    logger.debug('Starting restoring {}'.format(tape_url))

    batch_id = tape_url.split(':')[1]

    retrieval_dir = _make_tape_url_dir(tape_url)

    logger.debug('Restoring to {}'.format(retrieval_dir))

    cmd = 'et_get.py -v -l {} -b {} -r {} -t {}'.format(
        _make_logfile_name(LOG_FILE_DIR), batch_id, retrieval_dir,
        MAX_ET_GET_PROC)

    logger.debug('et_get.py command is:\n{}'.format(cmd))

    try:
        # cmd_out = _run_command(cmd)
        pass
    except RuntimeError as exc:
        logger.error('et_get.py command for batch id {} failed\n{}'.
                     format(batch_id, exc.message))
        sys.exit(1)

    logger.debug('Restored {}'.format(tape_url))


def copy_files_into_drs(retrieval, tape_url):
    """
    Copy files from the restored data into the DRS structure.

    :param pdata_app.models.RetrievalRequest retrieval: The retrieval object.
    :param str tape_url: The portion of the data now available on disk.
    """
    logger.debug('Copying files from tape url {}'.format(tape_url))

    url_dir = _make_tape_url_dir(tape_url, skip_creation=True)

    data_files = DataFile.objects.filter(
        data_request=retrieval.data_request.all(), tape_url=tape_url).all()

    for data_file in data_files:
        # TODO might be poss to group all files in a directory into one queryset
        submission_dir = data_file.data_submission.incoming_directory
        file_sub_dir = data_file.incoming_directory
        file_rel_path = os.path.relpath(file_sub_dir, submission_dir)
        extracted_file_path = os.path.join(url_dir, file_rel_path,
                                           data_file.name)
        if not os.path.exists(extracted_file_path):
            msg = ('Unable to find file {} in the extracted data at {}. The '
                   'expected path was {}'.format(data_file.name, url_dir,
                                                 extracted_file_path))
            logger.error(msg)
            sys.exit(1)

        drs_path = make_drs_path(data_file)
        drs_dir = os.path.join(BASE_OUTPUT_DIR, drs_path)
        dest_file_path = os.path.join(drs_dir, data_file.name)

        # create the path if it doesn't exist
        if not os.path.exists(drs_dir):
            os.makedirs(drs_dir)

        if _check_same_gws(extracted_file_path, drs_dir):
            # if src and destination are on the same GWS then create a hard
            # link, which will be faster and use less disk space
            os.link(extracted_file_path, dest_file_path)
        else:
            # if on different GWS then will have to copy
            shutil.copyfile(extracted_file_path, dest_file_path)

        # set directory and set status as being online
        data_file.directory = drs_dir
        data_file.online = True
        data_file.save()

        # TODO check checksum????


def make_drs_path(data_file):
    """
    Make the CMIP6 DRS directory path for the specified file.

    :param pdata_app.models.DataFile data_file:
    :returns: A string containing the DRS directory structure
    """
    return os.path.join(
        data_file.project.short_name,
        data_file.institute.short_name,
        data_file.climate_model.short_name,
        data_file.activity_id.short_name,
        data_file.experiment.short_name,
        data_file.rip_code,
        data_file.variable_request.table_name,
        data_file.variable_request.cmor_name,
        data_file.grid,
        data_file.version
    )


def _check_same_gws(path1, path2):
    """
    Check that two paths both start with the same group workspace name.

    :param str path1: The first path
    :param str path2: The second path
    :returns: True if both paths are in the same group workspace
    """
    gws_pattern = r'^/group_workspaces/jasmin2/primavera\d'
    gws1 = re.match(gws_pattern, path1)
    gws2 = re.match(gws_pattern, path2)

    if not gws1:
        msg = 'Cannot determine group workspace name from {}'.format(path1)
        raise RuntimeError(msg)
    if not gws2:
        msg = 'Cannot determine group workspace name from {}'.format(path2)
        raise RuntimeError(msg)

    return True if gws1.group(0) == gws2.group(0) else False


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


def parse_args():
    """
    Parse command-line arguments
    """
    parser = argparse.ArgumentParser(description='Perform a PRIMAVERA '
                                                 'retrieval request.')
    parser.add_argument('retrieval_id', help='the id of the retrieval request '
                                             'to carry out.', type=int)
    parser.add_argument('-n', '--no_restore', help="don't restore data from "
        "tape. Assume that it already has been and extract files from the "
        "restoration directory.", action='store_true')
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
        logger.error('Retrieval {} was already completed, at {}. '.
                     format(retrieval.id,
                            retrieval.date_complete.strftime('%Y-%m-%d %H:%M')))
        sys.exit(1)

    # Retrieve the data from tape
    #
    # retrieval.data_request.values('datafile__tape_url') gives:
    # <QuerySet [{'datafile__tape_url': u'et:9876'},
    #            {'datafile__tape_url': u'et:8765'}]>

    tape_urls = list(set([qs['datafile__tape_url'] for qs in
                          retrieval.data_request.values('datafile__tape_url')]))

    for tape_url in tape_urls:
        if not args.no_restore:
            get_tape_url(tape_url)

        copy_files_into_drs(retrieval, tape_url)

    # set date_complete in the db
    retrieval.date_complete = timezone.now()
    retrieval.save()

    logger.debug('Completed retrieve_request.py')


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
