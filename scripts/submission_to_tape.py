#!/usr/bin/env python2.7
"""
validate_data_submission.py

This script is run by users to validate submitted data files and to create a
data submission in the Data Management Tool.
"""
import argparse
import logging
import os
import re
import subprocess
import sys

import django
django.setup()
import django.core.exceptions
from django.template.defaultfilters import pluralize

from pdata_app.models import DataSubmission
from pdata_app.utils.common import get_temp_filename

DEFAULT_LOG_LEVEL = logging.WARNING
DEFAULT_LOG_FORMAT = '%(levelname)s: %(message)s'

logger = logging.getLogger(__name__)


def _get_submission_object(submission_dir):
    """
    :param str submission_dir: The path of the submission's top level
    directory.
    :returns: The object corresponding to the submission.
    :rtype: pdata_app.models.DataSubmission
    """
    try:
        data_sub = DataSubmission.objects.get(
            incoming_directory=submission_dir)
    except django.core.exceptions.MultipleObjectsReturned:
        msg = 'Multiple DataSubmissions found for directory: {}'.format(
            submission_dir)
        logger.error(msg)
        raise ValueError(msg)
    except django.core.exceptions.ObjectDoesNotExist:
        msg = ('No DataSubmissions have been found in the database for '
               'directory: {}.'.format(submission_dir))
        logger.error(msg)
        raise ValueError(msg)

    return data_sub


def _run_command(command):
    """
    Run the command specified and return any output to stdout or stderr as
    a list of strings.

    :param str command: The complete command to run.
    :returns: Any output from the command as a list of strings.
    :raises RuntimeError: If the command did not complete successfully.
    """
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
    parser = argparse.ArgumentParser(description='Move a data submission to '
                                                 'elastic tape')
    parser.add_argument('directory', help="the submission's top-level "
                                          "initial directory")
    parser.add_argument('-l', '--log-level', help='set logging level to one '
                                                  'of debug, info, warn (the '
                                                  'default), or error')
    args = parser.parse_args()

    return args


def main(args):
    """
    Main entry point
    """
    data_sub = _get_submission_object(args.directory)

    # make a file containing the paths of the files to write to tape
    filelist_name = get_temp_filename('filelist.txt')
    with open(filelist_name, 'w') as fh:
        for data_file in data_sub.get_data_files():
            fh.write(os.path.join(data_file.directory, data_file.name) + '\n')
    logger.debug('File list written to {}'.format(filelist_name))

    # run the et_put.py command to send the files to tape
    cmd = 'et_put.py -v -w primavera -f {}'.format(filelist_name)
    cmd_output = _run_command(cmd)

    # find the batch id from the text returned by et_put.py
    batch_id = None
    for line in cmd_output:
        components = re.match(r'Batch ID: (\d+)', line)
        if components:
            batch_id = components.group(1)
            break

    if batch_id:
        logger.debug('Submission written to elastic tape with batch id {}'.
                     format(batch_id))
        # add the batch id to all of the submission's files.
        num_files_updated = 0
        for data_file in data_sub.get_data_files():
            data_file.tape_url = 'et:{}'.format(batch_id)
            data_file.save()
            num_files_updated += 1
        logger.debug('Elastic tape URL added to {} file{}.'.
                     format(num_files_updated, pluralize(num_files_updated)))
    else:
        msg = ('Unable to determine the batch id from the output of '
               'et_put.py:\n{}'.format('\n'.join(cmd_output)))
        logger.error(msg)
        raise RuntimeError(msg)


if __name__ == '__main__':
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
            logger.error('log-level must be one of: debug, info, warn or '
                         'error')
            sys.exit(1)
    else:
        logger.setLevel(DEFAULT_LOG_LEVEL)
    console.setFormatter(fmtr)
    logger.addHandler(console)

    # run the code
    main(cmd_args)
