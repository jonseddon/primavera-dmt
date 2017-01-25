#!/usr/bin/env python2.7
"""
submit_tape_write.py

This script is run by a cron job. It identifies data submissions that have a
status of VALIDATED but don't have a
"""
from __future__ import print_function
import os
import pwd
import stat
import subprocess

import django
django.setup()
from pdata_app.models import DataSubmission
from vocabs.vocabs import STATUS_VALUES

STATUS_TO_PROCESS = STATUS_VALUES['PENDING_PROCESSING']
ADMIN_USER = 'jseddon'
TAPE_WRITE_SCRIPT = ('/home/users/jseddon/primavera/primavera-dmt/scripts/'
    'submit_tape_write.sh')
LOTUS_OPTIONS = '-o ~/%J.o -q short-serial -W 01:00'


def submit_validation(submission):
    """
    Submit a LOTUS job to run the validation.

    :param submission:
    :return:
    """
    cmd = 'bsub {} {} --log-level DEBUG --processes {} {}'.format(
        LOTUS_OPTIONS, VALIDATE_SCRIPT, NUM_PROCS_USE_LOTUS, submission)
    bsub_output = subprocess.check_output(cmd, shell=True)
    print('Sunmission: {}'.format(submission))
    print(bsub_output)


def main():
    submissions = DataSubmission.objects.filter(status=STATUS_TO_PROCESS)

    for submission in submissions:
        # if are_files_chowned(submission):
        #     submission.status = STATUS_VALUES['ARRIVED']
        #     submission.save()
        submit_validation(submission.directory)


if __name__ == "__main__":
    main()
