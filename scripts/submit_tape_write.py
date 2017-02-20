#!/usr/bin/env python2.7
"""
submit_tape_write.py

This script is run by a cron job. It identifies data submissions that have a
status of VALIDATED but don't have a
"""
from __future__ import print_function
import subprocess

import django
django.setup()
from django.db.models import Count
from pdata_app.models import DataSubmission
from vocabs.vocabs import STATUS_VALUES

TAPE_WRITE_SCRIPT = ('/home/users/jseddon/primavera/primavera-dmt/scripts/'
    'submission_to_tape.sh')
LOTUS_OPTIONS = '-o ~/lotus/%J.o -q short-serial -W 01:00'


def submit_tape_write(submission):
    """
    Submit a LOTUS job to run the validation.

    :param submission:
    :return:
    """
    cmd = 'bsub {} {} --log-level DEBUG {}'.format(
        LOTUS_OPTIONS, TAPE_WRITE_SCRIPT, submission)
    bsub_output = subprocess.check_output(cmd, shell=True)
    print('Submission: {}'.format(submission))
    print(bsub_output)


def main():
    status_to_process = STATUS_VALUES['VALIDATED']

    # find all data submissions that have been validated and contain no
    # datafiles that have a tape_url
    submissions = (DataSubmission.objects.annotate(Count('datafile__tape_url')).
        filter(status=status_to_process, datafile__tape_url__count=0))

    for submission in submissions:
        submit_tape_write(submission.incoming_directory)


if __name__ == "__main__":
    main()
