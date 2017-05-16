#!/usr/bin/env python2.7
"""
create_root_chown_list.py

This script is run by a cron job. It the DataSubmission's with a status of
PENDING_PROCESSING and writes their top-level directory path to a file. This
file will then be read by another cron job running as CEDA root, which will
chown the files in the submission from the user who submitted them to the
PRIMAVERA DMT admin user.
"""
from __future__ import print_function
from datetime import datetime
import os
import sys

import django
django.setup()
from pdata_app.models import DataSubmission
from vocabs.vocabs import STATUS_VALUES

STATUS_TO_PROCESS = STATUS_VALUES['PENDING_PROCESSING']
OUTPUT_DIRECTORY = '/home/users/jseddon/primavera/root_cron'
TIME_STRING = '%Y%m%d-%H%M'
OUTPUT_FILENAME_FORMAT = 'primavera_chown_list.txt'


def main():
    submissions = DataSubmission.objects.filter(status=STATUS_TO_PROCESS)
    if not submissions:
        sys.exit(0)

    time_now = datetime.utcnow()
    filename = OUTPUT_FILENAME_FORMAT.format(time_now.strftime(TIME_STRING))
    file_path = os.path.join(OUTPUT_DIRECTORY, filename)

    with open(file_path, 'w') as fh:
        for submission in submissions:
            fh.write(submission.directory + '\n')


if __name__ == "__main__":
    main()
