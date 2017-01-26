#!/bin/bash
#BSUB -o /home/users/jseddon/lotus/%J.o
#BSUB -e /home/users/jseddon/lotus/%J.e
#BSUB -q short-serial
#BSUB -W 01:00
#
# SYNOPSIS
#
#   submission_to_tape.sh <options> <directory>
#
# DESCRIPTION
#
#   This script is a thin shell wrapper around the submission_to_tape.py
#   Python script. All options are passed through to the Python script after
#   some initialisation has been performed.
#
# ARGUMENTS
#
#   directory
#       The top-level directory of the submission
#
# OPTIONS
#
#   Any additional options or arguments are passed through as-is to the specified
#   processing application. Please see the submission_to_tape.py options.

DMT_DIR=/home/users/jseddon/primavera/primavera-dmt/

. /home/users/jseddon/primavera/venvs/django/bin/activate

cd $DMT_DIR
. $DMT_DIR/setup_jon.sh
$DMT_DIR/scripts/submission_to_tape.py --log-level debug "$@"
