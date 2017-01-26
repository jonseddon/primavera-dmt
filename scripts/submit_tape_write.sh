#!/bin/bash
#BSUB -o /home/users/jseddon/%J.o
#BSUB -e /home/users/jseddon/%J.e
#BSUB -q short-serial
#BSUB -W 01:00
#
# SYNOPSIS
#
#   submit_tape_write.sh <options> <directory>
#
# DESCRIPTION
#
#   This script is a thin shell wrapper around the submit_tape_write.py
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
#   processing application. Please see the submit_tape_write.py options.

DMT_DIR=/home/users/jseddon/primavera/primavera-dmt/

. /home/users/jseddon/primavera/venvs/django/bin/activate

cd $DMT_DIR
. $DMT_DIR/setup_jon.sh
$DMT_DIR/scripts/submit_tape_write.py --log-level debug "$@"
