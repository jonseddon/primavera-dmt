#!/bin/bash
#BSUB -o /home/users/jseddon/%J.o
#BSUB -e /home/users/jseddon/%J.e
#BSUB -q short-serial
#BSUB -n 8
#BSUB -R "span[hosts=1]"
#BSUB -W 01:00
#
# SYNOPSIS
#
#   validate_data_submission.sh <options> <directory>
#
# DESCRIPTION
#
#   This script is a thin shell wrapper around the validate_data_submission.py
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
#   processing application. Please see the validate_data_submission.py options.

DMT_DIR=/home/users/jseddon/primavera/primavera-dmt/

. /home/users/jseddon/primavera/venvs/django/bin/activate

cd $DMT_DIR
. $DMT_DIR/setup_jon.sh
$DMT_DIR/scripts/validate_data_submission.py --log-level debug --processes 8 "$@"
