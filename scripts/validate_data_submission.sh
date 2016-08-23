#!/bin/bash
#BSUB -o /home/users/jseddon/%J.o
#BSUB -e /home/users/jseddon/%J.e
#BSUB -q lotus
#BSUB -n 8
#BSUB -R "span[hosts=1]"
#BSUB -W 01:00
DMT_DIR=/home/users/jseddon/primavera/primavera-dmt/
DATA_DIR=/group_workspaces/jasmin2/primavera2/febbraio_netcdf/METOFFICE/HadGEM3-GC2/present_day/day/atmos/uas

. /home/users/jseddon/primavera/venvs/django/bin/activate

cd $DMT_DIR
. $DMT_DIR/setup_jon.sh
$DMT_DIR/scripts/validate_data_submission.py --log-level debug --processes 12 $DATA_DIR
