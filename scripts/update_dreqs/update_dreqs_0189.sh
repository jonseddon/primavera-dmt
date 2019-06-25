#!/bin/bash
# update_dreqs_0189.sh
# Loop through the batches of the specified retrieval and
# extract any tar files in them.

RET_DIR=/gws/nopw/j04/primavera2/.et_retrievals/ret_2049

for batch_dir in "$RET_DIR"/batch_*;
do
    echo "$batch_dir";
    cd "$batch_dir";
    for tmp_file in tmp*;
    do
        echo "$tmp_file";
        tar -xvf "$tmp_file";
    done;
done