#!/usr/bin/env bash
# variable settings
mapmon_machine="72.246.193.143"
local_dir="/home/ychang/Documents/Projects/18-DDC/MRQOS_local_data/"
timenow=`date -d%s`
local_file=`echo "mrqos_region_"$timenow".csv"`
temp_file="temp.csv"

# fetch the table as a remote CSV on mapmon machine
gwsh -2 $mapmon_machine "/a/bin/sql2 --csv ' select * from _local_a_maprule_qos_view_by_region ' > ~/full-table-mrqos-view-by-region"
scp -Sgwsh testgrp@$mapmon_machine:/home/testgrp/full-table-mrqos-view-by-region $local_dir
