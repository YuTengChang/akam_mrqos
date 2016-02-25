#!/bin/bash

hadoop fs -ls /ghostcache/hadoop/data/MRQOS/mrqos_join | tail -n+2 | awk '{print $NF}' | cut -d"=" -f2 | awk 'BEGIN{pi=3.14159265359;}{print $0, NR, cos(-2*pi*2*(NR-1)/2016), sin(-2*pi*2*(NR-1)/2016), cos(-2*pi*14*(NR-1)/2016), sin(-2*pi*14*(NR-1)/2016)}' > /home/testgrp/MRQOS/mrqos_data/mrqos_ts.tmp
hadoop fs -rm /ghostcache/hadoop/data/MRQOS/mrqos_ts/mrqos_ts.tmp
hadoop fs -put /home/testgrp/MRQOS/mrqos_data/mrqos_ts.tmp /ghostcache/hadoop/data/MRQOS/mrqos_ts/mrqos_ts.tmp

