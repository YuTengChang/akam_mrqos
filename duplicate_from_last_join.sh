#!/bin/bash

hadoop_locale='/ghostcache/hadoop/data/MRQOS/mrqos_join/'
file_second_last=`hadoop fs -ls $hadoop_locale | tail -2 | head -1 | awk '{print $NF}'`
file_last=`hadoop fs -ls $hadoop_locale | tail -1 | awk '{print $NF}'`
hadoop fs -rm $file_last/joined_table.tmp
hadoop fs -cp $file_second_last/joined_table.tmp $file_last/joined_table.tmp