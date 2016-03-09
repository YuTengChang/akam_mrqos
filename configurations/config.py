# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 10:57:38 2015

@author: ychang
"""

### configuration files

#==============================================================================
# # HDFS Locations
#==============================================================================
#hdfs_score = '/ghostcache/hadoop/data/MRQOS/score/ts=%s'
#hdfs_distance = '/ghostcache/hadoop/data/MRQOS/distance/ts=%s'
#hdfs_in_country = '/ghostcache/hadoop/data/MRQOS/in_country/ts=%s'
#hdfs_in_continent = '/ghostcache/hadoop/data/MRQOS/in_continent/ts=%s'
#hdfs_ra_load = '/ghostcache/hadoop/data/MRQOS/ra_load/ts=%s'
hdfs_table = '/ghostcache/hadoop/data/MRQOS'
hdfs_table_score = '/ghostcache/hadoop/data/MRQOS/score'
hdfs_table_join = '/ghostcache/hadoop/data/MRQOS/mrqos_join'

hdfs_qos_rg_info = '/ghostcache/hadoop/data/MRQOS/mrqos_region/datestamp=%s/hour=%s/ts=%s'

#==============================================================================
# # Local File Locations
#==============================================================================
#mrqos_data = '/home/ychang/Documents/Projects/18-DDC/MRQOS/mrqos_data'
#mrqos_query = '/home/ychang/Documents/Projects/18-DDC/MRQOS/mrqos_query'
mrqos_data = '/home/testgrp/MRQOS/mrqos_data'
mrqos_query = '/home/testgrp/MRQOS/mrqos_query'
mrqos_data_backup = '/home/testgrp/MRQOS/mrqos_data/backup'
#mrqos_data_join_backup = '/home/testgrp/MRQOS/mrqos_data/backup/000000_0.deflate'


#==============================================================================
# # Constant Configurations
#==============================================================================
query_retrial = 15 # 15 times
query_timeout = 20 # 20 sec

mrqos_table_delete = 60 * 30 # 1800 sec = 30 minutes
mrqos_join_delete = 60 * 60 * 24 * 15 # 15 days


#==============================================================================
# # Shell Scripts
#==============================================================================
#provide_peak = '~/Documents/Projects/18-DDC/MRQOS/peak_label.sh'
provide_peak = '/home/testgrp/MRQOS/peak_label.sh'
provide_join = '/home/testgrp/MRQOS/query_table_join.sh'
copy_from_last_join = '/home/testgrp/MRQOS/duplicate_from_last_join.sh'
create_ts_table = '/home/testgrp/MRQOS/create_mrqos_ts.sh'
obtain_14d = '/home/testgrp/MRQOS/obtain_2wk_ago_summarization.sh'



#==============================================================================
# # HIVE Scripts, table managements
#==============================================================================
add_rg_partition = 'use MRQOS; alter table mrqos_region add partition(datestamp=%s,hour=%s,ts=%s);'