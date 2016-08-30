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
hdfs_table_distance = '/ghostcache/hadoop/data/MRQOS/distance'
hdfs_table_in_country = '/ghostcache/hadoop/data/MRQOS/in_country'
hdfs_table_in_continent = '/ghostcache/hadoop/data/MRQOS/in_continent'
hdfs_table_ra_load = '/ghostcache/hadoop/data/MRQOS/ra_load'
hdfs_table_join = '/ghostcache/hadoop/data/MRQOS/mrqos_join'
hdfs_table_join2 = '/ghostcache/hadoop/data/MRQOS/mrqos_join2'

hdfs_qos_rg_info = '/ghostcache/hadoop/data/MRQOS/mrqos_region/datestamp=%s/hour=%s/ts=%s'
hdfs_qos_rg_hour = '/ghostcache/hadoop/data/MRQOS/mrqos_region_hour/datestamp=%s/hour=%s'
hdfs_qos_rg_day = '/ghostcache/hadoop/data/MRQOS/mrqos_region_day/datestamp=%s'
hdfs_qos_rg_view_hour = '/ghostcache/hadoop/data/MRQOS/region_view_hour/datestamp=%s/hour=%s'
hdfs_qos_case_view_hour = '/ghostcache/hadoop/data/MRQOS/case_view_hour/datestamp=%s/hour=%s'

#==============================================================================
# # Local File Locations
#==============================================================================
#mrqos_data = '/home/ychang/Documents/Projects/18-DDC/MRQOS/mrqos_data'
#mrqos_query = '/home/ychang/Documents/Projects/18-DDC/MRQOS/mrqos_query'
mrqos_data = '/home/testgrp/MRQOS/mrqos_data'
mrqos_query = '/home/testgrp/MRQOS/mrqos_query'
mrqos_data_backup = '/home/testgrp/MRQOS/mrqos_data/backup'
mrqos_hive_query = '/home/testgrp/MRQOS/mrqos_hive_query'
mrqos_query_result = '/home/testgrp/query_results'
#mrqos_data_join_backup = '/home/testgrp/MRQOS/mrqos_data/backup/000000_0.deflate'
lp_solution_depot = '/home/testgrp/MRQOS/mrqos_data/lp'
mrqos_logging = '/home/testgrp/logs'
mako_local = mrqos_data_backup + '/mako_local'

#==============================================================================
# # Constant Configurations
#==============================================================================
query_retrial = 15 # 15 times
query_timeout = 30 # 20 sec

mrqos_table_delete = 60 * 60 # 1800 sec = 60 minutes
mrqos_join_delete = 60 * 60 * 24 * 15 # 15 days

mrqos_region_delete = 60 * 60 * 24 * 5 # 5 days
region_view_hour_delete = 60 * 60 * 24 * 1 # 1 days
case_view_hour_delete = 60 * 60 * 24 * 1 # 1 days

region_summary_back_filling = 6 # 6 hours
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



#==============================================================================
# # Local Parameter Settings
#==============================================================================
region_view_hour_data_source = '81.52.137.180'
mrqos_data_node = '81.52.137.195'
web_server_machine = '172.25.23.222' #(need to check periodically against: 'dev-platformperf-scidb02.kendall.corp.akamai.com')

region_view_hour_db = '/opt/web-data/SQLite3/ra_mrqos.db'
#region_view_hour_init = '/u4/ychang/Projects/18-MRQOS/SQLite3/region_view_hour_init.sql'
case_view_hour_db = '/opt/web-data/SQLite3/case_view_hour.db'
#case_view_hour_init = '/u4/ychang/Projects/18-MRQOS/SQLite3/case_view_hour_init.sql'
#alist_init_import = '/u4/ychang/Projects/18-MRQOS/SQLite3/aslist_init_and_import.sql'
region_view_hour_init = '/home/ychang/Documents/Projects/18-DDC/MRQOS/local_other_script/region_view_hour_init.sql'
case_view_hour_init = '/home/ychang/Documents/Projects/18-DDC/MRQOS/local_other_script/case_view_hour_init.sql'
alist_init_import = '/home/ychang/Documents/Projects/18-DDC/MRQOS/local_other_script/aslist_init_and_import.sql'

region_view_hour_data_local = '/home/ychang/Documents/Projects/18-DDC/MRQOS_local_data/region_view_hour/'
case_view_hour_data_local = '/home/ychang/Documents/Projects/18-DDC/MRQOS_local_data/case_view_hour/'
aslist_file = '/home/ychang/Documents/Projects/18-DDC/MRQOS_local_data/mrqos_aslist.csv'

region_view_hour_data_VM = '/opt/MRQOS_local_data/region_view_hour/'
case_view_hour_data_VM = '/opt/MRQOS_local_data/case_view_hour/'

