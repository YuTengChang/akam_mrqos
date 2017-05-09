#!/usr/bin/env
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 11 15:32:42 2014

@author: ychang
"""
# %% MODULE IMPORT
#==============================================================================
# this is importation of the modules
#==============================================================================
import subprocess as sp
import numpy
import pandas as pd

# the mixed-data-type
data = numpy.genfromtxt('/u4/ychang/Projects/18-MRQOS/Data/summarized_table2.csv',
                        delimiter='\t',
                        dtype='str',
                        skip_header=1)
headers = ["startdate","enddate","maprule","geoname","netname","sp99_t95",
           "sp99_t90","sp99_t85","sp99_t75","sp99_t50","sp95_t95","sp95_t90",
           "sp95_t85","sp95_t75","sp95_t50","sp90_t95","sp90_t90","sp90_t85",
           "sp90_t75","sp90_t50","sp75_t95","sp75_t90","sp75_t85","sp75_t75",
           "sp75_t50","star","sp99d_area","sp95d_area","sp90d_area","sp75d_area",
           "sp99d_max","sp95d_max","sp90d_max","sp75d_max","sp99d_freq",
           "sp95d_freq","sp90d_freq","sp75d_freq","dp99_t95","dp99_t90",
           "dp99_t85","dp99_t75","dp99_t50","dp95_t95","dp95_t90","dp95_t85",
           "dp95_t75","dp95_t50","dp90_t95","dp90_t90","dp90_t85","dp90_t75",
           "dp90_t50","dp75_t95","dp75_t90","dp75_t85","dp75_t75","dp75_t50",
           "dtar","dp99d_area","dp95d_area","dp90d_area","dp75d_area","dp99d_max",
           "dp95d_max","dp90d_max","dp75d_max","dp99d_freq","dp95d_freq",
           "dp90d_freq","dp75d_freq","icy_t95","icy_t90","icy_t85","icy_t75",
           "icy_t50","icy_tar","icyd_area","icyd_max","icyd_freq","ict_t95",
           "ict_t90","ict_t85","ict_t75","ict_t50","ict_tar","ictd_area",
           "ictd_max","ictd_freq","io_t95","io_t90","io_t85","io_t75","io_t50",
           "io_tar","iod_area","iod_max","iod_freq","peak95_kbps","ping_mbps",
           "p2t_bps_pct_min","sp99_cw","sp99_cd","sp95_cw","sp95_cd","sp90_cw",
           "sp90_cd","sp75_cw","sp75_cd","dp99_cw","dp99_cd","dp95_cw","dp95_cd",
           "dp90_cw","dp90_cd","dp75_cw","dp75_cd","icy_cw","icy_cd","ict_cw",
           "ict_cd","io_cw","io_cd","zero_ping_count","zp_ratio","n_count","avg_sp99d",
           "avg_sp95d","avg_sp90d","avg_sp75d","avg_dp99d","avg_dp95d","avg_dp90d",
           "avg_dp75d","avg_icyd","avg_ictd","avg_iod"]


dff = pd.DataFrame(data,columns=headers);
df = dff.loc[:,['startdate','enddate','maprule','geoname','netname',
                'sp99_t95','avg_sp99d',
                'sp95_t95','star','avg_sp95d',
                'sp90_t95','avg_sp90d',
                'sp75_t95','avg_sp75d',
		        'dp95_t95','dtar','avg_dp95d',
                'icy_t95','icy_tar','avg_icyd',
		        'ict_t95','ict_tar','avg_ictd',
                'io_t95','io_tar','avg_iod',
		        'peak95_kbps']]

# some target number is zero so end up with inifinity violation ratio
# replace such 'Infinity' with valie -1
df.replace('Infinity',-1,inplace=True)
df.replace('NULL',-1,inplace=True)

df['avg_sp99d'] = [str(int(float(x)*100)) for x in df['avg_sp99d']]
df['avg_sp95d'] = [str(int(float(x)*100)) for x in df['avg_sp95d']]
df['avg_sp90d'] = [str(int(float(x)*100)) for x in df['avg_sp90d']]
df['avg_sp75d'] = [str(int(float(x)*100)) for x in df['avg_sp75d']]
df['avg_dp95d'] = [str(int(float(x)*100)) for x in df['avg_dp95d']]
df['avg_icyd'] = [str(int(float(x)*100)) for x in df['avg_icyd']]
df['avg_ictd'] = [str(int(float(x)*100)) for x in df['avg_ictd']]
df['avg_iod'] = [str(int(float(x)*100)) for x in df['avg_iod']]
df['peak95_kbps'] = [str(int(float(x))) for x in df['peak95_kbps']]

columns = ['startdate','enddate','maprule','geoname','netname',
              'sp99_t95','sp99_vio',
              'sp95_t95','sp95_tar','sp95_vio',
              'sp90_t95','sp90_vio',
              'sp75_t95','sp75_vio',
              'dp95_t95','dp95_tar','dp95_vio',
              'icy_t95','icy_tar','icy_vio',
              'ict_t95','ict_tar','ict_vio',
              'io_t95','io_tar','io_vio',
              'load']

df.to_csv('/u4/ychang/Projects/18-MRQOS/Data/temp.csv', index=False, header=False)

cmd_str = 'cat /u4/ychang/Projects/18-MRQOS/Data/mrqos_table_first_line.csv /u4/ychang/Projects/18-MRQOS/Data/temp.csv > /u4/ychang/Projects/18-MRQOS/Data/Mapper.1.MCM.Tables.mrqos_table.csv'
sp.check_call( cmd_str, shell=True )

# p4 folder sync
cmd_str = 'export P4PORT="rsh:ssh -q -a -x -l p4ssh -q -x perforce.akamai.com /bin/true"; /usr/bin/p4 sync /home/ychang/Workspace/mapgrp/config/sysData/staticInfoTables/tables/Mapper.1.MCM.Tables.mrqos_table.csv'
sp.check_call( cmd_str, shell=True )

# p4 edit and update
cmd_str = 'export P4PORT="rsh:ssh -q -a -x -l p4ssh -q -x perforce.akamai.com /bin/true"; /usr/bin/p4 edit /home/ychang/Workspace/mapgrp/config/sysData/staticInfoTables/tables/Mapper.1.MCM.Tables.mrqos_table.csv; cp /u4/ychang/Projects/18-MRQOS/Data/Mapper.1.MCM.Tables.mrqos_table.csv /home/ychang/Workspace/mapgrp/config/sysData/staticInfoTables/tables/Mapper.1.MCM.Tables.mrqos_table.csv; /usr/bin/p4 submit -d"file update" /home/ychang/Workspace/mapgrp/config/sysData/staticInfoTables/tables/Mapper.1.MCM.Tables.mrqos_table.csv;'
sp.check_call( cmd_str, shell=True )
