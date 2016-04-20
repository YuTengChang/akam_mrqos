# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 11:31:55 2015

@author: ychang
"""

import sys,os
sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import time
import datetime
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline
import glob

def main():
    ts = calendar.timegm(time.gmtime())
    print "###################"
    print "# Performing the LP solution check and push procedure"
    print "# starting processing time is " + str(ts) + " = " + time.strftime('GMT %Y-%m-%d %H:%M:%S', time.localtime(ts))
    print "###################"
    print "  >> check the LP solutions <<"

    filelist = glob.glob(os.path.join(config.lp_solution_depot,'*_prod'))

    for file in filelist:
        print "    **** processing file: "+file


    # upload the summarized table in hive
    print "    #****  upload the summarized table to HDFS."
    hdfs_d = os.path.join(config.hdfs_table,'mrqos_sum_1d','datestamp=%s' % str(datenow))
    upload_to_hive(processed_file, hdfs_d, 'datestamp', str(datenow), 'mrqos_sum_1d')


def upload_to_hive(listname, hdfs_d, partition_name, partition_idx, tablename):
    """ this function will create a partition directory in hdfs with the requisite timestamp. It will
    then add the partition to the table cl_ns_pp with the appropriate timestamp """

    #hdfs_d = config.hdfsclnspp % (ts)
    # create the partition
    try:
        sp.check_call(['hadoop', 'fs', '-mkdir', hdfs_d])
    # upload the data
    except sp.CalledProcessError:
        raise HadoopDirectoryCreateError
    try:
        sp.check_call(['hadoop', 'fs', '-put', listname, hdfs_d])
    except sp.CalledProcessError:
        raise HadoopDataUploadError

    # add the partition
    try:
        hiveql_str = 'use mrqos; alter table %s add partition(%s=%s);' % (tablename,
                                                                          partition_name,
                                                                          partition_idx)
        sp.check_call(['hive', '-e', hiveql_str])
    except sp.CalledProcessError:
        raise HiveCreatePartitionError

#==============================================================================
# # hdfs error category
#==============================================================================
class HadoopDirectoryCreateError(Exception):
    def __init__(self):
        self.message = "Unable to create directory."

class HadoopDataUploadError(Exception):
    def __init__(self):
        self.message = "Unable to upload data to hdfs."

class HiveCreatePartitionError(Exception):
    def __init__(self):
        self.message = "Unable to create partition"

class GenericHadoopError(Exception):
    def __init__(self):
        self.message = "Something went wrong in deleting a partition or associated data"

if __name__=='__main__':
    sys.exit(main())