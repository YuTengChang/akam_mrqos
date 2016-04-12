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

def main():
    """  this function will compute the statistics of MRQOS tables within the
    past two weeks (X-1 day : X-2 days) and (X-1 day : X-4 days)"""
    timenow = int(time.time())
    datenow = str(datetime.date.today()-datetime.timedelta(1))
    datenow = datenow[0:4]+datenow[5:7]+datenow[8:10]
    print "###################"
    print "# Start processing the data back in " + datenow + " for two-week window"
    print "# starting processing time is " + str(timenow)
    print "###################"
    max_retrial = 10

    print "    ****  running hive summarizing script."
    script_file = '/home/ychang/MRQOS/mrqos_hive_query/MRQOS_table_summarize_1d.hive'
    output_file = '/home/ychang/MRQOS/mrqos_data/summarized_table_1d.tmp'
    my_retrial(max_retrial, script_file, output_file=output_file)

    return


def my_retrial(max_retrial, script_file, output_file=''):
    retrial = 0
    while retrial < max_retrial:
        try:
            tic = time.time()
            if len(output_file) > 0:
                f = open('%s' % output_file, 'w')
                print "  file opened: %s" % output_file
                sp.check_call(['hive', '-f', '%s' % script_file], stdout=f)
                print "  script running: %s" % script_file
                f.close()
            else:
                sp.check_call(['hive', '-f', '%s' % script_file])
            print "    # success with time cost = %s" % str(time.time()-tic)

            break
        except:
            retrial += 1
            print "    # failed retrial #%s with time cost = %s" % (str(retrial), str(time.time()-tic))
        #if len(output_file) > 0:
        #    f.close()


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