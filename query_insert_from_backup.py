#!/home/ychang/anaconda/bin/python2.7
"""
Created on Thu Apr 16 16:47:15 2015

@author: ychang
"""
import sys,os
import shutil
sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import time
#import httplib
import YT_Timeout as ytt
import configurations.config as config

def main():
    '''  this function will look at the backup directory, try to upload (previously failed, due to HDFS)
    the data to hdfs accordingly, this also join tables at single time point '''

    mtype = ['score','distance','in_country','in_continent','ra_load'];

    timecast = int(time.time())

    print "###################"
    print "Start processing the data insersion for previously failed HDFS operations"
    print "starting processing time is " + str(timecast)
    print "###################"

    # fetch the data through query
    # mrqos_data_backup = '/home/testgrp/MRQOS/mrqos_data/backup'
    for ts_list in os.listdir( config.mrqos_data_backup ):
        timenow = ts_list
        try:
            # upload individual files
            try:
                for item in mtype:
                    listname = os.path.join( config.mrqos_data_backup, ts_list, item+'.tmp' )
                    hdfs_d = os.path.join(config.hdfs_table,item,'ts=%s' % str(timenow))
                    upload_to_hive(listname, hdfs_d, str(timenow), item)
            except:
                print "individual file upload failed, could due to existed folder or HDFS failed"

            # join in the hive for this timestamp
            f = open('/home/testgrp/MRQOS/MRQOS_table_join2.hive','r')
            strcmd = f.read()
            strcmd_s = strcmd % (str(timenow), str(timenow), str(timenow), str(timenow), str(timenow), str(timenow))
            cmd_list = ['hive','-e',strcmd_s]
            sp.check_call( cmd_list )

            # remove the corresponding backup dir
            shutil.rmtree('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow))
        except:
            print "HDFS upload and Hive join failed, backup file retains"


    # check whether last upload is empty, if empty, copy from the second latest one (joined_table.tmp)
    #statinfo = os.stat(listname)
    #if (statinfo.st_size == 0):
    #    sp.call( [config.copy_from_last_join], shell=True )

    # clear the expired data in mrqos_table
    mrqos_table_cleanup()
    # clear the expired data in mrqos_join
    mrqos_join_cleanup()

#==============================================================================
# # remove partitions from hive table
#==============================================================================

def mrqos_table_cleanup():
    ''' when called, this function will delete all partitions
        the clnspp table as long as it is older than the threshold '''

    #get the lowest partition
    partition_list = open('/home/testgrp/MRQOS/mrqos_data/temp_file/mrqos_table_partitions.txt','w')
    sp.call(['hive','-e','use mrqos; show partitions score;'],stdout=partition_list)
    partition_list.close()
    partition_list = open('/home/testgrp/MRQOS/mrqos_data/temp_file/mrqos_table_partitions.txt','r')
    str_parts = partition_list.read()
    partition_list.close()
    os.remove('/home/testgrp/MRQOS/mrqos_data/temp_file/mrqos_table_partitions.txt')
    str_parts_list = [i.split('=',1)[1] for i in str_parts.strip().split('\n')]
    str_parts_list_int=map(int,str_parts_list)

    #check if "partitions" is within the threshold
    timenow = int(time.time())
    for partition in str_parts_list_int:
        if partition < timenow - config.mrqos_table_delete:
            try:
                mtype = ['score','distance','in_country','in_continent','ra_load'];
                for item in mtype:
                    # drop partitions
                    sp.check_call(['hive','-e','use mrqos; alter table ' + item + ' drop if exists partition(ts=%s)' % partition])
                    # remove data from HDFS
                    hdfs_d = os.path.join(config.hdfs_table,item,'ts=%s' % partition)
                    sp.check_call(['hadoop','fs','-rm','-r', hdfs_d])
            except sp.CalledProcessError:
                raise GenericHadoopError


#==============================================================================
# # remove partitions from hive table
#==============================================================================

def mrqos_join_cleanup():
    ''' when called, this function will delete all partitions
        the clnspp table as long as it is older than the threshold '''

    #get the lowest partition
    partition_list = open('/tmp/testgrp/mrqos_table_partitions.txt','w')
    sp.call(['hive','-e','use mrqos; show partitions mrqos_join;'],stdout=partition_list)
    partition_list.close()
    partition_list = open('/tmp/testgrp/mrqos_table_partitions.txt','r')
    str_parts = partition_list.read()
    partition_list.close()
    os.remove('/tmp/testgrp/mrqos_table_partitions.txt')
    str_parts_list = [i.split('=',1)[1] for i in str_parts.strip().split('\n')]
    str_parts_list_int=map(int,str_parts_list)

    #check if "partitions" is within the threshold
    timenow = int(time.time())
    for partition in str_parts_list_int:
        if partition < timenow - config.mrqos_join_delete:
            try:
                # drop partitions
                sp.check_call(['hive','-e','use mrqos; alter table mrqos_join drop if exists partition(ts=%s)' % partition])
                # remove data from HDFS
                hdfs_d = os.path.join(config.hdfs_table,'mrqos_join','ts=%s' % partition)
                sp.check_call(['hadoop','fs','-rm','-r', hdfs_d])
            except sp.CalledProcessError:
                raise GenericHadoopError


#==============================================================================
# # upload to hdfs and link to hive table
#==============================================================================

def upload_to_hive(listname, hdfs_d, ts, tablename):
    ''' this function will create a partition directory in hdfs with the requisite timestamp. It will
    then add the partition to the table cl_ns_pp with the appropriate timestamp '''

    #hdfs_d = config.hdfsclnspp % (ts)
    # create the partition
    try:
        sp.check_call(['hadoop','fs','-mkdir',hdfs_d])
    # upload the data
    except sp.CalledProcessError:
        raise HadoopDirectoryCreateError
    try:
        sp.check_call(['hadoop','fs','-put',listname,hdfs_d])
    except sp.CalledProcessError:
        raise HadoopDataUploadError

    # add the partition
    try:
        hiveql_str = 'use mrqos; alter table ' + tablename + ' add partition(ts=%s);' % (ts)
        sp.check_call(['hive','-e',hiveql_str])
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
