#!/home/ychang/anaconda/bin/python2.7
"""
Created on Thu Apr 16 16:47:15 2015

@author: ychang
"""
import sys, os
import shutil

sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import time
import YT_Timeout as ytt
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline


def main():
    '''  this function will do the query on 5 different measurement and upload
    the data to hdfs accordingly, this also join tables at single time point '''

    # different queries (various types)
    mtype = ['score', 'distance', 'in_country', 'in_continent', 'ra_load']

    sql = """sql2 -q map.mapnoccthree.query.akadns.net --csv "`cat """
    post = """`" | tail -n+3 | awk -F"," 'BEGIN{OFS=","}{$1=""; print $0}' | sed 's/^,//g' > """

    # current time
    timenow = int(time.time())

    print "###################"
    print "Start processing the data back in for 10 minute joins"
    print "starting processing time is " + str(timenow)
    print "###################"

    # fetch the data through query with retrials
    for item in mtype:
        flag = 0
        count = 0
        dest = os.path.join(config.mrqos_data, item + '.tmp')
        aggs = os.path.join(config.mrqos_query, item + '.qr')

        cmd = sql + aggs + post + dest
        n_retrial = config.query_retrial
        t_timeout = config.query_timeout
        # multiple times with timeout scheme
        while (flag == 0) and (count < n_retrial):
            try:
                with ytt.Timeout(t_timeout):
                    sp.call(cmd, shell=True)
                    flag = 1
            except:
                count += 1
        # if not fetched successfully, break all
        if count >= n_retrial:
            print "data fetch failed in querying table %s" % item
            return

    # provide SCORE table with peak/off-peak attribute
    sp.call([config.provide_peak], shell=True)

    # backup the individual query file
    if not os.path.exists('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow)):
        os.makedirs('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow))
        for item in mtype:
            filesrc = os.path.join(config.mrqos_data, item + '.tmp')
            filedst = '/home/testgrp/MRQOS/mrqos_data/backup/%s/' % str(timenow)
            shutil.copy(filesrc, filedst)

    # upload to hdfs and link to hive tables
    try:
        for item in mtype:
            listname = os.path.join(config.mrqos_data, item + '.tmp')
            hdfs_d = os.path.join(config.hdfs_table, item, 'ts=%s' % str(timenow))
            upload_to_hive(listname, hdfs_d, str(timenow), item)
        shutil.rmtree('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow))

        # new version of the join tables in hive: direct insert
        hdfs_file = os.path.join(config.hdfs_table, 'mrqos_join', 'ts=%s' % str(timenow), '000000_0.deflate')
        local_file = os.path.join(config.mrqos_data, '000000_0.deflate')
        try:
            f = open('/home/testgrp/MRQOS/MRQOS_table_join2.hive', 'r')
            strcmd = f.read()
            strcmd_s = strcmd % (str(timenow), str(timenow), str(timenow), str(timenow), str(timenow), str(timenow))
            cmd_list = ['hive', '-e', strcmd_s]
            sp.check_call(cmd_list)
            # beeline.BL_e(strcmd_s)
            hdfsutil.get(hdfs_file, config.mrqos_data)
        except:
            print "direct join and insert failed, trying to copy the last successed one"
            try:
                hdfsutil.put(local_file, hdfs_file)
                try:
                    hiveql_str = 'use mrqos; alter table mrqos_join add partition(ts=%s);' % str(timenow)
                    sp.check_call(['hive', '-e', hiveql_str])
                    # beeline.BL_e(hiveql_str)
                except sp.CalledProcessError:
                    raise HiveCreatePartitionError
            except:
                print "copy from duplicated file for mrqos_join failed"
    except:
        print "HDFS upload failed, backup file retains"

        # # join tables in hive (single timestamp)
        # sp.call( [config.provide_join], shell=True )

        # # upload the joined table in hive
        # listname = os.path.join(config.mrqos_data, 'joined_table.tmp')
        # hdfs_d = os.path.join(config.hdfs_table,'mrqos_join','ts=%s' % str(timenow))
        # upload_to_hive(listname, hdfs_d, str(timenow), 'mrqos_join')

    # # check whether last upload is empty, if empty, copy from the second latest one (joined_table.tmp)
    # statinfo = os.stat(listname)
    # if (statinfo.st_size == 0):
    #     sp.call( [config.copy_from_last_join], shell=True )

    # clear the expired data in mrqos_table
    mrqos_table_cleanup()
    # clear the expired data in mrqos_join
    mrqos_join_cleanup()


# ==============================================================================
# # remove partitions from hive table
# ==============================================================================

def mrqos_table_cleanup():
    ''' when called, this function will delete all partitions
        the clnspp table as long as it is older than the threshold '''

    # get the lowest partition
    temp_outputfile = '/home/testgrp/MRQOS/mrqos_data/mrqos_table_partitions.txt'
    hiveql_str = 'use mrqos; show partitions score;'
    # beeline.BL_e_outcall(hiveql_str, temp_outputfile)
    partition_list = open(temp_outputfile, 'w')
    sp.call(['hive', '-e', 'use mrqos; show partitions score;'], stdout=partition_list)
    partition_list.close()
    partition_list = open(temp_outputfile, 'r')
    str_parts = partition_list.read()
    partition_list.close()
    os.remove(temp_outputfile)
    str_parts_list = [i.split('=', 1)[1] for i in str_parts.strip().split('\n')]
    str_parts_list_int = map(int, str_parts_list)

    # check if "partitions" is within the threshold
    timenow = int(time.time())
    for partition in str_parts_list_int:
        if partition < timenow - config.mrqos_table_delete:
            try:
                mtype = ['score', 'distance', 'in_country', 'in_continent', 'ra_load'];
                for item in mtype:
                    # drop partitions
                    hiveql_str = 'use mrqos; alter table ' + item + ' drop if exists partition(ts=%s)' % partition
                    # beeline.BL_e( hiveql_str )
                    sp.check_call(['hive', '-e',
                                   'use mrqos; alter table ' + item + ' drop if exists partition(ts=%s)' % partition])
                    # remove data from HDFS
                    hdfs_d = os.path.join(config.hdfs_table, item, 'ts=%s' % partition)
                    sp.check_call(['hadoop', 'fs', '-rm', '-r', hdfs_d])
            except sp.CalledProcessError:
                raise GenericHadoopError


# ==============================================================================
# # remove partitions from hive table
# ==============================================================================

def mrqos_join_cleanup():
    ''' when called, this function will delete all partitions
        the clnspp table as long as it is older than the threshold '''

    # get the lowest partition
    temp_outputfile = '/home/testgrp/MRQOS/mrqos_data/mrqos_table_partitions.txt'
    hiveql_str = 'use mrqos; show partitions mrqos_join;'
    # beeline.BL_e_outcall(hiveql_str, temp_outputfile)

    partition_list = open(temp_outputfile, 'w')
    sp.call(['hive', '-e', 'use mrqos; show partitions mrqos_join;'], stdout=partition_list)
    partition_list.close()
    partition_list = open(temp_outputfile, 'r')
    str_parts = partition_list.read()
    partition_list.close()
    os.remove(temp_outputfile)
    str_parts_list = [i.split('=', 1)[1] for i in str_parts.strip().split('\n')]
    str_parts_list_int = map(int, str_parts_list)

    # check if "partitions" is within the threshold
    timenow = int(time.time())
    for partition in str_parts_list_int:
        if partition < timenow - config.mrqos_join_delete:
            try:
                # drop partitions
                hiveql_str = 'use mrqos; alter table mrqos_join drop if exists partition(ts=%s)' % partition
                # beeline.BL_e( hiveql_str )
                sp.check_call(
                        ['hive', '-e', 'use mrqos; alter table mrqos_join drop if exists partition(ts=%s)' % partition])
                # remove data from HDFS
                hdfs_d = os.path.join(config.hdfs_table, 'mrqos_join', 'ts=%s' % partition)
                sp.check_call(['hadoop', 'fs', '-rm', '-r', hdfs_d])
            except sp.CalledProcessError:
                raise GenericHadoopError


# ==============================================================================
# # upload to hdfs and link to hive table
# ==============================================================================

def upload_to_hive(listname, hdfs_d, ts, tablename):
    ''' this function will create a partition directory in hdfs with the requisite timestamp. It will
    then add the partition to the table cl_ns_pp with the appropriate timestamp '''

    # hdfs_d = config.hdfsclnspp % (ts)
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
        hiveql_str = 'use mrqos; alter table ' + tablename + ' add partition(ts=%s);' % (ts)
        # beeline.BL_e( hiveql_str )
        sp.check_call(['hive', '-e', hiveql_str])
    except sp.CalledProcessError:
        raise HiveCreatePartitionError


# ==============================================================================
# # hdfs error category
# ==============================================================================
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


if __name__ == '__main__':
    sys.exit(main())
