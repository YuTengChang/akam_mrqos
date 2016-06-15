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
    """  this function will do the query on 5 different measurement and upload
    the data to hdfs accordingly, this also join tables at single time point """

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
    print "    ****  querying mrqos data."
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
        # if any of the query not fetched successfully, break all and stop running
        if count >= n_retrial:
            print ">> data fetch failed in querying table %s" % item
            return

    # provide SCORE table with peak/off-peak attribute
    print "    ****  provide PEAK in score."
    sp.call([config.provide_peak], shell=True)

    # backup the individual query file by copying to backup folder
    print "    ****  backing up queried results."
    if not os.path.exists('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow)):
        os.makedirs('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow))
        for item in mtype:
            filesrc = os.path.join(config.mrqos_data, item + '.tmp')
            filedst = '/home/testgrp/MRQOS/mrqos_data/backup/%s/' % str(timenow)
            shutil.copy(filesrc, filedst)

    # upload to hdfs and link to hive tables
    print "    ****  uploading to hdfs and hive."
    try:
        # adding the individual query result to hdfs and add hive partitions
        for item in mtype:
            listname = os.path.join(config.mrqos_data, item + '.tmp')
            hdfs_d = os.path.join(config.hdfs_table, item, 'ts=%s' % str(timenow))
            upload_to_hive(listname, hdfs_d, str(timenow), item)
        shutil.rmtree('/home/testgrp/MRQOS/mrqos_data/backup/%s' % str(timenow))

        # new version of the join tables in hive: direct insert #
        # specify the new joined file in hdfs
        hdfs_file = os.path.join(config.hdfs_table, 'mrqos_join', 'ts=%s' % str(timenow), '000000_0.deflate')
        # specify the local copy of the joined file
        local_file = os.path.join(config.mrqos_data_backup, '000000_0.deflate')
        try:
            print "    ****  direct join and insert into mrqos_join."
            # direct join and insert in hive
            f = open('/home/testgrp/MRQOS/MRQOS_table_join2.hive', 'r')
            strcmd = f.read()
            strcmd_s = strcmd % (str(timenow), str(timenow), str(timenow), str(timenow), str(timenow), str(timenow))
            f.close()
            print "    ****  perform beeline for join."
            beeline.bln_e(strcmd_s)
            # have the local copy of the joined file
            print "    ****  copy the joined file for backup."
            hdfsutil.get(hdfs_file, local_file)
        except sp.CalledProcessError as e:
            print ">> direct join and insert failed, trying to copy the last succeeded one"
            print e.message
            try:
                # upload the last succeeded one from local
                hdfsutil.put(local_file, hdfs_file)
                try:
                    # using hive to add partitions to joined query results
                    hiveql_str = 'use mrqos; alter table mrqos_join add partition(ts=%s);' % str(timenow)
                    beeline.bln_e(hiveql_str)
                except sp.CalledProcessError as e:
                    print ">> copying from duplicated file for mrqos_join failed in adding partitions"
                    print e.message
                    #raise HiveCreatePartitionError
            except:
                print "copying from duplicated file for mrqos_join failed in uploading to hdfs"
    except:
        print "HDFS upload failed, backup file retains"

    # clear the expired data in mrqos_table
    print "    ****  clean up mrqos individual table."
    mrqos_table_cleanup()
    # clear the expired data in mrqos_join
    print "    ****  clean up mrqos joined table."
    mrqos_join_cleanup()


# ==============================================================================
# # remove partitions from hive table
# ==============================================================================

def mrqos_table_cleanup():
    """ when called, this function will delete all partitions
        the clnspp table as long as it is older than the threshold """

    # get the lowest partition by checking the HDFS folders
    score_partitions = hdfsutil.ls(config.hdfs_table_score)
    str_parts_list = [i.split('=', 1)[1] for i in score_partitions]
    str_parts_list_int = map(int, str_parts_list)

    # check if "partitions" is within the threshold, if not, drop in hive table and remove from hdfs
    timenow = int(time.time())
    mtype = ['score', 'distance', 'in_country', 'in_continent', 'ra_load']

    for partition in str_parts_list_int:
        if partition < timenow - config.mrqos_table_delete:
            for item in mtype:
                try:
                    # drop partitions (ok even if partition does not exist)
                    hiveql_str = 'use mrqos; alter table ' + item + ' drop if exists partition(ts=%s)' % str(partition)
                    beeline.bln_e(hiveql_str)
                    # remove data from HDFS (ok even if folder in hdfs does not exist)
                    hdfs_d = os.path.join(config.hdfs_table, item, 'ts=%s' % partition)
                    hdfsutil.rm(hdfs_d, r=True)
                except sp.CalledProcessError as e:
                    print ">> failed in hive table clean up in table: %s." % item
                    print e.message
                    pass
                    # raise GenericHadoopError


# ==============================================================================
# # remove partitions from hive table
# ==============================================================================

def mrqos_join_cleanup():
    """ when called, this function will delete all partitions
        the clnspp table as long as it is older than the threshold """

    # get the lowest partition by checking the HDFS folders
    joined_partitions = hdfsutil.ls(config.hdfs_table_join)
    str_parts_list = [i.split('=', 1)[1] for i in joined_partitions]
    str_parts_list_int = map(int, str_parts_list)

    # check if "partitions" is within the threshold
    timenow = int(time.time())
    for partition in str_parts_list_int:
        if partition < timenow - config.mrqos_join_delete:
            try:
                # drop partitions (ok even if partition does not exist)
                hiveql_str = 'use mrqos; alter table mrqos_join drop if exists partition(ts=%s)' % partition
                beeline.bln_e(hiveql_str)
                # remove data from HDFS (ok even if folder in hdfs does not exist)
                hdfs_d = os.path.join(config.hdfs_table, 'mrqos_join', 'ts=%s' % partition)
                hdfsutil.rm(hdfs_d, r=True)
            except sp.CalledProcessError as e:
                print ">> failed in hive table clean up in table: mrqos_join."
                print e.message
                raise GenericHadoopError


# ==============================================================================
# # upload to hdfs and link to hive table
# ==============================================================================

def upload_to_hive(listname, hdfs_d, ts, tablename):
    """ this function will create a partition directory in hdfs with the requisite timestamp. It will
    then add the partition to the table cl_ns_pp with the appropriate timestamp """

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
        beeline.bln_e(hiveql_str)
        # sp.check_call(['hive', '-e', hiveql_str])
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
