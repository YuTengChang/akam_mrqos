#!//usr/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
"""
import sys, os
#import shutil

sys.path.append('/home/testgrp/MRQOS/')
import time
import calendar
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline

def main():
    """ get the date and hour for the previous hour. Will check from the beginning of the day, insert when missing. """
    ts = calendar.timegm(time.gmtime())
    ts_last_hour = ts-3600
    datestamp = time.strftime('%Y%m%d', time.gmtime(float(ts_last_hour)))
    hourstamp = time.strftime('%H', time.gmtime(float(ts_last_hour)))
    hour_list = [str("%02d" % x) for x in range(24)]

    # check if the summary has been performed on this particular hour (last hour)
    if hdfsutil.test_file(os.path.join(config.hdfs_qos_rg_hour % (datestamp, hourstamp), '000000_0.deflate')):
        f = open(os.path.join(config.mrqos_hive_query, 'mrqos_region_summarize_hour.hive'), 'r')
        strcmd = f.read()
        strcmd_s = strcmd % (datestamp, hourstamp, datestamp, hourstamp, datestamp, hourstamp)
        f.close()
        print "    ****  perform beeline for hourly summary for day = %s, hour = %s." %(datestamp, hourstamp)
        try:
            beeline.bln_e(strcmd_s)
        except:
            # delete the folder if summarization failed.
            print "    ****  summarization failed."
            hdfsutil.rm(config.hdfs_qos_rg_hour % (datestamp, hourstamp), r=True)

    # check if the summary has been performed since the beginning of the day, last check on day X is X+1/0:30:00
    for hour in hour_list:
        if hour < hourstamp:
            print "    ****  checking day = %s, hour = %s." % (datestamp, hour)
            if hdfsutil.test_file(os.path.join(config.hdfs_qos_rg_hour % (datestamp, hour), '000000_0.deflate')):
                f = open(os.path.join(config.mrqos_hive_query, 'mrqos_region_summarize_hour.hive'), 'r')
                strcmd = f.read()
                strcmd_s = strcmd % (datestamp, hour, datestamp, hour, datestamp, hour)
                f.close()
                print "    ****  perform beeline for hourly summary for day = %s, hour = %s." %(datestamp, hourstamp)
                try:
                    beeline.bln_e(strcmd_s)
                except:
                    # delete the folder if summarization failed.
                    print "    ****  summarization failed."
                    hdfsutil.rm(config.hdfs_qos_rg_hour % (datestamp, hour), r=True)





# ==============================================================================
# # remove partitions from hive table
# ==============================================================================
#def mrqos_region_cleanup():
#    """ when called, this function will delete all partitions
#        the mrqos_region table as long as it is older than the threshold """



if __name__ == '__main__':
    sys.exit(main())