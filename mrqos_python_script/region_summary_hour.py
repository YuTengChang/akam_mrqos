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
    # get the date and hour for the previous hour
    ts = calendar.timegm(time.gmtime())
    ts_last_hour = ts - 3600
    datestamp = time.strftime('%Y%m%d', time.gmtime(float(ts_last_hour)))
    hourstamp = time.strftime('%H', time.gmtime(float(ts_last_hour)))

    if hdfsutil.test_dic(config.hdfs_qos_rg_hour % (datestamp, hourstamp)):
        f = open(os.path.join(config.mrqos_hive_query, 'mrqos_region_summarize_hour.hive'), 'r')
        strcmd = f.read()
        strcmd_s = strcmd % (datestamp, hourstamp, datestamp, hourstamp, datestamp, hourstamp)
        f.close()
        print "    ****  perform beeline for hourly summary for day = %s, hour = %s." %(datestamp, hourstamp)
        try:
            beeline.bln_e(strcmd_s)
        except:
            print "    ****  summarization failed."
            hdfsutil.rm(config.hdfs_qos_rg_hour % (datestamp, hourstamp), r=True)



# ==============================================================================
# # remove partitions from hive table
# ==============================================================================
#def mrqos_region_cleanup():
#    """ when called, this function will delete all partitions
#        the mrqos_region table as long as it is older than the threshold """



if __name__ == '__main__':
    sys.exit(main())