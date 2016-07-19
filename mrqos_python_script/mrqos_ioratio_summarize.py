#!/a/bin/python2.7
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
import logging

def main():
    # logging set-up
    logging.basicConfig(filename=os.path.join(config.mrqos_logging, 'io_ratio_prefetch.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    # ##############################
    # start the script
    # parameter setting

    ts = int(time.time())
    datestamp = time.strftime('%Y%m%d', time.gmtime(float(ts)))
    window_length = config.mrqos_join_delete + 1*24*60*60
    datestamp_14d_ago = time.strftime('%Y%m%d', time.gmtime(float(ts-window_length)))
    logger.info('## Summarize IORATIO table started at %s.' % str(ts))

    logger.info("direct summarize and insert into mrqos_sum_io.")
    # direct join and insert in hive
    f = open('/home/testgrp/MRQOS/mrqos_hive_query/MRQOS_table_summarize_ioratio.hive', 'r')
    strcmd = f.read()
    strcmd_s = strcmd % (str(datestamp), str(datestamp_14d_ago), str(datestamp))
    f.close()
    print "    ****  perform beeline for join."
    beeline.bln_e(strcmd_s)

if __name__ == '__main__':
    sys.exit(main())