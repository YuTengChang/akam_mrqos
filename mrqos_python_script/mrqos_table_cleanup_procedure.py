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
    logging.basicConfig(filename=os.path.join(config.mrqos_logging, 'hive_table_cleanup.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    # ##############################
    # start the script
    # parameter setting

    ts = int(time.time())
    ts_timeout = ts - config.mrqos_table_delete * 24 * 3 # 3 days = (24*3) hours of time-out

    date_timeout = time.strftime('%Y%m%d', time.gmtime(float(ts_timeout)))
    # hourstamp = time.strftime('%H', time.gmtime(float(ts)))

    list_to_clean = sorted(list(set([x.split('/')[0] for x in beeline.show_partitions('mrqos.mrqos_region').split('\n')])))
    list_to_clean = [x for x in list_to_clean if '=' in x]

    try:
        # remove the hdfs folder
        for item in list_to_clean:
            hdfsutil.rm(os.path.join(config.hdfs_table,
                                     'mrqos_region',
                                     '%s' % item),
                        r=True)

        # alter the hive table: mrqos_region
        try:
            beeline.drop_partitions(tablename='mrqos.mrqos_region',
                                    condition='datestamp<%s' % str(date_timeout))
        except sp.CalledProcessError as e:
            logger.error('drop partition failed')
            logger.error('error: %s' % e.message)

    except sp.CalledProcessError as e:
        logger.error('removed data from hdfs failed')
        logger.error('error: %s' % e.message)

    # alter hive tables:





if __name__ == '__main__':
    sys.exit(main())