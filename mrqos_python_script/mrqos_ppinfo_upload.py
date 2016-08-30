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
import glob

def main():
    # logging set-up
    logging.basicConfig(filename=os.path.join(config.mrqos_logging, 'pp_info_upload.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    # ##############################
    # start the script
    # parameter setting

    ts = int(time.time())
    #datestamp = time.strftime('%Y%m%d', time.gmtime(float(ts)))
    #hourstamp = time.strftime('%H', time.gmtime(float(ts)))

    logger.info('Process ppinfo table started at %s.' % str(ts))

    file_list = glob.glob(os.path.join(config.mako_local, 'pp_info_*.txt'))

    for item in file_list:
        filename = item.split('/')[-1]
        hourstamp = filename.split('.')[-2]
        datestamp = filename.split('_')[-1].split('.')[0]

        # upload to hdfs and add hive partitions
        try:
            hdfs_d = os.path.join(config.hdfs_table,
                                    'ppinfo',
                                    'datestamp=%s/hour=%s' % (str(datestamp), str(hourstamp)))

            beeline.upload_to_hive(os.path.join(config.mako_local, filename),
                                   hdfs_d,
                                   'datestamp=%s, hour=%s' % (str(datestamp), str(hourstamp)),
                                   'mrqos.ppinfo',
                                   logger)
            logger.info('successfully upload to HDFS and add partition: mrqos.%s' % item)
            os.remove(item)

        except sp.CalledProcessError as e:
            logger.error('upload to hive and add partition failed: mrqos.%s' % item)
            logger.error('error: %s' % e.message)


if __name__ == '__main__':
    sys.exit(main())