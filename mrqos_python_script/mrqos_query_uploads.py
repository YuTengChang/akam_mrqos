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
import glob
import logging


def main():
    """  this function will do the query on 5 different measurement and upload
    the data to hdfs accordingly, this also join tables at single time point """

    # different queries (various types)
    # logging set-up
    logging.basicConfig(filename=os.path.join(config.mrqos_logging, 'mrqos_query_uploads.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    # ##############################
    # start the script
    # parameter setting
    # ##############################

    # check what to upload:
    filelist = sorted(glob.glob(os.path.join(config.mrqos_data_backup,
                                             'joined',
                                             'mrqos_join.*.csv')))

    for fileitem in filelist:
        this_ts = fileitem.split('.')[-2]
        filename = fileitem.split('/')[-1]
        hdfs_d = os.path.join(config.hdfs_table,
                              'mrqos_join',
                              'ts=%s' % this_ts)
        try:
            logger.info('trying upload and alter for file: %s' % filename)
            beeline.upload_to_hive(fileitem,
                                   hdfs_d,
                                   'ts=%s' % this_ts,
                                   'mrqos_join',
                                   logger)
            # remove local file
            os.remove(fileitem)
            logger.info('remove local: %s' % filename)
        except sp.CalledProcessError as e:
            logger.error('upload to hdfs & alter hive table failed for file: %s' % fileitem)
            logger.error('error message: %s', e.message)


    # TODO: clean up the joined

if __name__ == '__main__':
    sys.exit(main())