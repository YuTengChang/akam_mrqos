#!//usr/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
"""
import sys, os
#import shutil

sys.path.append('/home/testgrp/MRQOS/')
import time
import datetime
import calendar
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline
import logging

def main():
    # initialze the logger
    logging.basicConfig(filename=os.path.join('/home/testgrp/logs/', 'mapmon_summarize.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    timenow = int(time.time())
    datenow = str(datetime.date.today()-datetime.timedelta(1))
    date_idx = datenow[0:4]+datenow[5:7]+datenow[8:10]

    # get the latest barebone day_idx
    bb_day_idx = beeline.get_last_partitions('mapper.barebones').split('=')[1]
    logger.info("barebone index: day={}".format(bb_day_idx))

    # get the latest mpd yesterday
    uuid_list = [x.split('=')[-1] for x in hdfsutil.ls(os.path.join(os.path.dirname(config.hdfs_table),'mapper','mapmon','day={}'.format(date_idx)))]
    for uuid_idx in uuid_list:
        logger.info("dealing with day={}, uuid={}".format(date_idx, uuid_idx))
        file_location = os.path.join(config.hdfs_table,
                                           'mapmon_sum',
                                           'day={}'.format(date_idx),
                                           'mpg_uuid={}'.format(uuid_idx))
        if hdfsutil.test_dic(file_location):
            logger.info('creating folder: {}'.format(file_location))
            hdfsutil.mkdir(file_location)


        if hdfsutil.test_file(os.path.join(file_location, '000000_0.deflate')):
            f = open(os.path.join(config.mrqos_hive_query, 'mapmon_summarize.hive'), 'r')
            strcmd = f.read()
            strcmd_s = strcmd % (date_idx, uuid_idx, bb_day_idx,
                                 date_idx, uuid_idx,
                                 date_idx, uuid_idx)
            f.close()
            try:
                beeline.bln_e(strcmd_s)
            except:
                # delete the folder if summarization failed.
                logger.warn("summarization failed, removing hdfs folder.")
                hdfsutil.rm(file_location, r=True)
        else:
            logger.info(" file exists.")

if __name__ == '__main__':
    sys.exit(main())