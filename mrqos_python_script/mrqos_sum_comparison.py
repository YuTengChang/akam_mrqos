# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 11:31:55 2015

@author: ychang
"""

import sys,os
sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import time
import datetime
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline
import logging

def main():
    """  this function will do the query on 5 different measurement and upload
    the data to hdfs accordingly, this also join tables at single time point """

    # different queries (various types)
    # logging set-up
    logging.basicConfig(filename=os.path.join(config.mrqos_logging, 'mrqos_sum_comparison.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    # ##############################
    # start the script
    # parameter setting
    # ##############################
    n_retrial = config.query_retrial

    list_of_partitions = [x for x in beeline.show_partitions('mrqos.mrqos_sum').split('\n') if '=' in x]
    ts_now = list_of_partitions[-1].split('=')[-1]
    ts_14d = list_of_partitions[-15].split('=')[-1]
    ts_28d = list_of_partitions[-29].split('=')[-1]
    ts_3d = list_of_partitions[-4].split('=')[-1]

    qry0 = "set mapred.child.java.opts=-Xmx2000m; use mrqos; set hive.cli.print.header=true; SELECT b.ts_2w, a.*, b.sp95_t95_2w, b.sp95_t90_2w, b.sp75_t95_2w, b.sp75_t90_2w, b.dp95_t95_2w, b.dp95_t90_2w, b.dp75_t95_2w, b.dp75_t90_2w, b.ocy_t95_2w, b.ocy_t90_2w, b.oct_t95_2w, b.oct_t90_2w FROM (SELECT ts ts_now, maprule, geoname, netname, sp95_t95, sp95_t90, sp75_t95, sp75_t90, dp95_t95, dp95_t90, dp75_t95, dp75_t90, 100-icy_t95 ocy_t95, 100-icy_t90 ocy_t90, 100-ict_t95 oct_t95, 100-ict_t90 oct_t90, total_mbps from mrqos_sum where ts='%s' ) a JOIN ( select maprule, geoname, netname, sp95_t95 sp95_t95_2w, sp95_t90 sp95_t90_2w, sp75_t95 sp75_t95_2w, sp75_t90 sp75_t90_2w, dp95_t95 dp95_t95_2w, dp95_t90 dp95_t90_2w, dp75_t95 dp75_t95_2w, dp75_t90 dp75_t90_2w, 100-icy_t95 ocy_t95_2w, 100-icy_t90 ocy_t90_2w, 100-ict_t95 oct_t95_2w, 100-ict_t90 oct_t90_2w, ts ts_2w, total_mbps total_mbps_2w from mrqos_sum where ts='%s' ) b ON ( a.maprule = b.maprule AND a.geoname = b.geoname AND a.netname = b.netname ) "
    qry = " set mapred.child.java.opts=-Xmx2000m; use mrqos; set hive.cli.print.header=true; SELECT b.ts_2w, a.*, b.sp95_t95_2w, b.sp95_t90_2w, b.sp75_t95_2w, b.sp75_t90_2w, b.dp95_t95_2w, b.dp95_t90_2w, b.dp75_t95_2w, b.dp75_t90_2w, b.ocy_t95_2w, b.ocy_t90_2w, b.oct_t95_2w, b.oct_t90_2w, b.total_mbps_2w FROM (SELECT ts ts_now, maprule, geoname, netname, sp95_t95, sp95_t90, sp75_t95, sp75_t90, dp95_t95, dp95_t90, dp75_t95, dp75_t90, 100-icy_t95 ocy_t95, 100-icy_t90 ocy_t90, 100-ict_t95 oct_t95, 100-ict_t90 oct_t90, total_mbps from mrqos_sum where ts='%s' ) a JOIN ( select maprule, geoname, netname, sp95_t95 sp95_t95_2w, sp95_t90 sp95_t90_2w, sp75_t95 sp75_t95_2w, sp75_t90 sp75_t90_2w, dp95_t95 dp95_t95_2w, dp95_t90 dp95_t90_2w, dp75_t95 dp75_t95_2w, dp75_t90 dp75_t90_2w, 100-icy_t95 ocy_t95_2w, 100-icy_t90 ocy_t90_2w, 100-ict_t95 oct_t95_2w, 100-ict_t90 oct_t90_2w, ts ts_2w, total_mbps total_mbps_2w from mrqos_sum where ts='%s' ) b ON ( a.maprule = b.maprule AND a.geoname = b.geoname AND a.netname = b.netname ) "

    content = '''beeline.bln_e_output(qry0 % (ts_now, ts_14d), os.path.join(config.mrqos_data, 'processed_2wjoin_full.tmp')) '''
    my_retrial(content, id='2W summary (no load)', n_retrial=n_retrial, logger=logger)
    content = '''beeline.bln_e_output(qry % (ts_now, ts_14d), os.path.join(config.mrqos_data, 'processed_2wjoin_full_wloads.tmp')) '''
    my_retrial(content, id='2W summary', n_retrial=n_retrial, logger=logger)
    #content = '''beeline.bln_e_output(qry % (ts_now, ts_28d), os.path.join(config.mrqos_data, 'processed_4wjoin_full_wloads.tmp')) '''
    #my_retrial(content, id='4W summary', n_retrial=n_retrial, logger=logger)
    #content = '''beeline.bln_e_output(qry % (ts_now, ts_3d), os.path.join(config.mrqos_data, 'processed_3djoin_full_wloads.tmp')) '''
    #my_retrial(content, id='3D summary', n_retrial=n_retrial, logger=logger)

#    try:
#        beeline.bln_e_output(qry % (ts_now, ts_14d),
#                             os.path.join(config.mrqos_data,
#                                          'processed_2wjoin_full_wloads.tmp'))
#        beeline.bln_e_output(qry % (ts_now, ts_28d),
#                             os.path.join(config.mrqos_data,
#                                          'processed_4wjoin_full_wloads.tmp'))
#        beeline.bln_e_output(qry % (ts_now, ts_3d),
#                             os.path.join(config.mrqos_data,
#                                          'processed_3djoin_full_wloads.tmp'))
#    except sp.CalledProcessError as e:
#        logger.error('obtain summary failed')


def my_retrial(content, id, n_retrial, logger):
    flag = 0
    count = 0
    while (flag == 0) and (count < n_retrial):
        try:
            tic = time.time()
            codes = compile(content, '<string>', 'exec')
            exec codes
            #exec(content)
            flag = 1
            logger.info('%s success with time cost=%s sec' % (id, str(time.time()-tic)))
        except:
            count += 1
            logger.error('%s failed at %s trial.' % (id, str(count)))




if __name__ == '__main__':
    sys.exit(main())