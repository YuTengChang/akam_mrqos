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
    day_in_seconds = 86400

    list_of_partitions = [x for x in beeline.show_partitions('mrqos.mrqos_sum').split('\n') if '=' in x]
    ts_now = list_of_partitions[-1].split('=')[-1]

    ts_ex_14d = time.strftime('%Y%m%d',
                              time.gmtime(time.mktime(time.strptime(ts_now,
                                                                    '%Y%m%d')) - 14 * day_in_seconds))
    ts_14d = [x for x in list_of_partitions if x <= ts_ex_14d][-1].split('=')[-1]

    ts_ex_28d = time.strftime('%Y%m%d',
                              time.gmtime(time.mktime(time.strptime(ts_now,
                                                                    '%Y%m%d')) - 28 * day_in_seconds))
    ts_28d = [x for x in list_of_partitions if x <= ts_ex_28d][-1].split('=')[-1]

    ts_ex_3d = time.strftime('%Y%m%d',
                             time.gmtime(time.mktime(time.strptime(ts_now,
                                                                   '%Y%m%d')) - 3 * day_in_seconds))
    ts_3d = [x for x in list_of_partitions if x <= ts_ex_3d][-1].split('=')[-1]

    #content = '''beeline.bln_e_output(qry0 % (ts_now, ts_14d), os.path.join(config.mrqos_data, 'processed_2wjoin_full.tmp')) '''
    my_retrial(id='2W summary (no load)', n_retrial=n_retrial, logger=logger, ts1=ts_now, ts2=ts_14d)
    #content = '''beeline.bln_e_output(qry % (ts_now, ts_14d), os.path.join(config.mrqos_data, 'processed_2wjoin_full_wloads.tmp')) '''
    my_retrial(id='2W summary', n_retrial=n_retrial, logger=logger, ts1=ts_now, ts2=ts_14d)
    #content = '''beeline.bln_e_output(qry % (ts_now, ts_28d), os.path.join(config.mrqos_data, 'processed_4wjoin_full_wloads.tmp')) '''
    my_retrial(id='4W summary', n_retrial=n_retrial, logger=logger, ts1=ts_now, ts2=ts_28d)
    #content = '''beeline.bln_e_output(qry % (ts_now, ts_3d), os.path.join(config.mrqos_data, 'processed_3djoin_full_wloads.tmp')) '''
    my_retrial(id='3D summary', n_retrial=n_retrial, logger=logger, ts1=ts_now, ts2=ts_3d)

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


def my_retrial(id, n_retrial, logger, ts1, ts2):
    flag = 0
    count = 0
    qry0 = "set mapred.child.java.opts=-Xmx2000m; use mrqos; set hive.cli.print.header=true; SELECT b.ts_2w, a.*, b.sp95_t95_2w, b.sp95_t90_2w, b.sp75_t95_2w, b.sp75_t90_2w, b.dp95_t95_2w, b.dp95_t90_2w, b.dp75_t95_2w, b.dp75_t90_2w, b.ocy_t95_2w, b.ocy_t90_2w, b.oct_t95_2w, b.oct_t90_2w FROM (SELECT ts ts_now, maprule, geoname, netname, sp95_t95, sp95_t90, sp75_t95, sp75_t90, dp95_t95, dp95_t90, dp75_t95, dp75_t90, 100-icy_t95 ocy_t95, 100-icy_t90 ocy_t90, 100-ict_t95 oct_t95, 100-ict_t90 oct_t90, total_mbps from mrqos_sum where ts='%s' ) a JOIN ( select maprule, geoname, netname, sp95_t95 sp95_t95_2w, sp95_t90 sp95_t90_2w, sp75_t95 sp75_t95_2w, sp75_t90 sp75_t90_2w, dp95_t95 dp95_t95_2w, dp95_t90 dp95_t90_2w, dp75_t95 dp75_t95_2w, dp75_t90 dp75_t90_2w, 100-icy_t95 ocy_t95_2w, 100-icy_t90 ocy_t90_2w, 100-ict_t95 oct_t95_2w, 100-ict_t90 oct_t90_2w, ts ts_2w, total_mbps total_mbps_2w from mrqos_sum where ts='%s' ) b ON ( a.maprule = b.maprule AND a.geoname = b.geoname AND a.netname = b.netname ) "
    qry = " set mapred.child.java.opts=-Xmx2000m; use mrqos; set hive.cli.print.header=true; SELECT b.ts_2w, a.*, b.sp95_t95_2w, b.sp95_t90_2w, b.sp75_t95_2w, b.sp75_t90_2w, b.dp95_t95_2w, b.dp95_t90_2w, b.dp75_t95_2w, b.dp75_t90_2w, b.ocy_t95_2w, b.ocy_t90_2w, b.oct_t95_2w, b.oct_t90_2w, b.total_mbps_2w FROM (SELECT ts ts_now, maprule, geoname, netname, sp95_t95, sp95_t90, sp75_t95, sp75_t90, dp95_t95, dp95_t90, dp75_t95, dp75_t90, 100-icy_t95 ocy_t95, 100-icy_t90 ocy_t90, 100-ict_t95 oct_t95, 100-ict_t90 oct_t90, total_mbps from mrqos_sum where ts='%s' ) a JOIN ( select maprule, geoname, netname, sp95_t95 sp95_t95_2w, sp95_t90 sp95_t90_2w, sp75_t95 sp75_t95_2w, sp75_t90 sp75_t90_2w, dp95_t95 dp95_t95_2w, dp95_t90 dp95_t90_2w, dp75_t95 dp75_t95_2w, dp75_t90 dp75_t90_2w, 100-icy_t95 ocy_t95_2w, 100-icy_t90 ocy_t90_2w, 100-ict_t95 oct_t95_2w, 100-ict_t90 oct_t90_2w, ts ts_2w, total_mbps total_mbps_2w from mrqos_sum where ts='%s' ) b ON ( a.maprule = b.maprule AND a.geoname = b.geoname AND a.netname = b.netname ) "
    while (flag == 0) and (count < n_retrial):
        try:
            tic = time.time()

            if 'no load' in id:
                fqry0(qry0, ts1, ts2, 'processed_2wjoin_full.tmp')
            elif '2W' in id:
                fqry(qry, ts1, ts2, 'processed_2wjoin_full_wloads.tmp')
            elif '4W' in id:
                fqry(qry, ts1, ts2, 'processed_4wjoin_full_wloads.tmp')
            elif '3D' in id:
                fqry(qry, ts1, ts2, 'processed_3djoin_full_wloads.tmp')

            flag = 1
            logger.info('%s success with time cost=%s sec' % (id, str(time.time()-tic)))
        except:
            count += 1
            logger.error('%s failed at %s trial.' % (id, str(count)))


def fqry0(qry0, t1, t2, file_output):
    """
    function of query
    :param qry0: query content doing the comparison, no-load
    :param t1: timestamp #1
    :param t2: timestamp #2
    :param file_output: se.
    :return: none
    """
    beeline.bln_e_output(qry0 % (t1, t2), os.path.join(config.mrqos_data, file_output))


def fqry(qry, t1, t2, file_output):
    """
    function of query
    :param qry: query content doing the comparison
    :param t1: timestamp #1
    :param t2: timestamp #2
    :param file_output: se.
    :return: none
    """
    beeline.bln_e_output(qry % (t1, t2), os.path.join(config.mrqos_data, file_output))




if __name__ == '__main__':
    sys.exit(main())