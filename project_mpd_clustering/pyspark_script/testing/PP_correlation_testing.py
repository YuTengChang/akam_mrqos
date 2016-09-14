import sys, os
sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import time
from pyspark import SparkContext
from pyspark.sql import HiveContext
import configurations.config as config
import math
import pandas as pd
import numpy as np
import scipy
from scipy.stats.stats import pearsonr
import logging


def geodesic_distance(lat1, lon1, lat2, lon2):
    R = 3963.1676
    lat1r = math.radians(lat1)
    lat2r = math.radians(lat2)
    lon1r = math.radians(lon1)
    lon2r = math.radians(lon2)
    dlat = abs(lat2r - lat1r)/2
    dlon = abs(lon2r - lon1r)/2
    a = math.pow(math.sin(dlat), 2) + math.cos(lat2r) * math.cos(lat1r) * math.pow(math.sin(dlon), 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return [lat1, lon1, lat2, lon2, R*c]


def metric_relationship(list1, list2, list3):
    '''
    We are evaluating the relationship between list1 and list2
    :param list1: input of list1 (x): distance
    :param list2: input of list2 (y): latency
    :param list3: input of list3 (z): loss
    :return:
    n_valid_pp: # of valid pp (pingable pp)
    valid_ratio: ratio of valid pp / all pp records
    pr, prp: correlation-coefficient(x,y)
    ar, br: linear regression where y = ar * x + br
    err: the mean squared error of y: y - y_estimate
    err1_max: the max error of abs(y - y_estimate)
    list1r: output of valid list1 (x): distance
    list2r: output of valid list2 (y): latency
    list3m: output of valid list3 (z): loss
    '''
    #pearsonr = np.corrcoef(list1, list2)[0][1]

    in_list = [i for i,x in enumerate(list2) if x < 10000]
    list2m = [list2[i] for i in in_list]
    list1m = [list1[i] for i in in_list]
    list3m = [list3[i] for i in in_list]
    n_valid_pp = len(list1m)
    valid_ratio = round(100.0*n_valid_pp/len(list1), 2)
    if n_valid_pp > 10:
        (pr, prp) = pearsonr(list1m, list2m)
        (ar, br) = scipy.polyfit(list1m, list2m, 1)
        yr = scipy.polyval([ar,br],list1m)
        err = math.sqrt(sum((yr-list2m)**2)/len(yr))
        err1_max = max(abs(yr-list2m))
    else:
        pr = 0
        prp = 1
        ar = 0
        br = 0
        err = 0
        err1_max = 0
    list1r = [round(x,3) for x in list1m]
    list2r = [round(x,3) for x in list2m]
    return [n_valid_pp, valid_ratio, pr, prp, round(ar, 3), round(br, 3), err, round(err1_max, 3), list1r, list2r, list3m]


def toCSVLine(data):
  return '\t'.join(str(d) for d in data)


def main():
    # set up the logger
    logging.basicConfig(filename=os.path.join(config.mrqos_logging, 'pp_data.log'),
                            level=logging.INFO,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    datestamp = "20160913"
    hourstamp = "12"

    logger.info('processing the date of data: %s at hour instance: %s' % (datestamp, hourstamp))

    timenow = int(time.time())
    timestart = timenow - 10800

    ppinfo = ''' select ppip, asnum ppas, latitude pp_lat, longitude pp_lon, city pp_city, state pp_state, country pp_country, continent pp_cont from mrqos.ppinfo where datestamp=%s and hour=%s ''' % (str(datestamp), str(hourstamp))

    ppreply = ''' select a.ppip, a.region, a.ecor, a.latency, a.loss, b.rg_lat, b.rg_lon from (select ppip, region, ecor, latency, loss from perftmi.ppreply where ts>%s) a left outer join (select region, round(latitude, 4) rg_lat, round(longitude, 4) rg_lon from mapper.barebones where day=%s) b on a.region=b.region ''' % (str(timestart),
                                                                                                                                                                                                                                                                                            str(datestamp))

    sc = SparkContext()
    hiveCtx = HiveContext(sc)

    ppreply_data = hiveCtx.sql(ppreply)
    ppinfo_data = hiveCtx.sql(ppinfo)

    ppinfo1 = ppinfo_data.map(lambda x: (x[0], [x[1], # ppas
                                                x[2], # pp_lat
                                                x[3], # pp_lon
                                                x[4], # pp_city
                                                x[5], # pp_state
                                                x[6], # pp_country
                                                x[7]]) ) # pp_continent

    ppreply1 = ppreply_data.map(lambda x: (x[0], [x[1], # region
                                                  x[2], # ecor
                                                  x[3], # latency
                                                  x[4], # loss
                                                  x[5], # rg_lat
                                                  x[6]]) ) # rg_lon

    pp_raw = ppinfo1.join(ppreply1).map(lambda x: (x[0], # ppip
                                                  [geodesic_distance( x[1][0][1], # pp_lat
                                                                      x[1][0][2], # pp_lon
                                                                      x[1][1][4], # rg_lat
                                                                      x[1][1][5] ), # rg_lon
                                                  x[1][0][0], # ppas
                                                  x[1][0][3], # pp_city
                                                  x[1][0][4], # pp_state
                                                  x[1][0][5], # pp_country
                                                  x[1][0][6], # pp_continent
                                                  x[1][1][0], # region
                                                  x[1][1][1], # ecor
                                                  x[1][1][2], # latency
                                                  x[1][1][3] # loss
                                                  ]) )\
        .map(lambda x: (x[0],
                        [x[1][0][0], # pp_lat
                         x[1][0][1], # pp_lon
                         x[1][1], # ppas
                         x[1][2], # pp_city
                         x[1][3], # pp_state
                         x[1][4], # pp_country
                         x[1][5], # pp_continent
                         [x[1][0][4]], # [pp_rg_distance]
                         [x[1][8]], # [pp_latency]
                         [x[1][9]], # [pp_loss]
                         1 # record_count
                        ]) )\
        .reduceByKey(lambda a, b: [a[2], # country
                                   a[3], # continent
                                   a[4], # latitude
                                   a[5], # longitude
                                   a[6], # asnum
                                   a[0], # city
                                   a[1], # state
                                   a[7]+b[7], # [pp_rg_distance]
                                   a[8]+b[8], # [pp_latency]
                                   a[9]+b[9], # [pp_loss]
                                   a[10]+b[10] # ppreply_count
                                  ])

    #pp_raw.take(5)

    # remove raw distance and latency and loss records, replaced by valid (filtered) ones
    # x[1][7], # [pp_rg_distance]
    # x[1][8], # [pp_latency]
    # x[1][9], # [pp_lost]
    # metric_relationship returns: [n_valid_pp, valid_ratio, pr, prp, round(ar, 3), round(br, 3), err, round(err1_max, 3), list1r, list2r, list3m]
    pp_char = pp_raw.map(lambda x: [x[0], # ppip
                                    x[1][4], # pp_asn
                                    x[1][5], # pp_city
                                    x[1][6], # pp_state
                                    x[1][0], # pp_country
                                    x[1][1], # pp_continent
                                    x[1][2], # pp_lat
                                    x[1][3], # pp_lon
                                    x[1][10], # pp_num_record
                                    metric_relationship(x[1][7], x[1][8], x[1][9])
                                    ])\
            .map(lambda x: [x[0], # ppip
                            x[1], # ppas
                            x[2], # city
                            x[3], # state
                            x[4], # country
                            x[5], # continent
                            x[6], # lat
                            x[7], # lon
                            x[8], # n_record_pp
                            x[9][0], # n_valid_pp
                            x[9][1], # valid_ratio (100%)
                            x[9][2], # pearson r
                            x[9][3], # pearson r pv
                            x[9][4], # round(ar,3)
                            x[9][5], # round(br,3)
                            x[9][6], # err
                            x[9][7], # round(err1_max,3)
                            round(sum(x[9][8])/float(x[9][0]), 3), # mean(distance)
                            round(sum(x[9][9])/float(x[9][0]), 3), # mean(latency)
                            round(sum(x[9][10])/float(x[9][0]), 3), # mean(lost)
                            round(np.percentile([int(y) for y in x[9][8]], 95), 3), # p95(distance)
                            round(np.percentile([int(y) for y in x[9][9]], 95), 3), # p95(latency)
                            round(np.percentile([int(y) for y in x[9][10]], 95), 3), # p95(loss)
                            round(np.percentile([int(y) for y in x[9][8]], 50), 3), # p50(distance)
                            round(np.percentile([int(y) for y in x[9][9]], 50), 3), # p50(latency)
                            round(np.percentile([int(y) for y in x[9][10]], 50), 3), # p50(loss)
                            ':'.join([str(y) for y in x[9][8]]), # [distance]
                            ':'.join([str(y) for y in x[9][9]]) # [latency]
                            ])


    logger.info('now the final collect begins.')
    #pp_char_all = pp_char.collect()
    pp_char_all = pp_char.map(toCSVLine)
    pp_char_all.saveAsTextFile('/ghostcache/hadoop/data/MRQOS/sandbox/pp_test00') #,
    #                           compressionCodecClass="org.apache.hadoop.io.compress.SnappyCodec")
    logger.info('now the final collect ends.')

    pp_charateristics = pd.DataFrame(columns=['ppip',
                                               'ppas',
                                               'city',
                                               'state',
                                               'country',
                                               'continent',
                                               'pp_lat',
                                               'pp_lon',
                                               'ppreply_count',
                                               'p50_latency',
                                               'mean_latency',
                                               'p95_latency',
                                               'p50_loss',
                                               'mean_loss',
                                               'p95_loss',
                                               'pearsonr',
                                               'pearsonr_pv',
                                               'ar',
                                               'br',
                                               'err',
                                               'err_max',
                                               'distance',
                                               'latency',
                                               'loss'])

    #for item in range(len(pp_char_all)):
    #    temp = pp_char_all[item]
    #    pp_charateristics.loc[item] = temp # the above should be temp[1][0] for the mpglist

    data_folder = '/home/testgrp/'
    filename = 'ppchar.%s.%s.csv' % (datestamp, hourstamp)
    fileDestination = os.path.join(data_folder, filename)
    #pp_charateristics.to_csv(fileDestination,sep=',', index=False, header=False)

if __name__ == '__main__':
    sys.exit(main())