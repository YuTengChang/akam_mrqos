import sys, os

sys.path.append('/home/testgrp/MRQOS/')
import datetime
import configurations.config as config
import configurations.beeline as beeline
import logging
from pyspark import SparkContext
from pyspark.sql import HiveContext, Row
from pyspark.sql import functions as F

import math
import numpy as np


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
    return R * c


def geodesic_distance_weighted(lat1, lon1, lat2, lon2, weight):
    R = 3963.1676
    lat1r = math.radians(lat1)
    lat2r = math.radians(lat2)
    lon1r = math.radians(lon1)
    lon2r = math.radians(lon2)
    dlat = abs(lat2r - lat1r)/2
    dlon = abs(lon2r - lon1r)/2
    a = math.pow(math.sin(dlat), 2) + math.cos(lat2r) * math.cos(lat1r) * math.pow(math.sin(dlon), 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c * weight


def toCSVLine(data):
    return '\t'.join(str(d) for d in data)


def computeEntropyPMF(data_list):
    ar = np.array(data_list)
    return float(-1 * np.sum(np.log(ar) * ar))


def main():
    # set up the logger
    logging.basicConfig(filename=os.path.join(config.mrqos_logging, 'ra_summary.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)
    # table nsjoin (day, uuid)
    # table mapmon (day, uuid)
    datenow = str(datetime.date.today()-datetime.timedelta(1))
    day_idx = datenow[0:4]+datenow[5:7]+datenow[8:10]
    uuid_list = [x.split('=')[-1] for x in beeline.show_partitions('mrqos.mapmon_sum').split('\n') if day_idx in x]
    sc = SparkContext()
    hiveCtx = HiveContext(sc)
    post_partition_n = 1000

    for uuid_idx in uuid_list:
        # ns_ip, demand, asnum ns_asnum, ns_country, ns_continent, ns_lat, ns_lon, ns_mpgid, mpgload
        nsjoin_query = """ select ns_ip, demand, asnum ns_asnum, country_code ns_country, continent ns_continent, round(latitude,3) ns_lat, round(longitude,3) ns_lon, mpgid ns_mpgid, mpgload from mapper.nsjoin where day={} and mpd_uuid='{}' and longitude is not NULL and latitude is not NULL and demand > 1""".format(day_idx,
                                                                                                                                                                                                                                                                                                                            uuid_idx)

        # mpgid, mrid, mpg_type, region, link, min_s, max_s, min_r, max_r, ping, local, cont_fb, mpd_dftime, ecor, continent, country, latitude, longitude, prp
        mapmon_query = """ select mpgid, mrid, mpg_type, region, link, min_s, max_s, min_r, max_r, ping, local, cont_fb, mpd_dftime, ecor, continent, country, latitude, longitude, prp from mrqos.mapmon_sum where day={} and mpd_uuid='{}' and longitude is not NULL and latitude is not NULL""".format(day_idx,
                                                                                                                                                                                                                                                                                                          uuid_idx)
        logger.info('Processing data in day=%s, uuid=%s' % (day_idx, uuid_idx))

        nsjoin = hiveCtx.sql(nsjoin_query)
        nsjoin_rows = nsjoin.repartition(post_partition_n).cache()
        data = hiveCtx.sql(mapmon_query)
        data_rows = data.repartition(post_partition_n).cache()

        col = ['mpgid', 'mrid', 'mpg_type', 'region', 'link', 'min_s', 'max_s', 'min_r', 'max_r',
               'ping', 'local', 'cont_fb', 'mpd_dftime', 'ecor', 'continent', 'country', 'latitude', 'longitude', 'prp',
               'ns_ip', 'demand', 'ns_asnum', 'ns_country', 'ns_continent', 'ns_lat', 'ns_lon', 'mpgload']

        cols_appended = ['nsip', 'mrid', 'ns_demand', 'ns_asnum', 'ns_country', 'ns_continent', 'ns_lat', 'ns_lon',
                         'mpgid', 'mpg_type', 'mpg_load', 'regions', 'region_links', 'dftime_ratio', 'ecors',
                         'list_min_s', 'list_max_s', 'list_min_r', 'list_max_r',
                         'region_lats', 'region_lons', 'min_s', 'max_s', 'min_r', 'max_r', 'ping_ratio', 'local_ratio',
                         'cont_fb_ratio', 'in_cont_ratio', 'in_country_ratio', 'private_ratio', 'avg_distance',
                         'num_region_mapped', 'mapping_entropy', 'sum_dftime']

        df = nsjoin_rows.join(data_rows, data_rows.mpgid == nsjoin_rows.ns_mpgid, 'inner')[col].cache()
        row1 = data_rows.agg(F.max(data_rows.mpd_dftime)).collect()[0]
        max_dftime = row1[0]

        df2 = df.map(lambda x: x + Row(geodesic_distance_weighted(x.ns_lat,
                                                                  x.ns_lon,
                                                                  x.latitude,
                                                                  x.longitude,
                                                                  x.mpd_dftime)))\
                .map(lambda x: ((   x[19], # nsip
                                    x[20], # demand
                                    x[21], # ns_asnum
                                    x[22], # ns_country
                                    x[23], # ns_continent
                                    round(x[24], 3), # ns_lat & ns_lon
                                    round(x[25], 3),
                                    x[0], # mpgid
                                    x[1], # mrid
                                    x[2], # mpg type
                                    x[26], # mpg load
                                    ),
                               [   [int(x[3])], # region
                                   [str(int(x[3])) + "_" + str(int(x[4]))], # region_link
                                   x[5]/max_dftime, # min_s
                                   x[6]/max_dftime, # max_s
                                   x[7]/max_dftime, # min_r
                                   x[8]/max_dftime, # max_r
                                   x[9]/max_dftime, # ping ratio
                                   x[10]/max_dftime, # local ratio
                                   x[11]/max_dftime, # cont_fb ratio
                                   [round(x[12]/max_dftime, 3)], # mpd_dftime/max_dftime (time ratio)
                                   [int(x[13])], # ecor
                                   x[12]/max_dftime * [0, 1][x[14] == x[23]], # mapping in-continent ratio
                                   x[12]/max_dftime * [0, 1][x[15] == x[22]], # mapping in-country ratio
                                   [round(x[16], 3)], # lat
                                   [round(x[17], 3)], # lon
                                   x[18]/max_dftime, # prp
                                   x[27]/max_dftime, # w_distance
                                   x[12],
                                   [round(x[5]/x[12], 2)], # min_s list
                                   [round(x[6]/x[12], 2)], # max_s list
                                   [round(x[7]/x[12], 2)], # min_r list
                                   [round(x[8]/x[12], 2)], # max_r list
                               ]))\
                .reduceByKey(lambda a, b: [x+y for x, y in zip(a, b)])\
                .map(lambda x: [x[0][0], # nsip
                                x[0][8], # mrid
                                x[0][1], # demand
                                x[0][2], # ns_asnum
                                x[0][3], # ns_country
                                x[0][4], # ns_continent
                                x[0][5], # ns_lat
                                x[0][6], # ns_lon
                                x[0][7], # mpgid
                                x[0][9], # mpg type
                                x[0][10], # mpg load
                                x[1][0], # list of region
                                x[1][1], # list of region_link
                                [round(100 * float(y), 2) for y in x[1][9]], # list of covered_record ratio
                                x[1][10], # list of ecor
                                x[1][13], # list of region lat
                                x[1][14], # list of region lon
                                round(x[1][2] * max_dftime / x[1][17], 3) if x[1][17] > 0 else -1, # min_s
                                round(x[1][3] * max_dftime / x[1][17], 3) if x[1][17] > 0 else -1, # max_s
                                round(x[1][4] * max_dftime / x[1][17], 3) if x[1][17] > 0 else -1, # min_r
                                round(x[1][5] * max_dftime / x[1][17], 3) if x[1][17] > 0 else -1, # max_r
                                round(100 * x[1][6] * max_dftime / x[1][17], 2) if x[1][17] > 0 else -1, # ping ratio
                                round(100 * x[1][7] * max_dftime / x[1][17], 2) if x[1][17] > 0 else -1, # local ratio
                                round(100 * x[1][8] * max_dftime / x[1][17], 2) if x[1][17] > 0 else -1, # cont_fb ratio
                                round(100 * x[1][11] * max_dftime / x[1][17], 2) if x[1][17] > 0 else -1, # mapping in-continent ratio
                                round(100 * x[1][12] * max_dftime / x[1][17], 2) if x[1][17] > 0 else -1, # mapping in-country ratio
                                round(100 * x[1][15] * max_dftime / x[1][17], 2) if x[1][17] > 0 else -1, # private ratio
                                round(x[1][16] * max_dftime / x[1][17], 2) if x[1][17] > 0 else -1, # w_distance
                                round(x[1][17], 3), # summation of covered dftime
                                x[1][18], # list of min_s
                                x[1][19], # list of max_s
                                x[1][20], # list of min_r
                                x[1][21], # list of max_r
                                len(x[1][9]), # number of different regions mapped
                                round(computeEntropyPMF(x[1][9]), 6), # entropy of the region assignments
                                ])\
                .map(lambda x: x + [[i[0] for i in sorted(enumerate([float(y) for y in x[13]]), key=lambda z:z[1], reverse=True)]])\
                .map(lambda x: x[:11] + [':'.join([str(x[11][i]) for i in x[35]]), # list of region
                                         ':'.join([str(x[12][i]) for i in x[35]]), # list of region_link
                                         ':'.join([str(x[13][i]) for i in x[35]]), # list of covered_record ratio
                                         ':'.join([str(x[14][i]) for i in x[35]]), # list of ecor
                                         ':'.join([str(x[29][i]) for i in x[35]]), # list of min_s
                                         ':'.join([str(x[30][i]) for i in x[35]]), # list of max_s
                                         ':'.join([str(x[31][i]) for i in x[35]]), # list of min_r
                                         ':'.join([str(x[32][i]) for i in x[35]]), # list of max_r
                                         ':'.join([str(x[15][i]) for i in x[35]]), # list of region lat
                                         ':'.join([str(x[16][i]) for i in x[35]]), # list of region lon
                                         ] + x[17:28] + x[33:35] + [x[28]])\
                .toDF(cols_appended).cache()

        df_all = df2.map(lambda x: toCSVLine(x))
        logger.info('writing into HDFS')
        df_all.saveAsTextFile('/ghostcache/hadoop/data/MRQOS/mrqos_mapmon_stats/datestamp={}/uuid={}'.format(day_idx,
                                                                                                             uuid_idx))
        logger.info('updating Hive table: mrqos_mapmon_stats')
        beeline.add_partitions("mrqos.mrqos_mapmon_stats","datestamp='{}',uuid='{}'".format(day_idx,
                                                                                            uuid_idx))


if __name__ == '__main__':
    sys.exit(main())



