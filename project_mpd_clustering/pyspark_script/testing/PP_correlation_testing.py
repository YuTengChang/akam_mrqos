import sys, os

sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import time
import YT_Timeout as ytt
import configurations.config as config
import configurations.hdfsutil as hdfsutil
import configurations.beeline as beeline
import logging
from pyspark import SparkContext
from pyspark.sql import HiveContext

import math
import pandas as pd
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
    return [lat1, lon1, lat2, lon2, R*c]

datestamp = "20160830"
hourstamp = "14"

timenow = int(time.time())
timestart = timenow - 10800

ppinfo = ''' select ppip, asnum ppas, latitude pp_lat, longitude pp_lon, city pp_city, state pp_state, country pp_country, continent pp_cont from mrqos.ppinfo where datestamp=%s and hour=%s ''' % (str(datestamp), str(hourstamp))

ppreply = ''' select a.*, b.rg_lat, b.rg_lon from (select ppip, region, ecor, latency, loss from perftmi.ppreply where ts>%s) a left outer join (select region, round(latitude, 4) rg_lat, round(longitude, 4) rg_lon from mapper.barebones where day=%s) b on a.region=b.region ''' % (str(timestart),
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
    .reduceByKey(lambda a, b: [a[2], # ppas
                               a[3], # pp_city
                               a[4], # pp_state
                               a[5], # pp_country
                               a[6], # pp_continent
                               a[0], # pp_lat
                               a[1], # pp_lon
                               a[7]+b[7], # [pp_rg_distance]
                               a[8]+b[8], # [pp_latency]
                               a[9]+b[9], # [pp_loss]
                               a[10]+b[10] # ppreply_count
                              ])


pp_raw.first()

