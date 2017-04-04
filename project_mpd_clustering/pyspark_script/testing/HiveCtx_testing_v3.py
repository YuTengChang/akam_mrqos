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
from pyspark.sql import HiveContext, Row
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *

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


def geo_centroid(lat_array, lon_array, load_array):
    geo_data = pd.DataFrame(columns=['lat', 'lon', 'load'])
    geo_data.lat = lat_array
    geo_data.lon = lon_array
    geo_data.load = load_array
    R = 3963.1676 # miles
    geo_data['xi'] = [math.radians(90-x) for x in geo_data.lat]
    geo_data['theta'] = [math.radians(x) for x in geo_data.lon]
    geo_data['X'] = [R*math.sin(x)*math.cos(t) for (x,t) in zip(geo_data.xi, geo_data.theta)]
    geo_data['Y'] = [R*math.sin(x)*math.sin(t) for (x,t) in zip(geo_data.xi, geo_data.theta)]
    geo_data['Z'] = [R*math.cos(x) for x in geo_data.xi]
    X_centroid = np.average(geo_data.X, weights=geo_data.load)
    Y_centroid = np.average(geo_data.Y, weights=geo_data.load)
    Z_centroid = np.average(geo_data.Z, weights=geo_data.load)
    x_var = np.average((geo_data.X-X_centroid)**2, weights=geo_data.load)
    y_var = np.average((geo_data.Y-Y_centroid)**2, weights=geo_data.load)
    z_var = np.average((geo_data.Z-Z_centroid)**2, weights=geo_data.load)
    porsigma = math.sqrt(x_var + y_var + z_var)
    L = math.sqrt(X_centroid*X_centroid + Y_centroid*Y_centroid + Z_centroid*Z_centroid)
    por = L/R
    xi = math.acos(Z_centroid/L)
    theta = math.acos(X_centroid/L/math.sin(xi)) if Y_centroid/math.sin(xi) > 0 else math.acos(X_centroid/L/math.sin(xi))*(-1)
    lat_centroid = 90 - xi/math.radians(1)
    lon_centroid = theta/math.radians(1)
    return [round(lat_centroid,3), round(lon_centroid,3), round(por,4), round(porsigma,2)]


def allowlistPortion(rglist_pr, rglist_pb, allowlistMap, regMap):
    #sol = []
    Lsol = 0
    rg_list = []
# prepare the region list
    if rglist_pr != "NULL":
        rg_list += [int(x) for x in rglist_pr.split(":")]
    if rglist_pb != "NULL":
        rg_list += [int(x) for x in rglist_pb.split(":")]
    if not rg_list:
        return [[0, 1.0, 1.0, 1.0]]
    #
    rg_list = sorted(rg_list)
    rg_dict = {}
    # for each mr.
    for maprule in allowlistMap.keys():
        allowlist = allowlistMap[maprule]
        rg_mr = [regMap[x] for x in rg_list if x in allowlist and x in regMap.keys()]
        # if no region in this maprule, next
        if not rg_mr:
            continue
        # df = pd.DataFrame(rg_mr, columns = ["rg","gbps","gfps","nvips","service","pri"])
        # # add bit capacity ratio
        # if df["gbps"].sum() > 0:
        #     df["b_ratio"] = df["gbps"] / df["gbps"].sum()
        # else:
        #     df["b_ratio"] = 0
        # # add flit capacity ratio
        # if df["gfps"].sum() > 0:
        #     df["f_ratio"] = df["gfps"] / df["gfps"].sum()
        # else:
        #     df["f_ratio"] = 0
        # # add numvip ratio
        # if df["nvips"].sum() > 0:
        #     df["v_ratio"] = df["nvips"] / df["nvips"].sum()
        # else:
        #     df["v_ratio"] = 0
        sums = reduce(lambda a, b: [-1, a[1]+b[1], a[2]+b[2], a[3]+b[3], 'dummy', a[5]+b[5]], rg_mr)
        rgMrS = map(lambda x: [x[0],
                               x[1]/sums[1] if sums[1]>0 else 0,
                               x[2]/sums[2] if sums[2]>0 else 0,
                               x[3]/sums[3] if sums[3]>0 else 0,
                               x[4],
                               x[5]/sums[5] if sums[5]>0 else 0], rg_mr)
        #sol.append([maprule, [[int(x[0])] + x[1:] for x in df[["rg","pri","b_ratio",
        #                                                       "f_ratio","v_ratio"]].values.tolist()]])
        Lsol += 1
        # for rg in df["rg"]:
        #     if rg not in rg_dict:
        #         rg_dict[rg] = df[df["rg"] == rg][["b_ratio","f_ratio","v_ratio"]].values.tolist()[0]
        #     else:
        #         rg_dict[rg] = [x+y for x, y in zip(rg_dict[rg],
        #                                            df[df["rg"] == rg][["b_ratio","f_ratio","v_ratio"]].values.tolist()[0]
        #                                           )]
        for item in rgMrS:
            if item[0] not in rg_dict.keys():
                rg_dict[item[0]] = item[1:4]
            else:
                rg_dict[item[0]] = [x+y for x,y in zip(rg_dict[item[0]],
                                                       item[1:4])]
    # L = len(sol)
    # no region in the dictionary
    if not rg_dict:
        return [[0, 1.0, 1.0, 1.0]]
    # region in the dictionary
    rg_importance = []
    for key, val in rg_dict.items():
        rg_importance.append([int(key)] + [float(x/Lsol) for x in val])
    return rg_importance




day_idx = '20170321'
day_mpg_idx = '20170311'
uuid_idx = 'f7a35bf0-061f-11e7-8e73-300ed5cc4e6c'


# only need in spark-submit
# sc = SparkContext()
hiveCtx = HiveContext(sc)
udf_networkMapping = udf(networkMapping, StringType())

# region info
regInfoRows = hiveCtx.sql('select region, name, ecor, country, asnum, round(peak_bitcap_mbps/1024, 3) as peak_bitcap_gbps, round(peak_flitcap_mfps/1024, 3) as peak_flitcap_gfps, round(latitude, 3) as reg_lat, round(longitude, 3) as reg_lon, numvips, cast(case prp when "private" then 1 else 0 end as int) private, case network when "freeflow" then "W" when "essl" then "S" else "O" end as service from mapper.regioncapday where day=%s and peak_bitcap_mbps is not null and peak_flitcap_mfps is not null' % (day_idx))

# get mpg cluster data
# mpgClusterRows = hiveCtx.sql('select cl_geoname, cl_avgdistance, cl_lat, cl_lon, cl_por, cl_porsigma, cl_mpgload, cl_nmpg, pri_reglist, pub_reglist, mpgaslist, mpgnsiplist from mrqos.mpg_cluster where datestamp=%s and uuid="%s"' % (day_mpg_idx, uuid_idx))
mpgClusterRows = hiveCtx.sql('select cl_geoname, cl_mpgload, cl_nmpg, pri_reglist, pub_reglist from mrqos.mpg_cluster where datestamp=%s and uuid="%s"' % (day_mpg_idx, uuid_idx))

# allowlist data
allowlistRows = hiveCtx.sql('select maprule, collect_set(region) as allowlist from mrqos.mcm_region_pref where datestamp=%s group by maprule' % (day_idx))

# turn region info as a local map (dictionary)
regMap = regInfoRows.filter((col("service") == "S") | (col("service") == "W"))\
    .map(lambda x: (x.region, [x.region, x.peak_bitcap_gbps, x.peak_flitcap_gfps, x.numvips, x.service, x.private]))\
    .collectAsMap()

# broadcast the region dictionary
sc.broadcast(regMap)

allowlistMap = allowlistRows.map(lambda x: (x.maprule, x.allowlist)).collectAsMap()
sc.broadcast(allowlistMap)

mrW = [1,2948,4662,4667,4668,4669,332,290,2903,2905,2910,2923,121,122]
allowlistMapW = {key:value for key, value in allowlistMap.items()
                 if key in mrW}
allowlistMapS = {key:value for key, value in allowlistMap.items()
                 if key not in mrW}

sc.broadcast(allowlistMapW)
sc.broadcast(allowlistMapS)

col = mpgClusterRows.columns #+ ['rg_imp']

#schema = [f.dataType for f in mpgClusterRows.schema.fields]
#schema += [StructType]
#
# col_new = col[:8] + col[10:] + ["rg_idx", "rg_Bratio", "rg_Fratio", "rg_Vratio",
#                                 "rg_Dw_Bratio", "rg_Dw_Fratio", "rg_Dw_Vratio", "rg_mpgCluster_count"]
#
# schema = StructType([StructField("cl_geoname", StringType(), True),
#                      StructField("cl_avgdistance", FloatType(), True),
#                      StructField("cl_lat", FloatType(), True),
#                      StructField("cl_lon", FloatType(), True),
#                      StructField("cl_por", FloatType(), True),
#                      StructField("cl_porsigma", FloatType(), True),
#                      StructField("cl_mpgload", FloatType(), True),
#                      StructField("cl_nmpg", IntegerType(), True),
#                      StructField("mpgaslist", StringType(), True),
#                      StructField("mpgnsiplist", StringType(), True),
#                      StructField("rg_idx", IntegerType(), True),
#                      StructField("rg_Bratio", FloatType(), True),
#                      StructField("rg_Fratio", FloatType(), True),
#                      StructField("rg_Vratio", FloatType(), True),
#                      StructField("rg_Dw_Bratio", FloatType(), True),
#                      StructField("rg_Dw_Fratio", FloatType(), True),
#                      StructField("rg_Dw_Vratio", FloatType(), True),
#                      StructField("rg_mpgCluster_count", IntegerType(), True),
#                      ])
# mpg_Rg_table = mpgClusterRows.map(lambda x: [x, allowlistPortion(x.pri_reglist, x.pub_reglist, allowlistMap, regMap)])\
#     .flatMapValues(lambda x: x)\
#     .map(lambda x: [x[0][0],
#                     round(x[0][1],3), # avg distance
#                     round(x[0][2],3), # cl_lat
#                     round(x[0][3],3), # cl_lon
#                     round(x[0][4],5), # cl_por
#                     round(x[0][5],3), # cl_porsigma
#                     x[0][6], # cl_mpgload
#                     x[0][7], # cl_nmpg
#                     x[0][10], # mpgaslist
#                     x[0][11], # mpgnslist
#                     x[1][0], # rg_idx
#                     x[1][1], # rg_Bratio
#                     x[1][2], # rg_Fratio
#                     x[1][3], # rg_Vratio
#                     x[1][1] * x[0][6], # rg_Dw_Bratio (demand-weighted bit ratio)
#                     x[1][2] * x[0][6], # rg_Dw_Fratio
#                     x[1][3] * x[0][6], # rg_Dw_Vratio
#                     1, # rg_mpg_count
#                     ])\
#     .toDF(schema = schema)


mpg_Rg_table = mpgClusterRows.map(lambda x: [x, allowlistPortion(x.pri_reglist, x.pub_reglist, allowlistMapS, regMap)])\
    .cache()

rg_importance = mpg_Rg_table.flatMapValues(lambda x: x)\
    .cache()\
    .map(lambda x: (x[1][0], # rg_idx
                    [[x[0][0]], # [cl_geo]
                    x[0][1], # cl_mpgload
                    x[0][2], # cl_nmpg
                    x[1][1], # rg_Bratio
                    x[1][2], # rg_Fratio
                    x[1][3], # rg_Vratio
                    x[1][1] * x[0][1], # rg_Dw_Bratio (demand-weighted bit ratio)
                    x[1][2] * x[0][1], # rg_Dw_Fratio
                    x[1][3] * x[0][1], # rg_Dw_Vratio
                    1, # rg_mpg_count
                    ]))\
    .reduceByKey(lambda a, b: [x+y for x,y in zip(a,b)])\
    .map(lambda x: [x[0], ":".join(list(set(x[1][0]))), x[1][1], x[1][2], x[1][3],
                    x[1][4], x[1][5], x[1][6], x[1][7], x[1][8], x[1][9]])

rg_imp_local = rg_importance.collect()

rg_imp_tb = pd.DataFrame(columns=['region','geo_list','mpg_load','mpg_num','Bratio','Fratio','Vratio',
                                  'dw_Bratio','dw_Fratio','dw_Vratio','Gmpg_num'])

for item in rg_imp_local:
    rg_imp_tb.loc[len(rg_imp_tb)] = item

# checking the freeflow maps
mpg_Rg_table = mpgClusterRows.map(lambda x: [x, allowlistPortion(x.pri_reglist, x.pub_reglist, allowlistMapW, regMap)])\
    .cache()

rg_importance = mpg_Rg_table.flatMapValues(lambda x: x)\
    .cache()\
    .map(lambda x: (x[1][0], # rg_idx
                    [[x[0][0]], # [cl_geo]
                    x[0][1], # cl_mpgload
                    x[0][2], # cl_nmpg
                    x[1][1], # rg_Bratio
                    x[1][2], # rg_Fratio
                    x[1][3], # rg_Vratio
                    x[1][1] * x[0][1], # rg_Dw_Bratio (demand-weighted bit ratio)
                    x[1][2] * x[0][1], # rg_Dw_Fratio
                    x[1][3] * x[0][1], # rg_Dw_Vratio
                    1, # rg_mpg_count
                    ]))\
    .reduceByKey(lambda a, b: [x+y for x,y in zip(a,b)])\
    .map(lambda x: [x[0], ":".join(list(set(x[1][0]))), x[1][1], x[1][2], x[1][3],
                    x[1][4], x[1][5], x[1][6], x[1][7], x[1][8], x[1][9]])

rg_imp_local = rg_importance.collect()

for item in rg_imp_local:
    rg_imp_tb.loc[len(rg_imp_tb)] = item

rg_imp_tb["geo_list"] = [":".join(sorted(list(set(x.split(":"))))) for x in rg_imp_tb.geo_list]

rg_imp_tb.to_csv("/home/testgrp/reg_importance.csv",)