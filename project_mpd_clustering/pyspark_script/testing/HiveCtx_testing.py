from pyspark.sql import HiveContext
import math


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

day_idx = '20160427'
uuid_idx = '318c55e6-0c67-11e6-bbfd-300ed5cc4e6c'

getting_mappoint_data = ''' select b1.mpgid mpgid, b1.lat lat, b1.lon lon, b1.country country, b1.mpgload mpgload, b1.allowed_private_regions allowed_private_regions, b2.asnum asnum, b2.ip ip from (select mpgid, lat, lon, country, mpgload, allowed_private_regions from mapper.mappoints where day=%s and uuid="%s" and lat is not NULL and lon is not NULL and ghostonly=0 ) b1 left outer join (select collect_set(ip) ip, collect_set(asnum) asnum, mpgid from (select ip, mpd_uuid, mpgid, asnum, day from mapper.nsassoc where day=%s and mpd_uuid="%s") a group by mpgid) b2 on b2.mpgid=b1.mpgid ''' % (day_idx, uuid_idx, day_idx, uuid_idx)
geo_total_cap_query = ''' select * from (select country, sum(region_capacity) geo_total_cap, sum(numvips) geo_total_numvips, service from (select region, country, region_capacity/1000000 region_capacity, prp, case ghost_services when "W" then "W" when "JW" then "W" when "S" then "S" when "KS" then "S" else "O" end service, numvips, day from mapper.barebones where prp="private" and day=%s) a group by country, service) b where service in ("W","S") ''' % day_idx


hiveCtx = HiveContext(sc)

rows = hiveCtx.sql(getting_mappoint_data)

regInfoRows = hiveCtx.sql('select * from mapper.barebones where day=%s and latitude is not NULL and longitude is not NULL and ghost_services in ("W","S","KS","JW")' % day_idx)

geo_total_cap = hiveCtx.sql(geo_total_cap_query)


# rdd format: [regionid, [mpgid, mpg-lat, mpg-lon, mpg-country, mpg-load, mpg-asnum, mpg-nsip]]
region_mpginfo_pair = rows.map(lambda x: [[x.mpgid,
                                           x.lat,
                                           x.lon,
                                           x.country,
                                           x.mpgload,
                                           x.asnum,
                                           x.ip], x.allowed_private_regions])\
                            .flatMapValues(lambda x: x).map(lambda x: [x[1], x[0]])

#region_mpginfo_pair.first()

# rdd format: [regionid, [reg-lat, reg-lon, reg-capacity, reg-country, reg-numvips, reg-service]]
region_latlon = regInfoRows.map(lambda x: [x.region, [x.latitude,
                                                      x.longitude,
                                                      x.region_capacity,
                                                      x.country,
                                                      x.numvips,
                                                      'W' if x.ghost_services=='W' or x.ghost_services=='JW' else 'S']])

# perform the join into tuple of (K, (V1, V2):
# (regionid, ([mpgid, mpg-lat, mpg-lon, mpg-country, mpg-load], [reg-lat, reg-lon, reg-cap, reg-country, reg-numvips, reg-service]))
# rdd  = (mpgid, regionid, [lat1, lon1, lat2, lon2, distance],
#               reg-cap, reg-country, reg-numvips, reg-services,
#               mpg-country, mpg-load, mpg-asnum, mpg-nsip)
mpgid_reg_geo = region_mpginfo_pair.join(region_latlon).map(lambda x: [x[1][0][0],
                                                                       x[0],
                                                                       geodesic_distance(x[1][0][1],
                                                                                         x[1][0][2],
                                                                                         x[1][1][0],
                                                                                         x[1][1][1]),
                                                                       round(float(x[1][1][2])/1000000.0, 3),
                                                                       x[1][1][3], # reg-country
                                                                       x[1][1][4], # reg-numvips
                                                                       x[1][1][5], # reg-services
                                                                       x[1][0][3],
                                                                       x[1][0][4],
                                                                       x[1][0][5],
                                                                       x[1][0][6]])

mpgid_reg_geo.take(5)

# filtering on mapping distance < 500 miles
# rdd format = (mpgid, [[regionid], distance, [capacity-w, capacity-s], numvips, 1, mpg-country, mpg-load, mpg-asnum, mpg-nsip])
mpgid_reg_distance = mpgid_reg_geo.filter(lambda x: x[2][4] < 500)\
    .filter(lambda x: x[4] == x[7])\
    .filter(lambda x: x[3] > 1000)\
    .map(lambda x: (x[0], [[x[1]], x[2][4], [x[3], 0] if x[6]=='W' else [0, x[3]], x[5], 1, x[7], x[8], x[9], x[10]]))

mpgid_reg_distance.first()

# group by mpgid
# rdd format = (mpgid, [[reg-list],
#                       avg_distance,
#                       total_cap freeflow,
#                       total_cap essl,
#                       total num vips,
#                       rg_count,
#                       mpg-country,
#                       mpg-load,
#                       [mpg-asnum],
#                       [mpg-nsip])
mpgid_reglist_avgDistance_capacity_nReg = mpgid_reg_distance\
    .reduceByKey(lambda a, b: [a[0]+b[0], a[1]+b[1], [a[2][0]+b[2][0], a[2][1]+b[2][1]], a[3]+b[3], a[4]+b[4], a[5], a[6], a[7], a[8]])\
    .map(lambda x: (x[0], [sorted(x[1][0]), # region_list
                           round(x[1][1]/x[1][4], 2), # avg distance
                           round(x[1][2][0], 2), # total capacity - w
                           round(x[1][2][1], 2), # total capacity - s
                           x[1][3], # numvips
                           x[1][4], # total region count
                           x[1][5], # mpg country
                           x[1][6], # mpg load
                           x[1][7], # mpg asnum
                           x[1][8]])) # mpg nsip

total_mpg_with_region = mpgid_reglist_avgDistance_capacity_nReg.count()

# rdd format = ((reg-list), [[mpg-list], avg_distance, total_cap_w, total_cap_s, total_numvips,
#                           reg-count, cluster_country, mpg-load, mpg-count])
reglist_mpgid_avgDistance_capacity_nReg_country = mpgid_reglist_avgDistance_capacity_nReg\
    .map(lambda x: (tuple(x[1][0]), [[x[0]], # mpgid list
                                      x[1][1], # avg_distance
                                      x[1][2], # region total capacity freeflow
                                      x[1][3], # region total capacity essl
                                      x[1][4], # total num vips
                                      x[1][5], # total region count
                                      [x[1][6]], # mpg country list
                                      x[1][7], # mpg load
                                      1, # mpg-count
                                      x[1][8] if x[1][8] else [], # [mpg-asnum]
                                      x[1][9] if x[1][9] else []# [mpg-nsip]
                                     ]))\
    .reduceByKey(lambda a, b: [a[0]+b[0],
                               a[1],
                               a[2],
                               a[3],
                               a[4],
                               a[5],
                               a[6]+b[6],
                               a[7]+b[7],
                               a[8]+b[8],
                               a[9]+b[9],
                               a[10]+b[10]])\
    .map(lambda x: (x[0], [sorted(x[1][0]), # mpgid list
                           x[1][1], # avg_distance
                           x[1][2], # reg-cap-w
                           x[1][3], # reg-cap-s
                           x[1][4], # numvips
                           x[1][5], # reg-count
                           [str(y) for y in sorted(list(set(x[1][6])))], # mpg-country list
                           x[1][7], # mpg-load
                           x[1][8], # mpg-count
                           [str(y) for y in sorted(list(set(x[1][9])))], # [mpg-asnum]
                           [str(y) for y in sorted(list(set(x[1][10])))] ]))\
    .map(lambda x: [':'.join([str(y) for y in list(x[1][6])]), # [mpg-country list]
                    x[1][1], # avg_distance
                    x[1][2], # reg-cap-w
                    x[1][3], # reg-cap-s
                    x[1][4], # numvips
                    x[1][5], # reg-count
                    x[1][7], # mpg-load
                    x[1][8], # mpg-count
                    ':'.join([str(y) for y in x[0]]), # [region-list]
                    ':'.join([str(y) for y in list(x[1][0])]), # [mpg-list]
                    ':'.join([str(y) for y in x[1][9]]) if len(x[1][9])>0 else 'NULL', # [mpg-asnum]
                    ':'.join([str(y) for y in x[1][10]]) if len(x[1][10])>0 else 'NULL' ]) # [mpg-nsip]

reglist_mpgid_avgDistance_capacity_nReg_country.take(5)

# (mpg-country-list, [cluster-count, mpg-load])
geo_clusterN_totalMpgLoad = reglist_mpgid_avgDistance_capacity_nReg_country\
    .map(lambda x: (tuple(x[1][6]), [1, x[1][7]]))\
    .reduceByKey(lambda a, b: [a[0]+b[0],
                              a[1]+b[1]])

geo_clusterN_totalMpgLoad.take(5)

geo_cluster_info = geo_clusterN_totalMpgLoad.collect()

total_n_cluster = reglist_mpgid_avgDistance_capacity_nReg_country.count()

print "total computed mpg: %s" % str(total_mpg_with_region)
print "total number of cluster of mpgs: %s" % str(total_n_cluster)

# ============ Testing Adjacency/Distance Matrix ==============
mpgidx_capw_caps_rgs = reglist_mpgid_avgDistance_capacity_nReg_country.zipWithIndex().map(lambda x: (x[1], x[0][2], x[0][3], x[0][8]))

# rdd structure:
# after join -
# (reg, [[[pair-A, pair-B], cap-w, cap-s], [reg-lat, reg-lon, reg-cap, reg-country, reg-numvips, reg-service]])
# after reduce by key -
# ([pair-A, pair-B], [cap-w, cap-s, sum-of-reg-def-cap])
# only pick pairs that differ less than 10 regions
# drop half of the bidirectional pairs
reg_pair_capacities = mpgidx_capw_caps_rgs.cartesian(mpgidx_capw_caps_rgs)\
    .map(lambda x: ([x[0][0], x[1][0]], x[0][1], x[0][2],\
                    [a for a in x[0][3].split(':')+x[1][3].split(':')\
                              if (a not in x[0][3].split(':')) or (a not in x[1][3].split(':'))] ))\
    .filter(lambda x: x[0][0] < x[0][1])\
    .filter(lambda x: len(x[3]) < 10)\
    .map(lambda x: ((x[0], x[1], x[2]), [int(x) for x in x[3]]))\
    .flatMapValues(lambda x: x)\
    .map(lambda x: (x[1], [x[0][0], x[0][1], x[0][2]]))

reg_capacity = region_latlon.map(lambda x: (x[0], round(float(x[1][2])/1000000.0, 3)))

reg_pair_capacities_reginfo = reg_capacity\
    .join(reg_pair_capacities)\
    .map(lambda x: (x[1][0][0],\
                    [x[1][0][1],\
                     x[1][0][2],\
                     round(float(x[1][1][2])/1000000.0, 3)]))\
    .reduceByKey(lambda a, b: [a[0], a[1], a[2]+b[2]])\
    .take(5)


import pandas as pd
geo_cluster_info_df = pd.DataFrame(columns=['geoname', 'mpg_count', 'total_mpg_load'])

for item in range(len(geo_cluster_info)):
    temp = geo_cluster_info[item]
    geo_cluster_info_df.loc[item] = [':'.join([str(x) for x in list(temp[0])]), temp[1][0], round(temp[1][1],4)]

geo_cluster_info_df.to_csv('/home/testgrp/geo_mpgClusterCount_mpgTotalLoad.csv', sep='\t', index=False)


# another pd.DataFrmae
country_avgDistance_capacity_nReg_mpgLoad_nMpg_reglist_mpglist = pd.DataFrame(columns=['geoname',
                                                                                       'avgDistance',
                                                                                       'capacityW',
                                                                                       'capacityS',
                                                                                       'numvips',
                                                                                       'nReg',
                                                                                       'mpgLoad',
                                                                                       'nMpg',
                                                                                       'regList',
                                                                                       'mpgList',
                                                                                       'mpgASList',
                                                                                       'mpgNSIPList'])

geo_cluster_full_info = reglist_mpgid_avgDistance_capacity_nReg_country.collect()

for item in range(len(geo_cluster_full_info)):
    temp = geo_cluster_full_info[item]
    country_avgDistance_capacity_nReg_mpgLoad_nMpg_reglist_mpglist.loc[item] = [':'.join([str(x) for x in list(temp[1][6])]),
                                                                                temp[1][1],
                                                                                temp[1][2],
                                                                                temp[1][3],
                                                                                temp[1][4],
                                                                                temp[1][5],
                                                                                temp[1][7],
                                                                                temp[1][8],
                                                                                ':'.join([str(x) for x in temp[0]]),
                                                                                ':'.join([str(x) for x in list(temp[1][0])]),
                                                                                ':'.join([str(x) for x in temp[1][9]]) if len(temp[1][9])>0 else 'NULL',
                                                                                ':'.join([str(x) for x in temp[1][10]]) if len(temp[1][10])>0 else 'NULL'
                                                                               ] # the above should be temp[1][0] for the mpglist


country_avgDistance_capacity_nReg_mpgLoad_nMpg_reglist_mpglist.to_csv('/home/testgrp/geo_full_cluster_info.csv',
                                                                      sep=',', index=False, header=True)

# another pd.DataFrame
geo_total_cap_forDB = geo_total_cap.map(lambda x: (x[0], [[x[1], 0] if x[3]=='W' else [0, x[1]], x[2]]))\
                                    .reduceByKey(lambda a, b: [[a[0][0]+b[0][0], a[0][1]+b[0][1]], a[1]+b[1]])\
                                    .collect()

country_private_cap = pd.DataFrame(columns=['geoname',
                                            'total_private_cap_W',
                                            'total_private_cap_S',
                                            'total_num_vips'])

for item in range(len(geo_total_cap_forDB)):
    temp = geo_total_cap_forDB[item]
    country_private_cap.loc[item] = [temp[0],
                                     temp[1][0][0],
                                     temp[1][0][1],
                                     temp[1][1]]

country_private_cap.to_csv('/home/testgrp/geo_total_cap.csv',
                           sep=',', index=False, header=True)

# another pd.DataFrame

region_cap = region_latlon.map(lambda x: (x[0], round(x[1][2]/1000000.0),3)).collect()
region_cap_db = pd.DataFrame(columns=['region', 'capacity'])

for item in range(len(region_cap)):
    temp = region_cap[item]
    region_cap_db.loc[item] = [temp[0], temp[1]]

region_cap_db.to_csv('/home/testgrp/region_cap.csv', sep=',', index=False, header=True)


#=================


from pyspark.sql import HiveContext
import math
import time
import pandas as pd

ts_now = int(time.time())
getting_data = ''' select * from mrqos.mrqos_join where ts>%s ''' % str(ts_now-24*60*60*7)

hiveCtx = HiveContext(sc)

rows = hiveCtx.sql(getting_data)

case_load = rows.map(lambda x: [x.maprule, x.geoname, x.netname, x.total_bps])\
                .map(lambda x: (tuple([x[0], x[1], x[2]]), [1, int(x[3])]))\
                .reduceByKey(lambda a, b: [a[0]+b[0], a[1]+b[1]])\
                .map(lambda x: [x[0][0], x[0][1], x[0][2], round(8.0*x[1][1]/x[1][0],3)])

case_load_db = case_load.collect()

db = pd.DataFrame(columns=['maprule','geoname','netname','stat_bps'])
for item in range(len(case_load_db)):
    temp = case_load_db[item]
    db.loc[item] = [temp[0], temp[1], temp[2], temp[3]]

db.to_csv('/home/testgrp/mr_geoname_netname_oneWeekAvgLoad.csv', index=False, header=True)

# this saveAsTextFile function will save to HDFS
#case_load.saveAsTextFile('/home/testgrp/mr_geoname_netname_oneWeekAvgLoad.txt')