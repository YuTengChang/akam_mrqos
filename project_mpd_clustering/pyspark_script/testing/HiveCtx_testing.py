from pyspark.sql import HiveContext
import math
import pandas as pd
import numpy as np
import os


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


day_idx = '20160718'
uuid_idx = 'c57240a4-4cd6-11e6-a415-300ed5cc4e6c'

getting_mappoint_data = ''' select b1.mpgid mpgid, b1.lat lat, b1.lon lon, b1.country country, b1.mpgload mpgload, b1.allowed_private_regions allowed_private_regions, b2.asnum asnum, b2.ip ip from (select mpgid, lat, lon, country, mpgload, allowed_private_regions from mapper.mappoints where day=%s and uuid="%s" and lat is not NULL and lon is not NULL and ghostonly=0 ) b1 left outer join (select collect_set(ip) ip, collect_set(asnum) asnum, mpgid from (select ip, mpd_uuid, mpgid, asnum, day from mapper.nsassoc where day=%s and mpd_uuid="%s") a group by mpgid) b2 on b2.mpgid=b1.mpgid ''' % (day_idx, uuid_idx, day_idx, uuid_idx)
geo_total_cap_query = ''' select * from (select country, network, sum(peak_bitcap_mbps) peak_bitcap_mbps, sum(peak_flitcap_mfps) peak_flitcap_mfps, sum(numvips) numvips from mapper.regioncapday where day=%s and network in ('freeflow', 'essl') and prp='private' group by country, network) a ''' % day_idx
geo_total_cap_public_query = ''' select * from (select country, network, sum(peak_bitcap_mbps) peak_bitcap_mbps, sum(peak_flitcap_mfps) peak_flitcap_mfps, sum(numvips) numvips from mapper.regioncapday where day=%s and network in ('freeflow', 'essl') and prp='public' group by country, network) a ''' % day_idx


hiveCtx = HiveContext(sc)

rows = hiveCtx.sql(getting_mappoint_data)

#regInfoRows = hiveCtx.sql('select a.*, b.region_capacity, b.ecor_capacity, b.prp, case b.peak_bitcap_mbps when null then 0 else b.peak_bitcap_mbps end peak_bitcap_mbps, case b.peak_flitcap_mfps when null then 0 else b.peak_flitcap_mfps end peak_flitcap_mfps from (select * from mapper.barebones where day=%s and latitude is not NULL and longitude is not NULL and ghost_services in ("W","S","KS","JW")) a join (select * from mapper.regioncapday where day=%s) b on a.region=b.region' % (day_idx, day_idx))
regInfoRows = hiveCtx.sql('select * from mapper.regioncapday where day=%s and peak_bitcap_mbps is not null and peak_flitcap_mfps is not null' % (day_idx))
geo_total_cap = hiveCtx.sql(geo_total_cap_query)
geo_total_cap_p = hiveCtx.sql(geo_total_cap_public_query)


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

# rdd format: [regionid, [reg-lat, reg-lon, reg-capacity(bit mbps), reg-capacity(bit mfps), reg-country, reg-numvips, reg-service, reg-prp]]
# ps. prp=1: private, prp=0: public
region_latlon = regInfoRows.map(lambda x: [x.region, [x.latitude,
                                                      x.longitude,
                                                      x.peak_bitcap_mbps,
                                                      x.peak_flitcap_mfps,
                                                      x.country,
                                                      x.numvips,
                                                      'W' if x.network=='freeflow' else ('S' if x.network=='essl' else 'O'),
                                                      1 if x.prp=='private' else 0]])\
                            .filter(lambda x: x[1][6]=='W' or x[1][6]=='S')

region_public_list = region_latlon\
    .filter(lambda x: x[1][7] == 0)\
    .map(lambda x: ('all', [[x[0]]]))\
    .reduceByKey(lambda a, b: [a[0]+b[0]])\
    .map(lambda x: x[1][0]).collect()

region_public_list = sorted(region_public_list[0])

# perform the join into tuple of (K, (V1, V2):
# (regionid, ([mpgid, mpg-lat, mpg-lon, mpg-country, mpg-load], [reg-lat, reg-lon, reg-cap, reg-country, reg-numvips, reg-service]))
# rdd  = (mpgid, regionid, [lat1, lon1, lat2, lon2, distance],
#               reg-cap-bit(gbps), reg-cap-flit(gbps), reg-country, reg-numvips, reg-services,
#               mpg-country, mpg-load, mpg-asnum, mpg-nsip,
#               mpg-lat, mpg-lon)
mpgid_reg_geo = region_mpginfo_pair.join(region_latlon).map(lambda x: [x[1][0][0],
                                                                       x[0],
                                                                       geodesic_distance(x[1][0][1],
                                                                                         x[1][0][2],
                                                                                         x[1][1][0],
                                                                                         x[1][1][1]),
                                                                       round(float(x[1][1][2])/1000.0, 3),
                                                                       round(float(x[1][1][3])/1000.0, 3),
                                                                       x[1][1][4], # reg-country
                                                                       x[1][1][5], # reg-numvips
                                                                       x[1][1][6], # reg-services
                                                                       x[1][0][3],
                                                                       x[1][0][4],
                                                                       x[1][0][5],
                                                                       x[1][0][6],
                                                                       x[1][0][1],
                                                                       x[1][0][2]])

# filtering on mapping distance < 500 miles
# filtering on reg-country = mpg-country
# filtering on region capacity fbps > 1Gbps
# rdd format = (mpgid, [[regionid], distance, [capacity-w, capacity-s], numvips, 1, mpg-country, mpg-load, mpg-asnum, mpg-nsip,
#                        mpg-lat, mpg-lon])
#mpgid_reg_distance = mpgid_reg_geo.filter(lambda x: x[2][4] < 500)\
#    .filter(lambda x: x[5] == x[8])\
#    .filter(lambda x: x[3] > 1)\
#    .map(lambda x: (x[0], [[x[1]], x[2][4], [x[3], 0] if x[7]=='W' else [0, x[3]], x[6], 1, x[8], x[9], x[10], x[11], x[12], x[13]]))

# or this one, no-same-country constraint:
mpgid_reg_distance = mpgid_reg_geo.filter(lambda x: (x[2][4] < 500) or (x[5]==x[8] and x[2][4] < 1000))\
    .filter(lambda x: x[3] > 1)\
    .map(lambda x: (x[0], [[x[1]], x[2][4], [x[3], 0] if x[7]=='W' else [0, x[3]], x[6], 1, x[8], x[9], x[10], x[11], x[12], x[13]]))

#mpgid_reg_distance.first()

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
    .reduceByKey(lambda a, b: [a[0]+b[0], a[1]+b[1], [a[2][0]+b[2][0], a[2][1]+b[2][1]], a[3]+b[3], a[4]+b[4],
                               a[5], a[6], a[7], a[8], a[9], a[10]])\
    .map(lambda x: (x[0], [sorted(x[1][0]), # region_list
                           round(x[1][1]/x[1][4], 2), # avg distance
                           round(x[1][2][0], 2), # total capacity - w
                           round(x[1][2][1], 2), # total capacity - s
                           x[1][3], # numvips
                           x[1][4], # total region count
                           x[1][5], # mpg country
                           x[1][6], # mpg load
                           x[1][7], # mpg asnum
                           x[1][8], # mpg nsip
                           x[1][9], # mpg lat
                           x[1][10]])) # mpg lon

total_mpg_with_region = mpgid_reglist_avgDistance_capacity_nReg.count()

# rdd format = (reg, [(reg-list), [[mpg-list], avg_distance, total_cap_w, total_cap_s, total_numvips
#                           reg-count, cluster_country, mpg-load, mpg-count, mpg-lat, mpg-lon]])
reg_reglist_mpgid_avgDistance_capacity_nReg_country = mpgid_reglist_avgDistance_capacity_nReg\
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
                                      x[1][9] if x[1][9] else [], # [mpg-nsip]
                                      [x[1][10]], # [mpg-lat] # single element array
                                      [x[1][11]], # [mpg-lon] # single element array
                                      [x[1][7]] # [mpg-load] # single element array
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
                               a[10]+b[10],
                               a[11]+b[11],
                               a[12]+b[12],
                               a[13]+b[13]])\
    .filter(lambda x: sum(x[1][13]) > 0.0001)\
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
                           [str(y) for y in sorted(list(set(x[1][10])))], # [mpg-nsip]
                           geo_centroid(x[1][11], x[1][12], x[1][13]) # [mpg: lat, lon, por, porsigma]
                           ]))\
    .map(lambda x: ([':'.join([str(y) for y in list(x[1][6])]), # [mpg-country list]
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
                    ':'.join([str(y) for y in x[1][10]]) if len(x[1][10])>0 else 'NULL', # [mpg-nsip]
                    x[1][11] # [mpg-lat, mpg-lon, mpg-por, mpg-porsigma]
                    ],
                    region_public_list
                    ))\
    .flatMapValues(lambda x: x)\
    .map(lambda x: [x[1], x[0]])

reglist_mpgid_avgDistance_capacity_nReg_country = reg_reglist_mpgid_avgDistance_capacity_nReg_country\
    .join(region_latlon)\
    .map(lambda x: [x[1][0]]+[x[1][1]]+[geodesic_distance(x[1][0][12][0],
                                                         x[1][0][12][1],
                                                         x[1][1][0],
                                                         x[1][1][1])] + [x[0]])\
    .filter(lambda x: x[2][4] < 500)\
    .map(lambda x: (tuple([x[0][0],
                          x[0][1],
                          x[0][2],
                          x[0][3],
                          x[0][4],
                          x[0][5],
                          x[0][6],
                          x[0][7],
                          x[0][8],
                          x[0][9],
                          x[0][10],
                          x[0][11],
                          x[0][12][0],
                          x[0][12][1],
                          x[0][12][2],
                          x[0][12][3]]), # mpg-information
                    [x[1][2], # pub.region.cap.ff
                     x[1][3], # pub.region.cap.essl
                     x[1][5], # pub.region.vip
                     [x[3]] # single element region id
                     ]))\
    .reduceByKey(lambda a, b: [a[0]+b[0], # sum( pub.region.cap.ff )
                               a[1]+b[1], # sum( pub.region.cap.essl )
                               a[2]+b[2], # sum( pub.region.cap.vip )
                               a[3]+b[3] # [pub.regions]
                               ])\
    .map(lambda x: [x[0][0], # [mpg-country-list]
                    x[0][1], # avg-distance
                    x[0][12], # mpg-lat
                    x[0][13], # mpg-lon
                    x[0][14], # mpg-por
                    x[0][15], # mpg-porsigma
                    x[0][2], # pri.region.cap.ff (gbps)
                    x[0][3], # pri.region.cap.essl (gbps)
                    x[0][4], # pri.vips
                    x[0][5], # pri.region.count
                    round(float(x[1][0])/1000.0, 3), # pub.region.cap.ff (gbps)
                    round(float(x[1][1])/1000.0, 3), # pub.region.cap.essl (gbps)
                    x[1][2], # pub.vips
                    len(x[1][3]), # pub.region.count
                    x[0][6], # mpg-load
                    round(x[0][7], 6), # mpg-count
                    x[0][8], # [pri reg-list]
                    ':'.join([str(y) for y in sorted(x[1][3])]) if len(x[1][3])>0 else 'NULL', # [pub reg-list])
                    x[0][9], # [mpg-list]
                    x[0][10], # [mpg-assum]
                    x[0][11] # [mpg-nsip]
                    ])

# ================ NOT USED ===================
# (mpg-country-list, [cluster-count, mpg-load])
geo_clusterN_totalMpgLoad = reglist_mpgid_avgDistance_capacity_nReg_country\
    .map(lambda x: (x[0], [1, x[6]]))\
    .reduceByKey(lambda a, b: [a[0]+b[0],
                              a[1]+b[1]])

geo_clusterN_totalMpgLoad.take(5)

geo_cluster_info = geo_clusterN_totalMpgLoad.collect()

total_n_cluster = reglist_mpgid_avgDistance_capacity_nReg_country.count()

print "total computed mpg: %s" % str(total_mpg_with_region)
print "total number of cluster of mpgs: %s" % str(total_n_cluster)

# ============ Testing Adjacency/Distance Matrix ==============
#mpgidx_capw_caps_rgs = reglist_mpgid_avgDistance_capacity_nReg_country.zipWithIndex().map(lambda x: (x[1], x[0][2], x[0][3], x[0][8]))

# rdd structure:
# after join -
# (reg, [[[pair-A, pair-B], cap-w, cap-s], [reg-lat, reg-lon, reg-cap, reg-country, reg-numvips, reg-service]])
# after reduce by key -
# ([pair-A, pair-B], [cap-w, cap-s, sum-of-reg-def-cap])
# only pick pairs that differ less than 10 regions
# drop half of the bidirectional pairs
#reg_pair_capacities = mpgidx_capw_caps_rgs.cartesian(mpgidx_capw_caps_rgs)\
#    .map(lambda x: ([x[0][0], x[1][0]], x[0][1], x[0][2],\
#                    [a for a in x[0][3].split(':')+x[1][3].split(':')\
#                              if (a not in x[0][3].split(':')) or (a not in x[1][3].split(':'))] ))\
#    .filter(lambda x: x[0][0] < x[0][1])\
#    .filter(lambda x: len(x[3]) < 10)\
#    .map(lambda x: ((x[0], x[1], x[2]), [int(x) for x in x[3]]))\
#    .flatMapValues(lambda x: x)\
#    .map(lambda x: (x[1], [x[0][0], x[0][1], x[0][2]]))

#reg_capacity = region_latlon.map(lambda x: (x[0], round(float(x[1][2])/1000000.0, 3)))

#reg_pair_capacities_reginfo = reg_capacity\
#    .join(reg_pair_capacities)\
#    .map(lambda x: (x[1][0][0],\
#                    [x[1][0][1],\
#                     x[1][0][2],\
#                     round(float(x[1][1][2])/1000000.0, 3)]))\
#    .reduceByKey(lambda a, b: [a[0], a[1], a[2]+b[2]])\
#    .take(5)


# ============ END of Testing Adjacency/Distance Matrix ==============


geo_cluster_info_df = pd.DataFrame(columns=['geoname', 'mpg_count', 'total_mpg_load'])

for item in range(len(geo_cluster_info)):
    temp = geo_cluster_info[item]
    geo_cluster_info_df.loc[item] = [':'.join([str(x) for x in list(temp[0])]), temp[1][0], round(temp[1][1],4)]

geo_cluster_info_df.to_csv('/home/testgrp/geo_mpgClusterCount_mpgTotalLoad.csv', sep='\t', index=False)



# another pd.DataFrmae
country_avgDistance_capacity_nReg_mpgLoad_nMpg_reglist_mpglist = pd.DataFrame(columns=['cl_geoname',
                                                                                       'cl_avgDistance',
                                                                                       'cl_lat',
                                                                                       'cl_lon',
                                                                                       'cl_por',
                                                                                       'cl_porsigma',
                                                                                       'pri_cap_ff_gbps',
                                                                                       'pri_cap_essl_gbps',
                                                                                       'pri_nvips',
                                                                                       'pri_nReg',
                                                                                       'pub_cap_ff_gbps',
                                                                                       'pub_cap_essl_gbps',
                                                                                       'pub_nvips',
                                                                                       'pub_nReg',
                                                                                       'cl_mpgLoad',
                                                                                       'cl_nMpg',
                                                                                       'pri_regList',
                                                                                       'pub_regList',
                                                                                       'mpgList',
                                                                                       'mpgASList',
                                                                                       'mpgNSIPList'])

geo_cluster_full_info = reglist_mpgid_avgDistance_capacity_nReg_country.collect()

for item in range(len(geo_cluster_full_info)):
    temp = geo_cluster_full_info[item]
    country_avgDistance_capacity_nReg_mpgLoad_nMpg_reglist_mpglist.loc[item] = temp # the above should be temp[1][0] for the mpglist

fileDestination = os.path.join('/home/testgrp/','geo_full_cluster_info.%s.%s.csv' % (day_idx,
                                                                                     uuid_idx))
country_avgDistance_capacity_nReg_mpgLoad_nMpg_reglist_mpglist.to_csv(fileDestination,
                                                                      sep=',', index=False, header=True)

# another pd.DataFrame
geo_total_cap_forDB = geo_total_cap.collect()

country_private_cap = pd.DataFrame(columns=['geoname',
                                            'network',
                                            'total_private_cap_bit',
                                            'total_private_cap_flit',
                                            'total_num_vips'])

for item in range(len(geo_total_cap_forDB)):
    temp = geo_total_cap_forDB[item]
    country_private_cap.loc[item] = temp

country_private_cap.to_csv('/home/testgrp/geo_total_cap.csv',
                           sep=',', index=False, header=True)

# another pd.DataFrame for public capacity
geo_total_cap_forDB = geo_total_cap_p.collect()

country_private_cap = pd.DataFrame(columns=['geoname',
                                            'network',
                                            'total_private_cap_bit',
                                            'total_private_cap_flit',
                                            'total_num_vips'])

for item in range(len(geo_total_cap_forDB)):
    temp = geo_total_cap_forDB[item]
    country_private_cap.loc[item] = temp

country_private_cap.to_csv('/home/testgrp/geo_total_cap_p.csv',
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