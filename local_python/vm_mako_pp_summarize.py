#!/opt/anaconda/bin/python
"""
Created on Thu Apr 16 16:47:15 2015

@author: ychang
"""
import sys, os
import subprocess as sp
import pandas as pd
import time
from datetime import datetime, timedelta

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)
import configurations.config as config

def main():
    ds_now = datetime.now().strftime('%Y%m%d')
    ds_1w = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
    fileLocation = '/var/www/txt/pp_coverage'

    queryDict = {}

    queryDict["geo_as_pp"] = """ select a.*, b.sum_demand sum_demand_7d, b.in_asn_pct in_asn_pct_7d, b.in_provider_pct in_provider_pct_7d, b.in_country_pct in_country_pct_7d, b.has_pp_pct has_pp_pct_7d, b.weighted_distance distance_7d, b.datestamp datestamp_7d from (select * from geo_as_pp_coverage where datestamp={}) a join (select * from geo_as_pp_coverage where datesta
mp={}) b on a.country = b.country and a.asnum = b.asnum; """.format(ds_now, ds_1w)

    queryDict["geo_pp"] = """ select a.*, b.asns asns_7d, b.tot_demands tot_demands_7d, b.in_asn_pct in_asn_pct_7d, b.in_country_pct in_country_pct_7d, b.has_pp_pct has_pp_pct_7d, b.avg_distance distance_7d from (select * from geo_pp_coverage where datestamp={}) a join (select * from geo_pp_coverage where datestamp={}) b on a.country = b.country; """.format(ds_now, ds_1w)

    for item in ["geo_pp", "geo_as_pp"]:
        query = queryDict[item]
        fileDest = os.path.join(fileLocation, "{}_{}.csv".format(item, ds_now))
        cmd_str = """ /opt/anaconda/bin/sqlite3 /opt/web-data/SQLite3/pp_coverage.db '{}' > {}""".format(query, fileDest)
        sp.check_call(cmd_str, shell=True)


if __name__ == '__main__':
    sys.exit(main())