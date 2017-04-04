#!/opt/anaconda/bin/python
"""
Created on Thu Apr 16 16:47:15 2015

@author: ychang
"""
import sys, os
import shutil

sys.path.append('/home/testgrp/MRQOS/')
import subprocess as sp
import pandas as pd
import numpy
import time
import glob
import logging


def main():
    mako_cmd = """/home/ychang/bin/mako --csv -s "`cat {}`" -o {} """
    query_list = ["geo_pp_coverage",
                  "geo_as_pp_coverage",
                  "pp_coverage"]

    datestamp = int(time.strftime("%Y%m%d", time.gmtime()))

    header_list = {"geo_pp_coverage": ["COUNTRY", "ASNS", "TOT_DEMANDS", "IN_ASN_PCT", "IN_COUNTRY_PCT",
                                       "HAS_PP_PCT", "AVG_DISTANCE", "DATESTAMP"],
                   "geo_as_pp_coverage": ["COUNTRY", "ASNUM", "SUM_DEMAND", "IN_ASN_PCT", "IN_PROVIDER_PCT",
                                          "IN_COUNTRY_PCT", "HAS_PP_PCT", "WEIGHTED_DISTANCE", "DATESTAMP"],
                   "pp_coverage": ["TOT_DEMANDS", "IN_COUNTRY_PCT", "IN_ASN_PCT", "HAS_PP_PCT", "AVG_DISTANCE",
                                   "DATESTAMP"]
                   }

    query_dir = "/home/ychang/Documents/Projects/18-DDC/MRQOS/mrqos_query"
    csv_dir = "/home/ychang/Documents/Projects/18-DDC/MRQOS_local_data/ns_pp_coverage"

    for query in query_list:
        cmd = mako_cmd.format(os.path.join(query_dir, query + ".sql"),
                              os.path.join(csv_dir, query + ".csv"))
        sp.check_call(cmd, shell=True)
        df = pd.read_csv(os.path.join(csv_dir, query+".csv"), header=0)
        df['DATESTAMP'] = datestamp
        if "COUNTRY" not in df.columns and query != "pp_coverage":
            df["COUNTRY"] = df["CODE"]
        df = df[header_list[query]]

        df.to_csv(os.path.join(csv_dir, query+"_result.csv"),
                  header=False,
                  index=False)



if __name__ == '__main__':
    sys.exit(main())