#!/opt/anaconda/bin/python
"""
Created on Thu Apr 16 16:47:15 2015

@author: ychang
"""
import sys, os
import subprocess as sp
import pandas as pd
import time
import logging
import calendar

root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(root)
import configurations.config as config


def main():

    logging.basicConfig(filename=os.path.join('/home/ychang/logs/', 'pp_coverage.log'),
                        level=logging.INFO,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    mako_cmd = """/home/ychang/bin/mako --csv -s "`cat {}`" -o {} """
    query_list = ["geo_pp_coverage",
                  "geo_as_pp_coverage",
                  "pp_coverage"]

    ts = calendar.timegm(time.gmtime())

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
        logger.info("start processing {}".format(query))
        logger.info("fetch data from MAKO")
        # fetch the data from MAKO
        cmd = mako_cmd.format(os.path.join(query_dir, query + ".sql"),
                              os.path.join(csv_dir, query + ".csv"))
        sp.check_call(cmd, shell=True)
        df = pd.read_csv(os.path.join(csv_dir, query+".csv"), header=0)
        df['DATESTAMP'] = datestamp
        if "COUNTRY" not in df.columns and query != "pp_coverage":
            df["COUNTRY"] = df["CODE"]
        df = df[header_list[query]]

        local_file = os.path.join(csv_dir, query+"_result.csv")
        target_file = query+"_result.csv"

        df.to_csv(local_file,
                  header=False,
                  index=False)

        # move to VM
        logger.info("move data to VM and update to sqlite db")
        cmd_str = 'scp %s ychang@%s:%s' % (local_file,
                                           config.web_server_machine,
                                           os.path.join(config.pp_coverage_VM, target_file))
        sp.check_call(cmd_str, shell=True)
        # VM import sql
        cmd_str = "ssh %s 'echo .separator , > %s' " % (config.web_server_machine,
                                                        os.path.join(config.pp_coverage_VM, 'input_query.sql'))
        sp.check_call(cmd_str, shell=True)
        cmd_str = "ssh %s 'echo .import %s %s >> %s' " % (config.web_server_machine,
                                                          os.path.join(config.pp_coverage_VM, target_file),
                                                          query,
                                                          os.path.join(config.pp_coverage_VM, 'input_query.sql'))
        sp.check_call(cmd_str, shell=True)
        # VM data import
        cmd_str = "ssh %s '/opt/anaconda/bin/sqlite3 %s < %s' " % (config.web_server_machine,
                                                                   config.pp_coverage_db,
                                                                   os.path.join(config.pp_coverage_VM, 'input_query.sql'))
        sp.check_call(cmd_str, shell=True)


        # retire data from VM
        logger.info("retire the expired data on VM")
        expire_pp_coverage_vm = 60*60*24*30 # 30 days expiration (~ 1-month)
        expire_date = time.strftime('%Y%m%d', time.gmtime(float(ts - expire_pp_coverage_vm)))
        sql_str = '''PRAGMA temp_store_directory='/opt/web-data/temp'; delete from %s where DATESTAMP<%s; vacuum;''' % (query, str(expire_date))
        cmd_str = '''ssh %s "/opt/anaconda/bin/sqlite3 %s \\\"%s\\\" " ''' % (config.web_server_machine,
                                                                              config.pp_coverage_db,
                                                                              sql_str)
        sp.check_call(cmd_str, shell=True)

if __name__ == '__main__':
    sys.exit(main())