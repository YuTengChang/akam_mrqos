import sys, os
from datetime import date, timedelta
import subprocess as sp
sys.path.append('/home/ychang/Documents/Projects/18-DDC/MRQOS/')
import configurations.config as config


def main():

    # fetch new results from DDC cluster
    fetch_list = ['summarized_table.tmp',
                 'summarized_table2.tmp',
                 'compound_metric.tmp',
                 'processed_2wjoin_full.tmp',
                 'processed_2wjoin_full_wloads.tmp',
                 'processed_4wjoin_full_wloads.tmp',
                 'processed_3djoin_full_wloads.tmp',
                 'processed_2wjoin_full_wloads_wio.tmp',
                 'processed_4wjoin_full_wloads_wio.tmp',
                 'processed_3djoin_full_wloads_wio.tmp']

    for fetch_item in fetch_list:
        source_path = os.path.join(config.mrqos_data,
                                   fetch_item)
        dest_path = os.path.join(config.local_mrqos_data_summary_stats,
                                 fetch_item)
        cmd_str = 'scp -Sgwsh testgrp@{}:{} {}'.format(config.mrqos_data_node,
                                                       source_path,
                                                       dest_path)
        sp.check_call(cmd_str, shell=True)

    # appending datestamp in compound_metric.csv
    cmd_str = ''' head -2 /u4/ychang/Projects/18-MRQOS/Data/summarized_table.csv | tail -1 | awk '{print $1","$2}' '''
    proc = sp.Popen(cmd_str, stdout=sp.PIPE, shell=True)
    output = proc.stdout.read().split('\n')[0]
    source_path = os.path.join(config.local_mrqos_data_summary_stats, 'compound_metric.csv')
    dest_path = os.path.join(config.local_mrqos_data_summary_stats, 'compound_metric_comma.csv')
    cmd_str = ''' cat %s | awk -v var="$timenow" '{print var, $0}' | sed 's/\s\+/,/g' > %s ''' % (source_path,
                                                                                                  dest_path)
    cmd_str = 'timenow=$(echo {});'.format(output) + cmd_str
    sp.check_call(cmd_str, shell=True)
    vm_txt_root = '/var/www/txt/'
    cmd_str = 'cp {} {}'.format(dest_path,
                                os.path.join(vm_txt_root,
                                             'compound_metric_comma.csv'))
    sp.check_call(cmd_str, shell=True)

    # generate figures
    cmd_str = '/home/ychang/anaconda/bin/python2.7 {}'.format(os.path.join(config.local_mrqos_root,
                                                                           'local_python',
                                                                           'local_generate_figures_desktop.py'))
    sp.check_call(cmd_str, shell=True)

    # transfer file to VM
    report_date = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
    vm_tx_list = ['processed_2wjoin_full.csv',
                  'processed_2wjoin_full_wloads.csv',
                  'processed_3djoin_full_wloads.csv',
                  'compound_metric_comma.csv',
                  'processed.csv',
                  'Fred3DReport.{}.csv'.format(report_date),
                  'Fred2WReport.{}.csv'.format(report_date),
                  ]
    for tx_file in vm_tx_list:
        source_path = os.path.join(vm_txt_root, tx_file)
        cmd_str = 'scp {} ychang@{}:{}'.format(source_path,
                                               config.web_server_machine,
                                               source_path)
        sp.check_call(cmd_str, shell=True)

    # local SQLite3 DB @ local update
    # cmd_str = '/opt/anaconda/bin/sqlite3 /opt/web-data/SQLite3/mrqos.db < /u4/ychang/Projects/18-MRQOS/SQLite3/input_lpwl3.sql >> /opt/web-data/SQLite3/input_log_lpwl3.log'
    # sp.check_call(cmd_str, shell=True)

    # update static info table (perforce updates)
    cmd_str = '/opt/anaconda/bin/python {}'.format(os.path.join(config.local_mrqos_root,
                                                                'local_python',
                                                                'local_static_table_generate_mrqos_processed.py'))
    sp.check_call(cmd_str, shell=True)

if __name__ == '__main__':
    sys.exit(main())