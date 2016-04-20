#!/opt/anaconda/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
"""
import sys, os
import shutil
import time
import calendar
import subprocess as sp
sys.path.append('/home/ychang/Documents/Projects/18-DDC/MRQOS/')
import configurations.config as config

def main():
    ts = calendar.timegm(time.gmtime())
    print "###################"
    print "# Performing the LP solution check and push procedure"
    print "# starting processing time is " + str(ts) + " = " + time.strftime('GMT %Y-%m-%d %H:%M:%S', time.localtime(ts))
    print "###################"
    print "  >> check the LP solutions <<"

    local_lp_folder = '/home/ychang/Documents/Projects/18-DDC/MRQOS_local_data/lp'

    cmd_str = """ssh %s -A 'ls -d /opt/lp/*/' """ % config.web_server_machine
    filelist = sp.check_output(cmd_str, shell=True)
    production_filelist = [y for y in [x.split('/')[-2] for x in filelist.split('\n')[:-1]] if y[-5:] == '_prod']
    print production_filelist

    for file in production_list:
        cmd_str = 'scp ychang@dev-platformperf-scidb02:/opt/lp/%s %s' % (file, os.path.join(local_lp_folder, file))
        sp.check_call(cmd_str, shell=True)


if __name__ == '__main__':
    sys.exit(main())