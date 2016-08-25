#!/opt/anaconda/bin/python
"""
Created on Thu March 16 12:47:15 2016

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
    remote_machines = ['81.52.137.195','81.52.137.180','81.52.137.182','81.52.137.183','81.52.137.188']
    down_machines = ['']

    cmd = 'cd ~/Documents/Projects/18-DDC/MRQOS/; git pull;'
    sp.check_call(cmd, shell=True)

    update_machines = [x for x in remote_machines if x not in down_machines]

    for machine in update_machines:
        cmd = 'rsync -r -v --exclude="*.tmp" --exclude="*.log" -e gwsh ~/Documents/Projects/18-DDC/MRQOS/ %s:~/MRQOS/;' % machine
        sp.check_call(cmd, shell=True)

if __name__ == '__main__':
    sys.exit(main())