#!/opt/anaconda/bin/python
"""
Created on Thu March 16 12:47:15 2016

@author: ychang
"""

import sys
import subprocess as sp
sys.path.append('/home/ychang/Documents/Projects/18-DDC/MRQOS/')
import configurations.config as config

def main():
    remote_machines = ['81.52.137.195', '81.52.137.180', '81.52.137.182', '81.52.137.188', # old cluster
                       '104.121.149.12', '104.121.149.14', '104.121.149.20', '104.121.149.27'] # new cluster
    down_machines = ['']

    cmd = 'cd {}; git pull;'.format(config.local_mrqos_root)
    sp.check_call(cmd, shell=True)

    update_machines = [x for x in remote_machines if x not in down_machines]

    for machine in update_machines:
        cmd = 'rsync -r -v --exclude="*.tmp" --exclude="*.log" --exclude=".git" --exclude=".gitignore" -e gwsh %s %s:~/MRQOS/;' % (config.local_mrqos_root,
                                                                                                                                   machine)

        sp.check_call(cmd, shell=True)

    print "updated (synced) machines:",
    print update_machines

if __name__ == '__main__':
    sys.exit(main())