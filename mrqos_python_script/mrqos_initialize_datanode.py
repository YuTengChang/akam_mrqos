#!//usr/bin/python
"""
Created on Thu May 06 14:29:31 2017

@author: ychang
"""
import sys, os
import shutil
sys.path.append('/home/testgrp/MRQOS/')
import configurations.config as config

def main():
    node_root = os.path.dirname(os.path.dirname(config.mrqos_root))
    folder_list = [ os.path.join(node_root, 'logs'),
                    os.path.join(node_root, 'pid'),
                    os.path.join(node_root, 'lib'),
                    os.path.join(node_root, 'query_results'),
                    os.path.join(node_root, 'installs'),
                    config.mrqos_data_backup,
                    os.path.join(config.mrqos_data_backup, 'joined'),
                    ]
    file_list = [os.path.join(node_root, 'doasme_YT'),
                 os.path.join(node_root, '.bash_profile'),
                 os.path.join(node_root, '.dircolors'),
                 ]

    # create directories
    for folder in folder_list:
        if not os.path.isdir(folder):
            os.makedirs(folder)

    # copy file from /home/testgrp/MRQOS to /home/testgrp
    for fileid in file_list:
        if not os.path.isfile(fileid):
            file_name = fileid.split('/')[-1]
            shutil.copy2(os.path.join(config.mrqos_root, file_name),
                         fileid)


if __name__ == '__main__':
    sys.exit(main())