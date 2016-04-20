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

    for file in production_filelist:
        print "    **** processing the solution: "+str(file)
        # fetch the folder from VM
        cmd_str = 'scp ychang@dev-platformperf-scidb02:/opt/lp/%s %s' % (file, os.path.join(local_lp_folder, file))
        sp.check_call(cmd_str, shell=True)
        local_solution_folder = os.path.join(os.path.join(local_lp_folder, file), 'vis')

        # processing the file locally
        cmd_str = '''cat %s/vis_map_country_network %s/vis_map_country | sed 's/|/ /g' | awk '{if (NF<9){$2=$2" ANY";} print $0;}' > %s/%s ''' % (local_solution_folder,
                                                                                                                                                  local_solution_folder,
                                                                                                                                                  local_solution_folder,
                                                                                                                                                  'vis_all_'+file)
        sp.check_call(cmd_str, shell=True)

        # upload to the cluster (81.52.137.195)
        cmd_str = ''' scp -Sgwsh %s/%s testgrp@s195m.ddc:/home/testgrp/MRQOS/mrqos_data/lp/%s''' % (local_solution_folder,
                                                                                                    'vis_all_'+file,
                                                                                                    'vis_all_'+file)
        sp.check_call(cmd_str, shell=True)

        # rename the file at VM
        cmd_str = ''' ssh %s -A 'sudo mv /opt/lp/%s /opt/lp/%s' ''' % (file, file+'_p')
        sp.check_call(cmd_str, shell=True)

        # cleanup the local files
        shutil.rmtree(local_solution_folder)


if __name__ == '__main__':
    sys.exit(main())