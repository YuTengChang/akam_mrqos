import sys
sys.path.append('/home/testgrp/MRQOS/crondaemon')
import cronDaemon
import subprocess as sp



class joinUploadDaemon(cronDaemon.cronDaemon):
    """
    the child class of the daemon class
    """
    def __init__(self,args):
        cronDaemon.cronDaemon.__init__(self,args)
        self.log.info("CronDaemon for upload mrqos.mrqos_join/mrqos_join2 data")


    def run_job(self):
        cmdstr = '''/usr/bin/python /home/testgrp/MRQOS/mrqos_python_script/mrqos_query_uploads.py '''
        sp.check_call(cmdstr, shell=True)




