import sys
sys.path.append('/home/testgrp/MRQOS/crondaemon')
import cronDaemon
import subprocess as sp



class regionSummaryDaemon(cronDaemon.cronDaemon):
    """
    the child class of the daemon class
    """
    def __init__(self,args):
        cronDaemon.cronDaemon.__init__(self,args)
        self.log.info("CronDaemon for doing the region summary, case view, region view")


    def run_job(self):
        cmdstr = '''/usr/bin/python /home/testgrp/MRQOS/mrqos_python_script/region_summary_hour_backfill.py '''
        sp.check_call(cmdstr, shell=True)




