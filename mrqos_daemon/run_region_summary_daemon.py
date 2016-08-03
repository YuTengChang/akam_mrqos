import sys
sys.path.append('/home/testgrp/MRQOS/crondaemon')
import regionSummaryDaemon as ed
import backgroundStart as bs


if __name__=='__main__':

    """
    set the arguments:
    log_file: file location of the .log file
    log: python object for logger
    sleep_interval: interval of sequential check, in seconds
    """

    args = { 'log_file': '/home/testgrp/logs/cron_region_summary_hour.log',
             'log': 'REGION_SUMMARY_HOUR_Log',
             'sleep_interval': 60*60}

    dmn = ed.regionSummaryDaemon(args)
    proc = bs.backgroundStarter(args,'/home/testgrp/pid/region_summary_hour_pid.st',dmn)

    proc.run()

