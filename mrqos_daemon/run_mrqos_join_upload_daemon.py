import sys
sys.path.append('/home/testgrp/MRQOS/crondaemon')
import joinUploadDaemon as ed
import backgroundStart as bs


if __name__=='__main__':

    """
    set the arguments:
    log_file: file location of the .log file
    log: python object for logger
    sleep_interval: interval of sequential check, in seconds
    """

    args = { 'log_file': '/home/testgrp/logs/mrqos_query_uploads.log',
             'log': 'MRQOS_Join_Upload_Log',
             'sleep_interval': 10*60}

    dmn = ed.joinUploadDaemon(args)
    proc = bs.backgroundStarter(args,'/home/testgrp/pid/mrqos_query_uploads_pid.st',dmn)

    proc.run()

