#!/opt/anaconda/bin/python
"""
Created on Thu March 09 12:47:15 2016

@author: ychang
"""
import sys, os
import subprocess as sp
sys.path.append('/home/ychang/Documents/Projects/18-DDC/MRQOS/')
import configurations.config as config
import logging
import time

def main():
    # set up the logger
    logging.basicConfig(filename=os.path.join(config.local_logging, 'aslist_update.log'),
                            level=logging.INFO,
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            datefmt='%m/%d/%Y %H:%M:%S')
    logger = logging.getLogger(__name__)

    timestamp = time.strftime('%Y%m%d %H:%M:%S', time.gmtime())
    logger.info('start cron-tasking the aslist updates at date: %s' % (timestamp))


    cmd_str = 'python %s' % (os.path.join('/home/ychang/Documents/Projects/18-DDC/MRQOS/','local_python','mrqos_aslist_update.py'))
    sp.check_call(cmd_str, shell=True)

    if os.path.isfile(config.aslist_file) and os.stat(config.aslist_file).st_size > 0:
        try:
            cmd_str = 'scp %s ychang@%s:/u0/ychang/%s' % (config.web_server_machine,
                                                          config.aslist_file,
                                                          config.aslist_file.split('/')[-1])
            sp.check_call(cmd_str, shell=True)
            try:
                cmd_str = ''' ssh ychang@%s '/opt/anaconda/bin/sqlite3 %s < /u0/ychang/aslist_init_and_import.sql' ''' % (config.web_server_machine,
                                                                                                                          config.case_view_hour_db)
                sp.check_call(cmd_str, shell=True)
            except:
                logger.error('error when inserting to DB on VM')
        except:
            logger.error('error when uploading to scidb02 VM')
    else:
        logger.error('updating by query ASlist failed')


if __name__ == '__main__':
    sys.exit(main())