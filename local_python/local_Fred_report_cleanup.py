import sys,os
from datetime import date
import subprocess as sp
import glob
import re
import shutil

def main():
    # date today
    td = date.today()
    flist = sorted( glob.glob( os.path.join('/var/www/txt/','Fred2WReport.2017*.csv') ) )
    y2016_flag = [False]*100
    y2017_flag = [False]*100
    for fileitem in flist:
        datex = re.split('\.',fileitem)[1]
        datet = re.split('-',datex)
        datef = date( int(datet[0]), int(datet[1]), int(datet[2]))
        datec = datef.isocalendar()
        #print datet
        #print date( int(datet[0]), int(datet[1]), int(datet[2])).isocalendar()
#        if datec[0]==2016:
#            if not (y2016_flag[datec[1]]):
#                print 'keep ' + fileitem
#                y2016_flag[datec[1]] = True
#            else:
#                if (td-datef).days > 7:
#                    print 'discard ' + fileitem
#                    os.remove(fileitem)
#                else:
#                    print 'keep ' + fileitem
#                    y2016_flag[datec[1]] = True
        if datec[0]==2017:
            if not (y2017_flag[datec[1]]):
                print 'keep ' + fileitem
                y2017_flag[datec[1]] = True
            else:
                if (td-datef).days > 7:
                    print 'discard ' + fileitem
                    os.remove(fileitem)
                else:
                    print 'keep ' + fileitem
                    y2017_flag[datec[1]] = True

    flist = sorted( glob.glob( os.path.join('/var/www/txt/','Fred3DReport.2017*.csv') ) )
    y2016_flag = [False]*100
    y2017_flag = [False]*100
    for fileitem in flist:
        datex = re.split('\.',fileitem)[1]
        datet = re.split('-',datex)
        datef = date( int(datet[0]), int(datet[1]), int(datet[2]))
        datec = datef.isocalendar()
        #print datet
        #print date( int(datet[0]), int(datet[1]), int(datet[2])).isocalendar()
#        if datec[0]==2016:
#            if not (y2016_flag[datec[1]]):
#                print 'keep ' + fileitem
#                y2016_flag[datec[1]] = True
#            else:
#                if (td-datef).days > 7:
#                    print 'discard ' + fileitem
#                    os.remove(fileitem)
#                else:
#                    print 'keep ' + fileitem
#                    y2016_flag[datec[1]] = True
        if datec[0]==2017:
            if not (y2017_flag[datec[1]]):
                print 'keep ' + fileitem
                y2017_flag[datec[1]] = True
            else:
                if (td-datef).days > 7:
                    print 'discard ' + fileitem
                    os.remove(fileitem)
                else:
                    print 'keep ' + fileitem
                    y2017_flag[datec[1]] = True

if __name__=='__main__':
    sys.exit(main())
