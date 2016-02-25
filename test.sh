#!/bin/bash

sql2 -q mega.mapnoccfive.query.akadns.net "SELECT region,maprule_id,SUM(bytes) bytes,SUM(bytes_in) bytes_in FROM maprule_info d,mcm_machines_v2 e WHERE ghostip = ip GROUP BY 1, 2" > testout.txt
#SELECT maprule,geoname,netname,MIN(targetvalue) targetvalue FROM mrpm_maprule_qos_objectives WHERE attrname = 'IN-OUT-RATIO' GROUP BY 1, 2, 3" > testout.txt
# 40s - 3m
#SELECT MAX(casename) casename,mapruleid,geo,aslist,regionid,MAX(percentage) percentage FROM a_maprule_qos_view_by_region GROUP BY 2, 3, 4, 5" > testout.txt
# 3s
