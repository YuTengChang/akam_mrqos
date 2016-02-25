#!/bin/bash

origin=`date -u +%H`; 
cat /home/testgrp/MRQOS/geolistmap.txt | awk -v var=$origin '{temp=(24+var+$2)%24; if(temp>=18){print $1,"1";}else{print $1,"0";}}' > /home/testgrp/MRQOS/geopeak.txt;
cp /home/testgrp/MRQOS/mrqos_data/score.tmp /home/testgrp/MRQOS/mrqos_data/score1.tmp
awk 'FNR==NR{a[$1]=$2; next}FS=","{print $0","a[$3]}' /home/testgrp/MRQOS/geopeak.txt /home/testgrp/MRQOS/mrqos_data/score1.tmp | sed 's/,$/\,0/' > /home/testgrp/MRQOS/mrqos_data/score.tmp
