#!/usr/bin/env bash
for i in {1..10}; do /a/bin/sql2 --csv ' select * from a_maprule_qos_view_by_region ' >  /home/testgrp/full-table-mrqos-view-by-region; if [ `wc -l /home/testgrp/full-table-mrqos-view-by-region | awk '{print $1;}'` -gt "10" ]; then break; fi; done;
