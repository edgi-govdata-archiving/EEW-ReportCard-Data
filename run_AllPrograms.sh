#!/bin/bash

CURRENTDATE=`date +"%Y-%m-%d"`
SAVENAME=region.db-${CURRENTDATE}
echo 'Saving the current region.db'
cp region.db ${SAVENAME}
echo 'Saving the log file from the last population of region.db'
SAVENAME=AllPrograms.log-${CURRENTDATE}

echo 'Cleaning out the affected tables'
sqlite3 region.db < clean_regions.sql

echo 'Running the AllPrograms.py commands to populate region.db'
python AllPrograms.py -c state_cd-1.csv -f 2020 > AllPrograms.log
python AllPrograms.py -c state_cd-2.csv -f 2020 >> AllPrograms.log
python AllPrograms.py -c state_cd-3.csv -f 2020 >> AllPrograms.log
python AllPrograms.py -c state_cd-4.csv -f 2020 >> AllPrograms.log
python AllPrograms.py -c state_cd-5.csv -f 2020 >> AllPrograms.log
python AllPrograms.py -c state_cd-6.csv -f 2020 >> AllPrograms.log
python AllPrograms.py -c state_cd-7.csv -f 2020 >> AllPrograms.log
python AllPrograms.py -c state_cd-8.csv -f 2020 >> AllPrograms.log
python AllPrograms.py -c state_cd-9.csv -f 2020 >> AllPrograms.log
