#!/bin/bash

cd /home/steve/EEW-ReportCard-Data/

CURRENTDATE=`date +"%Y-%m-%d"`
SAVENAME=region.db-${CURRENTDATE}
LOGNAME=AllPrograms.log-${CURRENTDATE}
ERRORNAME=AllPrograms.error-${CURRENTDATE}
mv AllPrograms.log ${LOGNAME}
mv AllPrograms.error ${ERRORNAME}

echo 'Saving the current region.db' > AllPrograms.log 2> AllPrograms.error
cp region.db ${SAVENAME}
date >> AllPrograms.log
echo 'Saving the log file from the last population of region.db' >> AllPrograms.log

echo 'Cleaning out the affected tables' >> AllPrograms.log
sqlite3 region.db < clean_regions.sql

echo 'Running the AllPrograms.py commands to populate region.db' >> AllPrograms.log
date >> AllPrograms.log
python3 AllPrograms.py -c state_cd-1.csv -f 2020 >> AllPrograms.log 2>> AllPrograms.error
date >> AllPrograms.log
python3 AllPrograms.py -c state_cd-2.csv -f 2020 >> AllPrograms.log 2>> AllPrograms.error
date >> AllPrograms.log
python3 AllPrograms.py -c state_cd-3.csv -f 2020 >> AllPrograms.log 2>> AllPrograms.error
date >> AllPrograms.log
python3 AllPrograms.py -c state_cd-4.csv -f 2020 >> AllPrograms.log 2>> AllPrograms.error
date >> AllPrograms.log
python3 AllPrograms.py -c state_cd-5.csv -f 2020 >> AllPrograms.log 2>> AllPrograms.error
date >> AllPrograms.log
python3 AllPrograms.py -c state_cd-6.csv -f 2020 >> AllPrograms.log 2>> AllPrograms.error
date >> AllPrograms.log
python3 AllPrograms.py -c state_cd-7.csv -f 2020 >> AllPrograms.log 2>> AllPrograms.error
date >> AllPrograms.log
python3 AllPrograms.py -c state_cd-8.csv -f 2020 >> AllPrograms.log 2>> AllPrograms.error
date >> AllPrograms.log
python3 AllPrograms.py -c state_cd-9.csv -f 2020 >> AllPrograms.log 2>> AllPrograms.error
date >> AllPrograms.log
