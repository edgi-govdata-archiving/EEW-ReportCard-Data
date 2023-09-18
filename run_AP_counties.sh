#!/bin/bash

source ~/.profile
cd ${EEW_HOME}

CURRENTDATE=`date +"%Y-%m-%d"`
SAVENAME=region_counties.db-${CURRENTDATE}
LOGNAME=AllPrograms.log-${CURRENTDATE}
ERRORNAME=AllPrograms.error-${CURRENTDATE}
mv AllPrograms.log ${LOGNAME}
mv AllPrograms.error ${ERRORNAME}

echo 'Saving the current region_counties.db' > AllPrograms.log 2> AllPrograms.error
cp region_counties.db ${SAVENAME}
date >> AllPrograms.log
echo 'Saving the log file from the last population of region_counties.db' >> AllPrograms.log

echo 'Cleaning out the affected tables' >> AllPrograms.log
sqlite3 region_counties.db < clean_regions.sql

echo 'Running the AllPrograms.py commands to populate region_counties.db' >> AllPrograms.log

pip install --update git+https://github.com/edgi-govdata-archiving/ECHO_modules

date >> AllPrograms.log
python3 AllPrograms.py -c -f 2022 >> AllPrograms.log 2>> AllPrograms.error
if ! python3 check_AllPrograms.py -c; then
    echo "Error on $file, retrying" >> AllPrograms.error;
    date >> AllPrograms.log
    python3 AllPrograms.py -c -f 2022 >> AllPrograms.log 2>> AllPrograms.error
    if ! python3 check_AllPrograms.py -c $file; then
        echo "Error on retrying, failure" >> AllPrograms.error;
  	fi
    else 
        echo "found no errors" >> AllPrograms.log;
    fi