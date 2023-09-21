#!/bin/bash

source ~/.profile
cd ${EEW_HOME}

CURRENTDATE=`date +"%Y-%m-%d"`
SAVENAME=region.db-${CURRENTDATE}
LOGNAME=AllPrograms.log-${CURRENTDATE}
ERRORNAME=AllPrograms.error-${CURRENTDATE}
mv AllPrograms.log ${LOGNAME}
mv AllPrograms.error ${ERRORNAME}

echo 'Saving the current region.db' > AllPrograms.log 2> AllPrograms.error
cp region.db ${SAVENAME}
date >> AllPrograms.log
echo 'Saving the log file from the last population of region_counties.db' >> AllPrograms.log

echo 'Cleaning out the affected tables' >> AllPrograms.log
sqlite3 region.db < clean_regions.sql

echo 'Running the AllPrograms.py commands to populate region_counties.db' >> AllPrograms.log

# pip install --upgrade git+https://github.com/edgi-govdata-archiving/ECHO_modules

date >> AllPrograms.log

states_file=$1
echo "Reading $states_file"
while read -r state; do
    # Reading line by line
    echo "$state"
    # python3 AllPrograms.py -c $state -f 2022 >> AllPrograms.log 2>> AllPrograms.error
    python3 AllPrograms.py -c $state -f 2022 
    #TODO: Modify check_AllPrograms.py so it can be used here to verify success
    # for the given state
done < $states_file

