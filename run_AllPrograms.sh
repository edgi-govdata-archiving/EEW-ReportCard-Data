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

for file in `ls state_cd-*.csv`
do
    date >> AllPrograms.log
    python3 AllPrograms.py -c $file -f 2020 >> AllPrograms.log 2>> AllPrograms.error
    if ! python3 check_AllPrograms.py -c $file; then 
        echo "Error on $file, retrying" >> AllPrograms.error; 
        date >> AllPrograms.log
        python3 AllPrograms.py -c $file -f 2020 >> AllPrograms.log 2>> AllPrograms.error
        if ! python3 check_AllPrograms.py -c $file; then 
            echo "Error on retrying $file, failure" >> AllPrograms.error; 
	fi
    else 
        echo "$file found no errors" >> AllPrograms.log; 
    fi
done


