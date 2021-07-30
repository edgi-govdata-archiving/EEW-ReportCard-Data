#!/bin/bash

CURRENTDATE=`date +"%Y-%m-%d"`
SAVENAME=leg_info.db-${CURRENTDATE}
IMAGESNAME=CD_images-${CURRENTDATE}.tar.xz
echo 'Saving the current leg_info.db' > leg_info.log
date >> leg_info.log
cp leg_info.db ${SAVENAME}
echo 'Saving the current leg images' >> leg_info.log
tar cf - CD_images | xz -e > ${IMAGESNAME}
rm CD_images/*
echo 'Saving the log file from the last population of leg_info.db' >> leg_info.log
SAVENAME=leg_info.log-${CURRENTDATE}

echo 'Running the leg_info.py commands to populate leg_info.db' >> leg_info.log
date >> leg_info.log
python3 leg_info.py >> leg_info.log
date >> leg_info.log
