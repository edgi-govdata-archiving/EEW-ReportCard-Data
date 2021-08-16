#!/bin/bash

cd /home/steve/EEW-ReportCard-Data/

# Copy the exisiting active_facilities table to 
# active_facilities_previous

sqlite3 region.db < backup_active_facilities.sql
