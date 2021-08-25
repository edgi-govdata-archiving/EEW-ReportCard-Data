#!/bin/bash

source ~/.profile
cd ${EEW_HOME}

# Copy the exisiting active_facilities table to 
# active_facilities_previous in region.db

sqlite3 region.db < backup_active_facilities.sql
