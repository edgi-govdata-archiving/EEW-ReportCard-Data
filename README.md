 [![Code of Conduct](https://img.shields.io/badge/%E2%9D%A4-code%20of%20conduct-blue.svg?style=flat)](https://github.com/edgi-govdata-archiving/overview/blob/main/CONDUCT.md)

# EEW Report Card Data Collection
This repository contains code and scripts for gathering data from the Stonybrook University ECHO database and other web sources, filtering it to make it available for the generation of R Markdown report cards.

# Developer setup
* [Python](https://www.python.org/) and [SQLite](https://sqlite.org/index.html) are required.
* The [ECHO_modules repository](https://github.com/edgi-govdata-archiving/ECHO_modules) must be checked out into the home directory of this project. Currently we are using the watershed-geo branch of ECHO_modules. (From within the EEW-ReportCard-Data home directory, clone ECHO_modules and checkout the watershed-geo branch.)
* The latest leg_info.db SQLite database of legislator information, from the [EEW-Report-Making](https://github.com/edgi-govdata-archiving/EEW-Report-Making) repository, needs to be placed into the home directory of this project. (From within the EEW-ReportCard-Data home directory, clone EEW-Report-Making.)

# Overall Process
This diagram shows the overall process of getting the data for the report cards.  It looks complicated, but many of the steps
only need to be run when congressional districts change, or when legislators change.
![alt text](https://github.com/edgi-govdata-archiving/EEW-ReportCard-Data/blob/main/ReportCard-process-flow.png?raw=true)

# Very Occasional Tasks
Most of the data gathering shown in the diagram is done infrequently.  The _leg_info.py_ script that builds the leg_info.db SQLite database of legislator data only needs to be run when legislators change.  The same is true of the _get_leg_image.py_ script that gets the images for the current legislators.  The _RegionMap.py_ program that creates maps of the congressional districts and states only needs to be run when districts are re-drawn.

# Getting data from the Stonybrook University (SBU) ECHO database - _AllPrograms.py_
Monthly we will extract the data needed for all Congressional and State report cards from the Stonybrook database.  This is stored in a local SQLite database, _region.db_.
The extraction is performed by the _AllPrograms.py_ program.  _AllPrograms.py_ must gather a large volume of data in order to filter
it for the report cards, so it isn't able to process all Congressional districts at once.  The list of CDs to be processed for a single run of AllPrograms.py must be reduced to around 60 per run. 

The -c argument to _AllPrograms.py_ is a list of comma-separated state, CD number pairs (e.g. AL,1).  The -f option specifies the focus year of the data, which is generally the last full year of reliable data. (In 2021 we are specifying 2020 as the focus year.)

A script _run_AllPrograms.sh_ runs _AllPrograms.py_ with batches of congressional districts.  The districts are listed in 9 files _state_cd-X.csv_, where X is 1 to 9. The script is run as follows:
* run_AllPrograms.sh

_run_AllPrograms.sh_ first backs up the current _region.db_ database, appending the current date to the file name. Then it cleans out the tables that will be re-populated by running _AllPrograms.py_. 

A log file, _AllPrograms.log_, can be viewed to determine if there was any problem encountered in running the _AllPrograms.py_ on any of the _state_cd-X.csv_ files.

The _AllPrograms.py_ program writes into tables in the local _region.db_ SQLite database. The schema for this database is in _region_db.schema_.
* TBD - The goal is to make the monthly run of the run_AllPrograms.sh an automated cron job.
* TBD - The clean_regions.sql needs to be run prior to getting new monthly data with run_AllPrograms.sh.  It might be incorporated into the run_AllPrograms.sh script.
* TBD - We should also archive the regions.db before cleaning it.  

## Local region.db SQLite database

The schema of this small, local database is in _region_db.schema_.  The tables in the database are:
* regions - This identifies all of the regions (congressional districts) for which data exists. All other tables link via the regions table's rowid index.
* active_facilities - the count of facilities for each program--CAA, CWA, RCRA, GHG
* per_fac - counts of violations, etc. (type) by program (CAA, etc.) by year, per facility
* violations - counts of violations by program by year
* enforcements - counts of enforcements and penalty amounts by program by year
* ghg_emissions - amounts by year
* non_compliants - facilities, quarters of non-compliance, formal actions, URL, latitude and longitude by program
* violations_by_facilities - number of facilities and non-compliant quarters by program
* enf_per_fac - number of facilities, count of enforcements, amount of enforcements by year and program
* inflation - yearly inflation factors

## Using Regions.py to get data from regions.db in R
In the .Rmd templates, the _reticulate_ R package is employed to allow the use of the Python _Region.py_ code in the R Markdown file.

The _Region_ object is imported, and the constructor is called with the _type_ parameter set to 'State' or 'Congressional District', _value_ set to the CD number (or omitted for states) and _state_ set as expected.  The _region_ variable created can then be used to request data through the functions provided.

* Functions available through _Regions.py_ include:
```R
u <- import( 'Region' )
# region <- u$Region(type='State', state='TX')
region <- u$Region(type='Congressional District', value='34', state='TX')
USAinspectionsper1000_All <- region$get_per_1000( 'inspections', 'USA', 2020 )
inspectionsper1000_state <- region$get_per_1000( 'inspections', 'State', 2020 )
inspectionsper1000_cd <- region$get_per_1000( 'inspections', 'CD', 2020 )
USAviolationsper1000_All <- region$get_per_1000( 'violations', 'USA', 2020 )
violationsper1000_state <- region$get_per_1000( 'violations', 'State', 2020 )
violationsper1000_cd <- region$get_per_1000( 'violations', 'CD', 2020 )
inflation <- region$get_inflation( 2020 )
CWAper1000 <- region$get_cwa_per_1000( 2020 )
violations <- region$get_events( 'violations', 'All', 2020 )
CAAviolations <- region$get_events( 'violations', 'CAA', 2020 )
CWAviolations <- region$get_events( 'violations', 'CWA', 2020 )
RCRAviolations <- region$get_events( 'violations', 'RCRA', 2020 )
enforcement <- region$get_events( 'enforcements', 'All', 2020 )
CAAenforcement <- region$get_events( 'enforcements', 'CAA', 2020 )
CWAenforcement <- region$get_events( 'enforcements', 'CWA', 2020 )
RCRAenforcement <- region$get_events( 'enforcements', 'RCRA', 2020 )
CAArecurring <- region$get_recurring_violations( 'CAA' )
CWArecurring <- region$get_recurring_violations( 'CWA' )
RCRArecurring <- region$get_recurring_violations( 'RCRA' )
inspections <- region$get_events( 'inspections', 'All', 2020 )
CAAinspections <- region$get_events( 'inspections', 'CAA', 2020 )
CWAinspections <- region$get_events( 'inspections', 'CWA', 2020 )
RCRAinspections <- region$get_events( 'inspections', 'RCRA', 2020 )
CAA_active_facilities <- region$get_active_facilities('CAA')
CWA_active_facilities <- region$get_active_facilities('CWA')
RCRA_active_facilities <- region$get_active_facilities('RCRA')
```

---

## License & Copyright

Copyright (C) <year> Environmental Data and Governance Initiative (EDGI)
This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.0.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the [`LICENSE`](/LICENSE) file for details.
