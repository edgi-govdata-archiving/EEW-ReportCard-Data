 [![Code of Conduct](https://img.shields.io/badge/%E2%9D%A4-code%20of%20conduct-blue.svg?style=flat)](https://github.com/edgi-govdata-archiving/overview/blob/main/CONDUCT.md)

# EEW Report Card Data Collection
This repository contains code and scripts for gathering data from the Stonybrook University ECHO database, filtering it
to make it available for the generation of Rmd report cards.

# Developer setup
* Python and SQLite are required.
* The ECHO_modules repository, must be checked out into the home directory of this project. Currently we are using the watershed-geo branch of ECHO_modules.
* The latest leg_info.db SQLite database of legislator information, from the EEW-Report-Making repository, needs to be placed into the home directory of this project.

# Getting data from the Stonybrook University (SBU) ECHO database 
Current thinking is that monthly we will extract the data needed for all Congressional and State report cards from the Stonybrook database.  This will be stored in a local SQLite database.
The extraction is performed by the AllPrograms.py program.  It is run as follows:
* python AllPrograms.py -c state_cd-9.csv -f 2020
The -c argument is a list of comma-separated state, CD number pairs (e.g. AL,1).  The -f option specifies the focus year of the data, which is generally the last full year of reliable data. (In 2021 we are specifying 2020 as the focus year.)
Because of the large number of Congressional districts and the volume of data that AllPrograms.py extracts and holds in memory, the list of CDs to be processed must be reduced to around 60 per run.  The CDs have been divided into 9 separate state_cd-X.csv files so that AllPrograms.py can process them in smaller chunks.  All of these can be run in sequence with the run_AllPrograms.sh shell script.
The AllPrograms.py program writes into tables in the local region.db SQLite database.
* TBD - The goal is to make the monthly run of the run_AllPrograms.sh an automated cron job.
* TBD - The clean_regions.sql needs to be run prior to getting new monthly data with run_AllPrograms.sh.  It might be incorporated into the run_AllPrograms.sh script.
* TBD - We should also archive the regions.db before cleaning it.  

# Local region.db SQLite database
* Tables
* The schema is in region_db.schema.  

# Using Regions.py to get data from regions.db in R
* Functions available through Regions.py
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
active_facilities <- region$get_active_facilities()
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
```





---

## License & Copyright

Copyright (C) <year> Environmental Data and Governance Initiative (EDGI)
This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, version 3.0.

This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

See the [`LICENSE`](/LICENSE) file for details.
