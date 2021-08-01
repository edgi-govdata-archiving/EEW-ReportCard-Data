import sqlite3
import pandas as pd

'''
This program builds or re-builds the real_cds table of regions.db
from the CDs it finds in the leg_info.db legislators table
'''

leg_conn = sqlite3.connect( 'leg_info.db' )
region_conn = sqlite3.connect( 'region.db' )
region_cursor = region_conn.cursor()

clean_sql = 'delete from real_cds'
region_cursor.execute( clean_sql )

region_sql = 'insert into real_cds (state,cd) values (\'{}\', {})'

leg_sql = 'select distinct(cd_state) from legislators'

leg_df = pd.read_sql_query( leg_sql, leg_conn )

for idx, row in leg_df.iterrows():
    state_cd = row['cd_state']
    state = state_cd[:2]
    cd = state_cd[2:]
    if len(cd) > 0:
        region_cursor.execute( region_sql.format(state, int(cd)) )
region_conn.commit()