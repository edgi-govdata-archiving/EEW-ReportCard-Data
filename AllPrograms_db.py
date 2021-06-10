import pdb

import sqlite3
import AllPrograms_util
from ECHO_modules.get_data import get_echo_data

def get_region_rowid( cursor, state, cd ):
    sel_sql = 'select rowid from regions where region_type=\'{}\' and state=\'{}\''
    sel_cd_sql = sel_sql + ' and region = \'{}\''
    sel_state_sql = sel_sql + ' and region = \'\''
    ins_sql = 'insert into regions (region_type,state,region) values (\'{}\',\'{}\',\'{}\')'
    type = 'CD'
    if ( cd is not None ):
        sql = sel_cd_sql.format( 'CD', state, str(cd).zfill(2) )
    else:
        type = 'State'
        sql = sel_state_sql.format( 'State', state )
    cursor.execute( sql )
    result = cursor.fetchone()
    if ( result is not None ):
        return result[0]
    else:
        cd_str = '' if cd is None else  str( cd ).zfill(2)
        sql = ins_sql.format( type, state, cd_str )
        cursor.execute( sql )
        return cursor.lastrowid

def write_active_facs( active_facs, state, cd=None ):
    ins_sql = 'insert into active_facilities (region_id,program,count) values ({},\'{}\',{})'
    ins_sql += ' on conflict(region_id,program) do update set count = {}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    rowid = get_region_rowid( cursor, state, cd ) 
    for program, value in active_facs.items():
        sql = ins_sql.format( rowid, program, value, value )
        cursor.execute( sql )
    conn.commit()

def write_recurring_violations( state, cd, viol_list ):
    ins_sql = 'insert into recurring_violations (region_id,program,facilities,violations) values ({},\'{}\',{},{})'
    ins_sql += ' on conflict(region_id,program) do update set facilities = {}, violations = {}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()
    
    rowid = get_region_rowid( cursor, state, cd ) 
    # pdb.set_trace()
    if ( viol_list is not None ):
        for row in viol_list:
            sql = ins_sql.format( rowid, row[0], row[1], row[2], row[1], row[2] )
            cursor.execute( sql )
    conn.commit()

def write_violations( program, ds, ds_type ):
    ins_sql = 'insert into violations (region_id,program,year,count) values ({},\'{}\',{},{})'
    ins_sql += ' on conflict(region_id,program) do update set count = {}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = get_region_rowid( cursor, state, cd ) 
    df_pgm = AllPrograms_util.get_events( ds, ds_type )
    if ( df_pgm is not None ):
        # idx will be the year
        for idx, row in df_pgm.iterrows():
            sql = ins_sql.format( rowid, program, idx, row['Count'], row['Count'])
            cursor.execute( sql )
    conn.commit()
    return df_pgm

def write_CWA_violations( df, ds_type ):
    ins_sql = 'insert into violations (region_id,program,year,count) values ({},\'{}\',{},{})'
    ins_sql += ' on conflict(region_id,program,year) do update set count = {}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = get_region_rowid( cursor, state, cd ) 
    if ( df is not None ):
        # idx will be the year
        for idx, row in df.iterrows():
            sql = ins_sql.format( rowid, 'CWA', idx, row['Total'], row['Total'])
            cursor.execute( sql )
    conn.commit()

def write_inspections( program, ds, ds_type ):
    ins_sql = 'insert into inspections (region_id,program,year,count) values ({},\'{}\',{},{})'
    ins_sql += ' on conflict(region_id,program,year) do update set count = {}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = get_region_rowid( cursor, state, cd ) 
    df_pgm = AllPrograms_util.get_inspections( ds, ds_type )
    # pdb.set_trace()
    if ( df_pgm is not None ):
        # idx will be the year
        for idx, row in df_pgm.iterrows():
            sql = ins_sql.format( rowid, program, idx, row['Count'], row['Count'])
            cursor.execute( sql )
    conn.commit()
    return df_pgm

def write_total_inspections( program, df_pgm, ds_type ):
    ins_sql = 'insert into inspections (region_id,program,year,count) values ({},\'{}\',{},{})'
    ins_sql += ' on conflict(region_id,program,year) do update set count = {}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = get_region_rowid( cursor, state, cd ) 
    # pdb.set_trace()
    if ( df_pgm is not None ):
        # idx will be the year
        for idx, row in df_pgm.iterrows():
            sql = ins_sql.format( rowid, program, idx, row['Count'], row['Count'])
            cursor.execute( sql )
    conn.commit()

def write_enforcements( program, ds, ds_type ):
    ins_sql = 'insert into enforcements (region_id,program,year,amount,count) '
    ins_sql += 'values ({},\'{}\',{},{},{})'
    ins_sql += ' on conflict(region_id,program,year) do update set amount={}, count={}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = get_region_rowid( cursor, state, cd ) 
    df_pgm = AllPrograms_util.get_enforcements( ds, ds_type )
    # pdb.set_trace()
    if ( df_pgm is not None ):
        # idx will be the year
        for idx, row in df_pgm.iterrows():
            sql = ins_sql.format( rowid, program, idx, row['Amount'], row['Count'], 
                   row['Amount'], row['Count'])
            cursor.execute( sql )
    conn.commit()
    return df_pgm

def write_total_enforcements( program, df_pgm, ds_type ):
    ins_sql = 'insert into enforcements (region_id,program,year,amount,count) '
    ins_sql += 'values ({},\'{}\',{},{},{})'
    ins_sql += ' on conflict(region_id,program,year) do update set amount={}, count={}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = get_region_rowid( cursor, state, cd ) 
    # pdb.set_trace()
    if ( df_pgm is not None ):
        # idx will be the year
        for idx, row in df_pgm.iterrows():
            sql = ins_sql.format( rowid, program, idx, row['Amount'], row['Count'], 
                   row['Amount'], row['Count'])
            cursor.execute( sql )
    conn.commit()

def write_per_fac( program, ds_type, event, year, count ):
    ins_sql = 'insert into per_fac (region_id,program,type,year,count) '
    ins_sql += 'values ({},\'{}\',\'{}\',{},{})'
    ins_sql += ' on conflict(region_id,program,type,year) do update set count={}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = get_region_rowid( cursor, state, cd ) 
    # pdb.set_trace()
    sql = ins_sql.format( rowid, program, event, year, count, count )
    cursor.execute( sql )
    conn.commit()

def write_enf_per_fac( program, ds, ds_type, num_fac, year ):
    ins_sql = 'insert into enf_per_fac (region_id,program,year,count,amount,num_fac)'
    ins_sql += ' values ({},\'{}\',{},{},{},{}) on conflict(region_id,program,year)'
    ins_sql += ' do update set count={}, amount={}, num_fac={}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = get_region_rowid( cursor, state, cd ) 
    df_pgm = AllPrograms_util.get_enf_per_fac( ds, ds_type, num_fac, year )
    if ( df_pgm is not None and not df_pgm.empty ):
        sql = ins_sql.format( rowid, program, year, df_pgm.Count, 
                    df_pgm.Amount, num_fac, df_pgm.Count, df_pgm.Amount, num_fac )
        cursor.execute( sql )
    conn.commit()
    return df_pgm

        # Removed total_enf_per_fac.  It can be calculated from the individual
        # program records

def write_ghg_emissions( df, ds_type ):
    ins_sql = 'insert into ghg_emissions (region_id,year,amount) '
    ins_sql += 'values ({},{},{}) on conflict(region_id,year)'
    ins_sql += ' do update set amount={}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = get_region_rowid( cursor, state, cd ) 
    if ( df is not None ):
        # idx will be the year
        for idx, row in df.iterrows():
            sql = ins_sql.format( rowid, idx, row['Amount'], 
                    row['Amount'])
            cursor.execute( sql )
    conn.commit()

def write_top_violators( df, ds_type, program ):
    ins_sql = 'insert into non_compliants (region_id,program,fac_name,'
    ins_sql += ' noncomp_count,formal_action_count,dfr_url,fac_lat,fac_long)'
    ins_sql += 'values ({},\'{}\',"{}",{},{},\'{}\',{},{})'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = get_region_rowid( cursor, state, cd ) 
    if ( df is not None ):
        # idx will be the year
        for idx, row in df.iterrows():
            sql = ins_sql.format( rowid, program, row['FAC_NAME'],
                row['noncomp_count'],row['formal_action_count'],
                row['DFR_URL'],row['FAC_LAT'],row['FAC_LONG'] )
            cursor.execute( sql )
    conn.commit()

def write_violations_by_facilities( df, ds_type, program, action_field, flag,
                            noncomp_field ):
    ins_sql = 'insert into violations_by_facilities (region_id,program,'
    ins_sql += ' noncomp_qtrs,num_facilities) '
    ins_sql += ' values ({},\'{}\',{},{})'
    ins_sql += ' on conflict (region_id, program, noncomp_qtrs)'
    ins_sql += ' do update set noncomp_qtrs={}, num_facilities={}'

    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = get_region_rowid( cursor, state, cd ) 
    df = AllPrograms_util.get_violations_by_facilities( df, action_field,
                    flag, noncomp_field )
    if ( df is not None ):
        # idx will be the noncomp_qtrs
        for idx, row in df.iterrows():
            sql = ins_sql.format( rowid, program, idx,
                row['num_facilities'], idx, row['num_facilities'] )
            cursor.execute( sql )
    conn.commit()

def write_single_cd_states():
    ins_sql = 'insert into single_cd_states (state,cd) values (\'{}\', {})'
    conn = sqlite3.connect( 'region.db' )
    cursor = conn.cursor()
    single_cd_states = ['DE','VT','MT','AK', 'WY', 'ND', 'SD', 'VI', 'PR', 'MP', 'GU', 'AS', 'DC']
    for state in single_cd_states:
        sql = 'select distinct("FAC_DERIVED_CD113") from "ECHO_EXPORTER" where "FAC_STATE" = \'{}\''
        sql = sql.format(state)
        df = get_echo_data( sql )
        for idx, row in df.iterrows():
            sql = ins_sql.format( state, row['FAC_DERIVED_CD113'] )
            cursor.execute( sql )
    conn.commit()


test_facs = { 'XXX': 14, 'YYY': 20, 'ZZZ': 30 }
write_active_facs( test_facs, 'MX', 4 )
test_facs = { 'XXX': 15, 'YYY': 23, 'ZZZ': 33 }
write_active_facs( test_facs, 'MX', 4 )
write_active_facs( test_facs, 'MX' )
