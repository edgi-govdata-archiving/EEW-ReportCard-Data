import pdb

import pandas as pd
import geopandas
import sqlite3
import AllPrograms_util
from ECHO_modules.get_data import get_echo_data
from Region import Region, get_inflation


def get_active_facs(mode, state, cds_or_counties):
    # cds_or_counties is either a list of the counties in the state,
    # or a shapefile with CD boundaries
    columns = '"REGISTRY_ID", "FAC_NAME", "FAC_STATE", "FAC_COUNTY", \
        "DFR_URL", "FAC_LAT", "FAC_LONG", \
        "FAC_ACTIVE_FLAG", "AIR_FLAG", "NPDES_FLAG", "RCRA_FLAG", \
        "GHG_FLAG", "CAA_3YR_COMPL_QTRS_HISTORY", \
        "CWA_13QTRS_COMPL_HISTORY", "RCRA_3YR_COMPL_QTRS_HISTORY", \
        "CAA_FORMAL_ACTION_COUNT", "CWA_FORMAL_ACTION_COUNT", \
        "RCRA_FORMAL_ACTION_COUNT", "wkb_geometry"'
    sql = 'select {} from "ECHO_EXPORTER" where "FAC_STATE" = '.format(columns)
    sql += '\'{}\''.format(state)
    state_echo_data = get_echo_data(sql, "REGISTRY_ID")
    state_echo_data = state_echo_data[~state_echo_data['wkb_geometry'].isnull()]
    if mode == 'County':
        state_echo_data['cd_or_county'] = state_echo_data['FAC_COUNTY']
    if mode == 'Congressional District' and type(cds_or_counties) == geopandas.geodataframe.GeoDataFrame: 
        state_echo_data['geometry'] = geopandas.GeoSeries.from_wkb(
            state_echo_data['wkb_geometry'])
        state_echo_data.drop('wkb_geometry', axis=1, inplace=True)
        state_echo_data = geopandas.GeoDataFrame(state_echo_data, crs=4269)
        join = state_echo_data.sjoin(cds_or_counties, how="left")
        join['CD118FP'] = pd.to_numeric(join['CD118FP'], errors='coerce')
        state_echo_data = join
        '''
        join = state_echo_data.sjoin(cds_or_counties, how="left")
        state_echo_data = join.loc[join["CD118FP"] == float(region)]
        '''
    elif mode == 'State':
        state_echo_data['geometry'] = geopandas.GeoSeries.from_wkb(state_echo_data['wkb_geometry'])
        state_echo_data.drop('wkb_geometry', axis=1, inplace=True)
        state_echo_data = geopandas.GeoDataFrame(state_echo_data, crs=4269)
    state_echo_data = state_echo_data.loc[state_echo_data["FAC_ACTIVE_FLAG"] == "Y"]
    return state_echo_data

def get_exp_pgm(type):
    if type == 'CWA':
        return pd.read_csv('exp_to_npdes.csv', index_col='PGM_ID')

def get_real_cds(db, state):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    sql = "select cd from real_cds where state = '{}'".format(state)
    cursor.execute(sql)
    cds = cursor.fetchall()
    return cds

def write_active_facs(db, region_mode, active_facs, state, cd=None):
    ins_sql = (
        "insert into active_facilities (region_id,program,count) values ({},'{}',{})"
    )
    ins_sql += " on conflict(region_id,program) do update set count = {}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    for program, value in active_facs.items():
        sql = ins_sql.format(rowid, program, value, value)
        cursor.execute(sql)
    conn.commit()


def write_recurring_violations(db, region_mode, state, cd, viol_list):
    ins_sql = "insert into recurring_violations (region_id,program,violations,facilities)"
    ins_sql += " values ({},'{}',{},{})"
    ins_sql += (
        " on conflict(region_id,program) do update set facilities = {}, violations = {}"
    )

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    # pdb.set_trace()
    if viol_list is not None:
        for row in viol_list:
            sql = ins_sql.format(rowid, row[0], row[1], row[2], row[1], row[2])
            cursor.execute(sql)
    conn.commit()


def write_violations(db, program, ds, ds_type):
    ins_sql = (
        "insert into violations (region_id,program,year,count) values ({},'{}',{},{})"
    )
    ins_sql += " on conflict(region_id,program,year) do update set count = {}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    region_mode = ds_type[0]
    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    df_pgm = AllPrograms_util.get_events(ds, ds_type)
    if df_pgm is not None:
        # idx will be the year
        for idx, row in df_pgm.iterrows():
            sql = ins_sql.format(rowid, program, idx, row["Count"], row["Count"])
            cursor.execute(sql)
    conn.commit()
    return df_pgm


def write_CWA_violations(db, df, ds_type):
    ins_sql = (
        "insert into violations (region_id,program,year,count) values ({},'{}',{},{})"
    )
    ins_sql += " on conflict(region_id,program,year) do update set count = {}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    region_mode = ds_type[0]
    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    if df is not None:
        # idx will be the year
        for idx, row in df.iterrows():
            sql = ins_sql.format(rowid, "CWA", idx, row["Total"], row["Total"])
            cursor.execute(sql)
    conn.commit()


def write_inspections(db, program, ds, ds_type):
    ins_sql = (
        "insert into inspections (region_id,program,year,count) values ({},'{}',{},{})"
    )
    ins_sql += " on conflict(region_id,program,year) do update set count = {}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    region_mode = ds_type[0]
    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    df_pgm = AllPrograms_util.get_inspections(ds, ds_type)
    # pdb.set_trace()
    if df_pgm is not None:
        # idx will be the year
        for idx, row in df_pgm.iterrows():
            sql = ins_sql.format(rowid, program, idx, row["Count"], row["Count"])
            cursor.execute(sql)
    conn.commit()
    return df_pgm


'''
    # This can be calculated from the data for the programs.

def write_total_inspections(db, program, df_pgm, ds_type):
    ins_sql = (
        "insert into inspections (region_id,program,year,count) values ({},'{}',{},{})"
    )
    ins_sql += " on conflict(region_id,program,year) do update set count = {}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, state, cd)
    # pdb.set_trace()
    if df_pgm is not None:
        # idx will be the year
        for idx, row in df_pgm.iterrows():
            sql = ins_sql.format(rowid, program, idx, row["Count"], row["Count"])
            cursor.execute(sql)
    conn.commit()
'''


def write_enforcements(db, program, ds, ds_type, focus_year):
    ins_sql = "insert into enforcements (region_id,program,year,amount,count) "
    ins_sql += "values ({},'{}',{},{},{})"
    ins_sql += " on conflict(region_id,program,year) do update set amount={}, count={}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    
    region_mode = ds_type[0]
    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    df_pgm = AllPrograms_util.get_enforcements(ds, ds_type)
    inflation = get_inflation(db, focus_year)
    # pdb.set_trace()
    if df_pgm is not None:
        # idx will be the year
        for idx, row in df_pgm.iterrows():
            sql = ins_sql.format(
                rowid,
                program,
                idx,
                row["Amount"],
                row["Count"],
                row["Amount"],
                row["Count"],
            )
            cursor.execute(sql)
    conn.commit()
    return df_pgm


'''
    # This can be calculated from the data for the programs.

def write_total_enforcements(db, program, df_pgm, ds_type):
    ins_sql = "insert into enforcements (region_id,program,year,amount,count) "
    ins_sql += "values ({},'{}',{},{},{})"
    ins_sql += " on conflict(region_id,program,year) do update set amount={}, count={}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, state, cd)
    # pdb.set_trace()
    if df_pgm is not None:
        # idx will be the year
        for idx, row in df_pgm.iterrows():
            sql = ins_sql.format(
                rowid,
                program,
                idx,
                row["Amount"],
                row["Count"],
                row["Amount"],
                row["Count"],
            )
            cursor.execute(sql)
    conn.commit()
'''


def write_per_fac(db, program, ds_type, event, year, count):
    ins_sql = "insert into per_fac (region_id,program,type,year,count) "
    ins_sql += "values ({},'{}','{}',{},{})"
    ins_sql += " on conflict(region_id,program,type,year) do update set count={}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    region_mode = ds_type[0]
    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    # pdb.set_trace()
    sql = ins_sql.format(rowid, program, event, year, count, count)
    cursor.execute(sql)
    conn.commit()


def write_enf_per_fac(db, program, ds, ds_type, num_fac, year):
    ins_sql = "insert into enf_per_fac (region_id,program,year,count,amount,num_fac)"
    ins_sql += " values ({},'{}',{},{},{},{}) on conflict(region_id,program,year)"
    ins_sql += " do update set count={}, amount={}, num_fac={}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    region_mode = ds_type[0]
    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    df_pgm = AllPrograms_util.get_enf_per_fac(ds, ds_type, num_fac, year)
    if df_pgm is not None and not df_pgm.empty:
        sql = ins_sql.format(
            rowid,
            program,
            year,
            df_pgm.Count,
            df_pgm.Amount,
            num_fac,
            df_pgm.Count,
            df_pgm.Amount,
            num_fac,
        )
        cursor.execute(sql)
    conn.commit()
    return df_pgm

    # Removed total_enf_per_fac.  It can be calculated from the individual
    # program records


def write_ghg_emissions(db, region_mode, df, ds_type):
    ins_sql = "insert into ghg_emissions (region_id,year,amount) "
    ins_sql += "values ({},{},{}) on conflict(region_id,year)"
    ins_sql += " do update set amount={}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    if df is not None:
        # idx will be the year
        for idx, row in df.iterrows():
            sql = ins_sql.format(rowid, idx, row["Amount"], row["Amount"])
            cursor.execute(sql)
    conn.commit()


def write_top_violators(db, region_mode, df, ds_type, program):
    ins_sql = "insert into non_compliants (region_id,program,fac_name,"
    ins_sql += " noncomp_count,formal_action_count,dfr_url,fac_lat,fac_long)"
    ins_sql += "values ({},'{}',\"{}\",{},{},'{}',{},{})"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    state = ds_type[2]
    region = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, region)
    if df is not None:
        # idx will be the year
        for idx, row in df.iterrows():
            sql = ins_sql.format(
                rowid,
                program,
                row["FAC_NAME"],
                row["noncomp_count"],
                row["formal_action_count"],
                row["DFR_URL"],
                row["FAC_LAT"],
                row["FAC_LONG"],
            )
            cursor.execute(sql)
    conn.commit()


def write_violations_by_facilities(db, df, ds_type, program, 
                                   action_field, flag, noncomp_field):
    ins_sql = "insert into violations_by_facilities (region_id,program,"
    ins_sql += " noncomp_qtrs,num_facilities) "
    ins_sql += " values ({},'{}',{},{})"
    ins_sql += " on conflict (region_id, program, noncomp_qtrs)"
    ins_sql += " do update set noncomp_qtrs={}, num_facilities={}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    region_mode = ds_type[0]
    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    df = AllPrograms_util.get_violations_by_facilities(
        df, action_field, flag, noncomp_field
    )
    if df is not None:
        # idx will be the noncomp_qtrs
        for idx, row in df.iterrows():
            sql = ins_sql.format(
                rowid, program, idx, row["num_facilities"], idx, row["num_facilities"]
            )
            cursor.execute(sql)
    conn.commit()


def write_single_cd_states(db):
    ins_sql = "insert into single_cd_states (state,cd) values ('{}', {})"
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    single_cd_states = [
        "DE",
        "VT",
        "MT",
        "AK",
        "WY",
        "ND",
        "SD",
        "VI",
        "PR",
        "MP",
        "GU",
        "AS",
        "DC",
    ]
    for state in single_cd_states:
        sql = 'select distinct("FAC_DERIVED_CD113") from "ECHO_EXPORTER" where "FAC_STATE" = \'{}\''
        sql = sql.format(state)
        df = get_echo_data(sql)
        for idx, row in df.iterrows():
            sql = ins_sql.format(state, row["FAC_DERIVED_CD113"])
            cursor.execute(sql)
    conn.commit()

def clean_per_1000(db):
    """
    Clear all data from state_per_1000, cd_per_1000 and county_per_1000
    in preparation for rebuilding these tables.
    """
    conn = sqlite3.connect(db)
    sql = 'delete from {}'
    for table in ['state_per_1000', 'cd_per_1000', 'county_per_1000']:
        do_sql = sql.format(table)
        cur = conn.cursor()
        cur.execute(do_sql)
    conn.commit()

def make_per_1000(db, region_mode, focus_year):
    """
    Build the state_per_1000 and cd_per_1000 or county_per_1000
    tables with the get_all_per_1000() function for the five years
    ending with the focus year.
    
    Parameters
    ----------
    db : string
        The Sqlite database
    region_mode : string
        Either 'Congressional District' or 'County'
    focus_year : string
        The end year for the results.
    """

    exclude_states = ['AS', 'MX', 'GM', 'MB']
    start_year = int(focus_year) - 4
    total_df = get_all_per_1000(db, region_mode, start_year)
    for year in range(start_year + 1, int(focus_year)):
        df = get_all_per_1000(db, region_mode, year)
        for s in exclude_states:
            df = df[df['CD.State'] != s]
        df.set_index('CD.State')
        total_df['CAA.Insp.per.1000'] += df['CAA.Insp.per.1000']
        total_df['CAA.Viol.per.1000'] += df['CAA.Viol.per.1000']
        total_df['CAA.Enf.per.1000'] += df['CAA.Enf.per.1000']
        total_df['CWA.Insp.per.1000'] += df['CWA.Insp.per.1000']
        total_df['CWA.Viol.per.1000'] += df['CWA.Viol.per.1000']
        total_df['CWA.Enf.per.1000'] += df['CWA.Enf.per.1000']
        total_df['RCRA.Insp.per.1000'] += df['RCRA.Insp.per.1000']
        total_df['RCRA.Viol.per.1000'] += df['RCRA.Viol.per.1000']
        total_df['RCRA.Enf.per.1000'] += df['RCRA.Enf.per.1000']
    
    # Todo: Test this
    # For ranking the CDs, the single CD state needs its records copied to one
    # for Congressional District
    if region_mode == 'Congressional District':
        one_cd_states = ['AK', 'DC', 'DE', 'PR', 'ND', 'SD', 'VI', 'VT', 'WY']
        oner_df = pd.DataFrame()
        for s in one_cd_states:
            oner_df = pd.concat([oner_df, total_df[total_df['CD.State'] == s]], ignore_index=True)
            state_cd = '{}-00'.format(s)
            oner_df.loc[oner_df['CD.State'] == s, 'Region'] = 'Congressional District'
            oner_df.loc[oner_df['CD.State'] == s, 'CD.State'] = state_cd
        total_df = pd.concat([total_df, oner_df], ignore_index=True)
   
    total_df['CAA.Insp.per.1000'] /= 5
    total_df['CAA.Viol.per.1000'] /= 5
    total_df['CAA.Enf.per.1000'] /= 5
    total_df['CWA.Insp.per.1000'] /= 5
    total_df['CWA.Viol.per.1000'] /= 5
    total_df['CWA.Enf.per.1000'] /= 5
    total_df['RCRA.Insp.per.1000'] /= 5
    total_df['RCRA.Viol.per.1000'] /= 5
    total_df['RCRA.Enf.per.1000'] /= 5
    
    conn = sqlite3.connect(db)
    if region_mode == 'Congressional District':
        (state_per_1000, cd_per_1000) = AllPrograms_util.build_all_per_1000(region_mode, total_df)
    elif region_mode == 'County':
        (state_per_1000, county_per_1000) = AllPrograms_util.build_all_per_1000(region_mode, total_df)

    state_per_1000.to_sql(name="state_per_1000", con=conn, if_exists="replace")
    cd_per_1000.to_sql(name="cd_per_1000", con=conn, if_exists="replace")
    county_per_1000.to_sql(name="county_per_1000", con=conn, if_exists="replace")

    conn.close()


def _get_active_for_region(cursor, program, region_id):
    sql = 'select count from active_facilities'
    sql += ' where program=\'{}\' and region_id={}'
    sql = sql.format(program, region_id)
    cursor.execute(sql)
    fetch = cursor.fetchone()
    active = 0
    if fetch:
        active = fetch[0]
    return active


def _get_events_for_region(cursor, program, event_type, region_id, year):
    sql = 'select count from {} where'
    sql += ' program=\'{}\' and region_id={} and year={}'
    sql = sql.format(event_type, program, region_id, year)
    cursor.execute(sql)
    fetch = cursor.fetchone()
    num_events = 0
    if fetch:
        num_events = fetch[0] if fetch[0] else 0
    return num_events

def _get_events_per_fac(cursor, region_mode, region_id, num_events, year):
    for program in ['CAA', 'CWA', 'RCRA']:
        active = _get_active_for_region(cursor, program, region_id)
        for event_type in ['inspections', 'violations', 'enforcements']:
            num_events[(program, event_type)] = _get_events_for_region(cursor,
                                                                       program,
                                                                       event_type,
                                                                       region_id,
                                                                       year)
        for event_type in ['inspections', 'violations', 'enforcements']:
            num_events[(program, event_type)] = 0 if active == 0 \
                else 1000. * num_events[(program, event_type)] / active
    return (num_events[('CAA', 'inspections')],
            num_events[('CAA', 'violations')],
            num_events[('CAA', 'enforcements')],
            num_events[('CWA', 'inspections')],
            num_events[('CWA', 'violations')],
            num_events[('CWA', 'enforcements')],
            num_events[('RCRA', 'inspections')],
            num_events[('RCRA', 'violations')],
            num_events[('RCRA', 'enforcements')],
            region_mode)


def _get_state_per_1000(cursor, state, year):
    region_mode = 'State'
    sql = 'select rowid from regions where state=\'{}\''
    sql = sql.format(state)
    cursor.execute(sql)
    region_ids = cursor.fetchall()
    sql = 'select rowid from regions where state=\'{}\' and region_type=\'State\''
    sql = sql.format(state)
    cursor.execute(sql)
    state_region_id = cursor.fetchone()
    active = 0
    num_events = {}
    for program in ['CAA', 'CWA', 'RCRA']:
        for event_type in ['inspections', 'violations', 'enforcements']:
            num_events[(program, event_type)] = 0
    for program in ['CAA', 'CWA', 'RCRA']:
        for region_id in region_ids:
            region_id = region_id[0]
            active += _get_active_for_region(cursor, program, region_id)
            for event_type in ['inspections', 'violations', 'enforcements']:
                num_events[(program, event_type)] += _get_events_for_region(cursor,
                                                                            program,
                                                                            event_type,
                                                                            region_id,
                                                                            year)
    return _get_events_per_fac(cursor, region_mode, state_region_id, num_events, year)
    '''
        for event_type in ['inspections', 'violations', 'enforcements']:
            num_events[(program, event_type)] = 0 if active == 0 \
                else 1000. * num_events[(program, event_type)] / active
    return (num_events[('CAA', 'inspections')],
            num_events[('CAA', 'violations')],
            num_events[('CAA', 'enforcements')],
            num_events[('CWA', 'inspections')],
            num_events[('CWA', 'violations')],
            num_events[('CWA', 'enforcements')],
            num_events[('RCRA', 'inspections')],
            num_events[('RCRA', 'violations')],
            num_events[('RCRA', 'enforcements')],
            'State')
    '''

def _get_county_per_1000(cursor, region_id, state, county, year):
    num_events = {}
    for program in ['CAA', 'CWA', 'RCRA']:
        for event_type in ['inspections', 'violations', 'enforcements']:
            num_events[(program, event_type)] = 0
    return _get_events_per_fac(cursor, 'County', region_id, num_events, year)
    '''
    for program in ['CAA', 'CWA', 'RCRA']:
        active = _get_active_for_region(cursor, program, region_id)
        for event_type in ['inspections', 'violations', 'enforcements']:
            num_events[(program, event_type)] = _get_events_for_region(cursor,
                                                                       program,
                                                                       event_type,
                                                                       region_id,
                                                                       year)
        for event_type in ['inspections', 'violations', 'enforcements']:
            num_events[(program, event_type)] = 0 if active == 0 \
                else 1000. * num_events[(program, event_type)] / active
    return (num_events[('CAA', 'inspections')],
            num_events[('CAA', 'violations')],
            num_events[('CAA', 'enforcements')],
            num_events[('CWA', 'inspections')],
            num_events[('CWA', 'violations')],
            num_events[('CWA', 'enforcements')],
            num_events[('RCRA', 'inspections')],
            num_events[('RCRA', 'violations')],
            num_events[('RCRA', 'enforcements')],
            'County')
    '''

def _get_cd_per_1000(cursor, state, cd, year):
    region_mode = 'Congressional District'
    num_events = {}
    for program in ['CAA', 'CWA', 'RCRA']:
        for event_type in ['inspections', 'violations', 'enforcements']:
            num_events[(program, event_type)] = 0
    if cd == 0:
        # This is a single-cd state.
        # The data for the CD will be same as the data for the state.
        sql = 'select rowid from regions where state=\'{}\' and region_type=\'State\''
        sql = sql.format(state)
        cursor.execute(sql)
        region_id = cursor.fetchone()
        active = 0
        if region_id is None:
            return []
        region_id = region_id[0]
        return _get_events_per_fac(cursor, region_mode, region_id, num_events, year)
        '''
        for program in ['CAA', 'CWA', 'RCRA']:
            active += _get_active_for_region(cursor, program, region_id)
            for event_type in ['inspections', 'violations', 'enforcements']:
                num_events[(program, event_type)] += _get_events_for_region(cursor,
                                                                            program,
                                                                            event_type,
                                                                            region_id,
                                                                            year)
            for event_type in ['inspections', 'violations', 'enforcements']:
                num_events[(program, event_type)] = 0 if active == 0 \
                    else 1000. * num_events[(program, event_type)] / active
        return (num_events[('CAA', 'inspections')],
                num_events[('CAA', 'violations')],
                num_events[('CAA', 'enforcements')],
                num_events[('CWA', 'inspections')],
                num_events[('CWA', 'violations')],
                num_events[('CWA', 'enforcements')],
                num_events[('RCRA', 'inspections')],
                num_events[('RCRA', 'violations')],
                num_events[('RCRA', 'enforcements')],
                'Congressional District')
        '''
    else:
        # Get the results for just this single state/cd
        region_id = AllPrograms_util.get_region_rowid(cursor, region_mode,
                                                      state, str(cd).zfill(2))
        return _get_events_per_fac(cursor, region_mode, region_id, num_events, year)
        '''
        for program in ['CAA', 'CWA', 'RCRA']:
            active = _get_active_for_region(cursor, program, region_id)
            for event_type in ['inspections', 'violations', 'enforcements']:
                num_events[(program, event_type)] += _get_events_for_region(cursor,
                                                                            program,
                                                                            event_type,
                                                                            region_id,
                                                                            year)
            for event_type in ['inspections', 'violations', 'enforcements']:
                num_events[(program, event_type)] = 0 if active == 0 \
                    else 1000. * num_events[(program, event_type)] / active
        return (num_events[('CAA', 'inspections')],
                num_events[('CAA', 'violations')],
                num_events[('CAA', 'enforcements')],
                num_events[('CWA', 'inspections')],
                num_events[('CWA', 'violations')],
                num_events[('CWA', 'enforcements')],
                num_events[('RCRA', 'inspections')],
                num_events[('RCRA', 'violations')],
                num_events[('RCRA', 'enforcements')],
                'Congressional District')
        '''

def get_all_per_1000(db, region_mode, year):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    results = {}
    if region_mode == 'Congressional District':
        sql = 'select state, cd from real_cds'
        df_real = pd.read_sql_query(sql, conn)
        for idx, row in df_real.iterrows():
            # Results will be dictionary with key=AL01, state/cd,
            # and values a tuple (Num per 1000, event_type, Region) where
            # event_type is 'inspections', 'violations', or 'enforcements' and
            # Region is 'Congressional District' or 'County' or 'State'
            cd = row['cd']
            state = row['state']
            key = '{}-{}'.format(state, str(cd).zfill(2))
            results[key] = _get_cd_per_1000(cursor, state, cd, year)
    elif region_mode == 'County':    
        sql = 'select rowid as region_id, state, region from regions where region_type=\'County\''
        df_real = pd.read_sql_query(sql, conn)
        # for region_id, state, region in df_real2.iterrows():
        for idx, row in df_real.iterrows():
            county = row['region']
            state = row['state']
            key = '{}-{}'.format(state, county)
            results[key] = _get_county_per_1000(cursor, row['region_id'], state, county, year)

    sql = 'select distinct(state) from regions'
    cursor.execute(sql)
    states = cursor.fetchall()
    for state in states:
        # Include all identified cds or counties for the state.
        results[state[0]] = _get_state_per_1000(cursor, state[0], year)

    conn.close()
    df = pd.DataFrame.from_dict(results, orient='index',
                                columns=['CAA.Insp.per.1000', 'CAA.Viol.per.1000', 'CAA.Enf.per.1000',
                                         'CWA.Insp.per.1000', 'CWA.Viol.per.1000', 'CWA.Enf.per.1000',
                                         'RCRA.Insp.per.1000', 'RCRA.Viol.per.1000', 'RCRA.Enf.per.1000',
                                         'Region'])
    df.reset_index(inplace=True)

    if region_mode == 'Congressional District':
        df = df.rename(columns={'index': 'CD.State'})
    elif region_mode == 'County':    
        df = df.rename(columns={'index': 'County.State'})
    return df

