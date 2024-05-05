import pdb

import pandas as pd
import geopandas
import sqlite3
import AllPrograms_util
from ECHO_modules.get_data import get_echo_data
from Region import Region, get_inflation


def get_active_facs(mode, state, region, cds_or_counties):
    sql = 'select * from "ECHO_EXPORTER" where '
    sql += '"FAC_STATE" = \'{}\''.format(state)
    """ Old code for FAC_DERIVED_CD113
    if mode == 'Congressional District':
        if region != 0:
            sql += ' and "FAC_DERIVED_CD113" = {}'
            sql = sql.format(str(region))
    elif mode == 'County':
    """
    if mode == 'County':
        echo_counties = cds_or_counties[cds_or_counties['County'] == region]['FAC_COUNTY']
        county_str = "\',\'".join(echo_counties.tolist())
        # county_str = "\'" + county_str + "\'"
        sql += ' and "FAC_COUNTY" in (\'{}\')'
        sql = sql.format(county_str)
    region_echo_data = get_echo_data(sql, "REGISTRY_ID")
    region_echo_data = region_echo_data.loc[region_echo_data["FAC_ACTIVE_FLAG"] == "Y"]
    if mode == 'Congressional District' and type(cds_or_counties) == geopandas.geodataframe.GeoDataFrame: 
        region_echo_data['geometry'] = geopandas.GeoSeries.from_wkb(region_echo_data['wkb_geometry'])
        region_echo_data.drop('wkb_geometry', axis=1, inplace=True)
        region_echo_data = geopandas.GeoDataFrame(region_echo_data, crs=4269)
        join = region_echo_data.sjoin(cds_or_counties, how="left")
        join['CD118FP'] = pd.to_numeric(join['CD118FP'], errors='coerce')
        region_echo_data = join.loc[join["CD118FP"] == float(region)]
    return region_echo_data


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


def write_violations(db, region_mode, program, ds, ds_type):
    ins_sql = (
        "insert into violations (region_id,program,year,count) values ({},'{}',{},{})"
    )
    ins_sql += " on conflict(region_id,program,year) do update set count = {}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

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


def write_CWA_violations(db, region_mode, df, ds_type):
    ins_sql = (
        "insert into violations (region_id,program,year,count) values ({},'{}',{},{})"
    )
    ins_sql += " on conflict(region_id,program,year) do update set count = {}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    if df is not None:
        # idx will be the year
        for idx, row in df.iterrows():
            sql = ins_sql.format(rowid, "CWA", idx, row["Total"], row["Total"])
            cursor.execute(sql)
    conn.commit()


def write_inspections(db, region_mode, program, ds, ds_type):
    ins_sql = (
        "insert into inspections (region_id,program,year,count) values ({},'{}',{},{})"
    )
    ins_sql += " on conflict(region_id,program,year) do update set count = {}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

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


def write_enforcements(db, region_mode, program, ds, ds_type, focus_year):
    ins_sql = "insert into enforcements (region_id,program,year,amount,count) "
    ins_sql += "values ({},'{}',{},{},{})"
    ins_sql += " on conflict(region_id,program,year) do update set amount={}, count={}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

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


def write_per_fac(db, region_mode, program, ds_type, event, year, count):
    ins_sql = "insert into per_fac (region_id,program,type,year,count) "
    ins_sql += "values ({},'{}','{}',{},{})"
    ins_sql += " on conflict(region_id,program,type,year) do update set count={}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

    state = ds_type[2]
    cd = ds_type[1]
    rowid = AllPrograms_util.get_region_rowid(cursor, region_mode, state, cd)
    # pdb.set_trace()
    sql = ins_sql.format(rowid, program, event, year, count, count)
    cursor.execute(sql)
    conn.commit()


def write_enf_per_fac(db, region_mode, program, ds, ds_type, num_fac, year):
    ins_sql = "insert into enf_per_fac (region_id,program,year,count,amount,num_fac)"
    ins_sql += " values ({},'{}',{},{},{},{}) on conflict(region_id,program,year)"
    ins_sql += " do update set count={}, amount={}, num_fac={}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

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


def write_violations_by_facilities(db, region_mode,
        df, ds_type, program, action_field, flag, noncomp_field
):
    ins_sql = "insert into violations_by_facilities (region_id,program,"
    ins_sql += " noncomp_qtrs,num_facilities) "
    ins_sql += " values ({},'{}',{},{})"
    ins_sql += " on conflict (region_id, program, noncomp_qtrs)"
    ins_sql += " do update set noncomp_qtrs={}, num_facilities={}"

    conn = sqlite3.connect(db)
    cursor = conn.cursor()

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

def make_per_1000(db, focus_year):
    """
    Build the state_per_1000 and cd_per_1000 tables with the 
    get_all_per_1000() function for the five years
    ending with the focus year.
    
    Parameters
    ----------
    focus_year : string
        The end year for the results.
    """

    start_year = int(focus_year) - 4
    total_df = get_all_per_1000(start_year)
    for year in range(start_year + 1, int(focus_year)):
        df = get_all_per_1000(year)
        df = df[(df['CD.State'] != 'MX') & (df['CD.State'] != 'GM') &
                (df['CD.State'] != 'MB')]
        df.set_index('CD.State')
        total_df['CD.State'] = df['CD.State']
        total_df['Region'] = df['Region']

    (state_per_1000, cd_per_1000, county_per_1000) = AllPrograms_util.build_all_per_1000(total_df)

    conn = sqlite3.connect(db)
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


def _get_state_per_1000(cursor, state, year):
    sql = 'select rowid from regions where state=\'{}\''
    sql = sql.format(state)
    cursor.execute(sql)
    region_ids = cursor.fetchall()
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


def _get_county_per_1000(cursor, region_id, state, county, year):
    num_events = {}
    for program in ['CAA', 'CWA', 'RCRA']:
        for event_type in ['inspections', 'violations', 'enforcements']:
            num_events[(program, event_type)] = 0
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


def _get_cd_per_1000(cursor, region_mode, state, cd, year):
    num_events = {}
    for program in ['CAA', 'CWA', 'RCRA']:
        for event_type in ['inspections', 'violations', 'enforcements']:
            num_events[(program, event_type)] = 0
    if cd == 0:
        # This is a single-cd state.
        # Include all identified cds for the state.
        sql = 'select rowid from regions where state=\'{}\' and region_type=\'CD\''
        sql = sql.format(state)
        cursor.execute(sql)
        region_ids = cursor.fetchall()
        active = 0
        for program in ['CAA', 'CWA', 'RCRA']:
            for region_id in region_ids:
                active += _get_active_for_region(program, region_id)
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
    else:
        # Get the results for just this single state/cd
        region_id = AllPrograms_util.get_region_rowid(cursor, region_mode, state, str(cd).zfill(2))
        for program in ['CAA', 'CWA', 'RCRA']:
            active = _get_active_for_region(program, region_id)
            for event_type in ['inspections', 'violations', 'enforcements']:
                active = _get_active_for_region(program, region_id)
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


def get_all_per_1000(db, year):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    results = {}
    df_real = pd.DataFrame()
    sql = 'select state, cd from real_cds'
    df_real = pd.read_sql_query(sql, conn)
    for state, cd in df_real.iterrows():
        sql = 'select cd from real_cds where state = \'{}\''.format(state)
        df_real = pd.read_sql_query(sql, conn)
        # Results will be dictionary with key=AL01, state/cd,
        # and values a tuple (Num per 1000, event_type, Region) where
        # event_type is 'inspections', 'violations', or 'enforcements' and
        # Region is 'Congressional District' or 'County' or 'State'
        for idx, row in df_real.iterrows():
            cd = row['cd']
            key = '{}-{}'.format(state, str(cd).zfill(2))
            results[key] = _get_cd_per_1000(cursor, state, cd, year)

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
    df = df.rename(columns={'index': 'CD.State'})
    return df

# test_facs = {"XXX": 14, "YYY": 20, "ZZZ": 30}
# write_active_facs(test_facs, "MX", 4)
# test_facs = {"XXX": 15, "YYY": 23, "ZZZ": 33}
# write_active_facs(test_facs, "MX", 4)
# write_active_facs(test_facs, "MX")
