import pdb
import pandas as pd
import geopandas
import io
import os.path
import requests
import zipfile
import sqlite3
from ECHO_modules.geographies import fips

def set_focus_year(db, year):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    sql = "insert into config (focus_year) values ({}) \
        on conflict(focus_year) do update set focus_year = {}".format(year, year)
    cursor.execute(sql)
    conn.close()

def get_focus_year(db):
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    sql = 'select focus_year from config'
    cursor.execute(sql)
    focus_year = cursor.fetchone()
    conn.close()
    return focus_year

def get_region_rowid(cursor, region_mode, state, region):
    sel_sql = "select rowid from regions where region_type='{}' and state='{}'"
    sel_cd_sql = sel_sql + " and region = '{}'"
    sel_state_sql = sel_sql + " and region = ''"
    ins_sql = "insert into regions (region_type,state,region) values ('{}','{}','{}')"
    if region is not None:
        if region_mode == 'Congressional District':
            if region == 0:
                sql = "select rowid from regions where state='{}'".format(state)
            else:
                sql = sel_cd_sql.format(region_mode, state, str(region).zfill(2))
        elif region_mode == 'County':
            sql = sel_cd_sql.format(region_mode, state, region)
    else:
        sql = sel_state_sql.format("State", state)
    cursor.execute(sql)
    result = cursor.fetchone()
    if result is not None:
        return result[0]
    else:
        region_str = ""
        if region_mode == 'Congressional District':
            region_str = "" if region is None else str(region).zfill(2)
        elif region_mode == 'County':
            region_str = region
        sql = ins_sql.format(region_mode, state, region_str)
        cursor.execute(sql)
        return cursor.lastrowid


def get_cd118_shapefile(state):
    # See if we already have the file, so we don't need to download 
    # and unpack it
    state_fips = fips[state]
    cd_file = "./content/tl_2023_"+state_fips+"_cd118.shp"
    if not os.path.isfile(cd_file):
        try:
            url = "https://www2.census.gov/geo/tiger/TIGER2023/CD/tl_2023_"+state_fips+"_cd118.zip"
            request = requests.get(url)
            z = zipfile.ZipFile(io.BytesIO(request.content))
            z.extractall("./content")
        except:
            return None
    cd_shapefile = geopandas.read_file(cd_file, crs=4269)
    return cd_shapefile


# ### 7. Number of currently active facilities regulated in CAA, CWA, RCRRA, GHGRP
# * The program_count() function looks at the ECHO_EXPORTER data that is passed in and counts the number of facilities have the 'flag' parameter set to 'Y' (AIR_FLAG, NPDES_FLAG, RCRA_FLAG, GHG_FLAG)
# * cd_echo_data is a dictionary with key (state, cd), where the state_echo_data is filtered for records of the current CD.
# * cd_echo_active is a dictionary for active facilities in the region.
# * The number of records from these dictionaries is written into a file named like 'active-facilities_All_pg3', in a directory identified by the state and CD, e.g. "LA2".


def program_count(echo_data, program, flag, state, cd):
    count = echo_data.loc[echo_data[flag] == "Y"].shape[0]
    print(
        "There are {} active facilities in {} - {} tracked under {}.".format(
            str(count), state, cd, program
        )
    )
    return count


"""
Return the count of violations and number of facilities in the dataframe provided.
"""


def get_rowdata(df, field, flag):
    num_fac = df.loc[df[flag] == "Y"].shape[0]
    if num_fac == 0:
        return 0, 0
    count_viol = df.loc[
        ((df[field].str.count("S") + df[field].str.count("V")) >= 3)
    ].shape[0]
    return count_viol, num_fac


def get_cwa_df(df, focus_year):
    year = df["YEARQTR"].astype("str").str[0:4:1]
    df["YEARQTR"] = year
    df.rename(columns={"YEARQTR": "YEAR"}, inplace=True)
    # Remove fields not relevant to this graph.
    df = df.drop(
        columns=[
            "HLRNC",
            "FAC_NAME",
            "FAC_STREET",
            "FAC_CITY",
            "FAC_STATE",
            "FAC_COUNTY",
            "FAC_LAT",
            "FAC_LONG",
            "FAC_ZIP",
            "FAC_EPA_REGION",
            "FAC_DERIVED_WBD",
            "FAC_DERIVED_CD113",
            "FAC_PERCENT_MINORITY",
            "FAC_POP_DEN",
            "FAC_DERIVED_HUC",
            "FAC_SIC_CODES",
            "FAC_NAICS_CODES",
            "DFR_URL"
        ]
    )
    d = df.groupby(pd.to_datetime(df["YEAR"], format="%Y").dt.to_period("Y")).sum()
    d.index = d.index.strftime("%Y")
    d = d.copy()
    d = d[d.index <= focus_year]
    d = d[d.index > "2000"]
    cols = ['NUME90Q', 'NUMCVDT', 'NUMSVCD', 'NUMPSCH']
    d['Total'] = d[cols].sum(axis=1)
    # d1 = d[d.index <= focus_year]
    # d2 = d1[d1.index > "2000"]
    # d2["Total"] = d2[cols].sum(axis=1)
    # return d2
    return d


def get_inspections(ds, ds_type):
    df0 = ds.results[ds_type].dataframe
    if df0 is None:
        return None
    else:
        df_pgm = df0.copy()
    if len(df_pgm) > 0:
        df_pgm.rename(
            columns={ds.date_field: "Date", ds.agg_col: "Count"}, inplace=True
        )
        df_pgm = df_pgm.groupby(
            pd.to_datetime(df_pgm["Date"], format=ds.date_format, errors="coerce")
        )[["Count"]].agg("count")
        df_pgm = df_pgm.resample("Y").sum()
        df_pgm.index = df_pgm.index.strftime("%Y")
        df_pgm = df_pgm[df_pgm.index > "2000"]
    else:
        print("No records")
    return df_pgm


def get_events(ds, ds_type):
    df0 = ds.results[ds_type].dataframe
    if df0 is None:
        return None
    else:
        df_pgm = df0.copy()
    df_pgm.rename(columns={ds.date_field: "Date", ds.agg_col: "Count"}, inplace=True)

    try:
        df_pgm = df_pgm.groupby(
            pd.to_datetime(df_pgm["Date"], format=ds.date_format, errors="coerce")
        )[["Count"]].agg("count")
    except ValueError:
        print("Error with date {}".format(df_pgm["Date"]))
    df_pgm = df_pgm.resample("Y").sum()
    df_pgm.index = df_pgm.index.strftime("%Y")
    df_pgm = df_pgm[df_pgm.index >= "2001"]
    return df_pgm


def get_num_events(ds, ds_type, state, cd, year):
    df_pgm = get_events(ds, ds_type)
    if df_pgm is None:
        return 0
    if len(df_pgm) > 0:
        num_events = df_pgm[df_pgm.index == year]
        if num_events.empty:
            return 0
        else:
            return num_events["Count"][0]


def get_num_facilities(data_sets, program, ds_type, year):
    ds = data_sets[program]
    df0 = ds.results[ds_type].dataframe
    if df0 is None:
        return 0
    else:
        df_pgm = df0.copy()
    if len(df_pgm) > 0:
        df_pgm.rename(
            columns={ds.date_field: "Date", ds.agg_col: "Count"}, inplace=True
        )
        if program == "CWA Violations":
            yr = df_pgm["Date"].astype("str").str[0:4:1]
            df_pgm["Date"] = pd.to_datetime(yr, format="%Y")
        else:
            df_pgm["Date"] = pd.to_datetime(
                df_pgm["Date"], format=ds.date_format, errors="coerce"
            )
        df_pgm_year = df_pgm[df_pgm["Date"].dt.year == year].copy()
        df_pgm_year["Date"] = pd.DatetimeIndex(df_pgm_year["Date"]).year
        num_fac = len(df_pgm_year.index.unique())
        return num_fac


def get_enf_per_fac(ds_enf, ds_type, num_fac, year):
    df_pgm = get_enforcements(ds_enf, ds_type)
    if df_pgm is None or df_pgm.empty:
        print("There were no enforcement actions taken in the focus year")
    else:
        iyear = int(year)
        year_3 = str(iyear - 3)
        df_pgm = df_pgm[df_pgm.index > year_3]
        df_pgm = df_pgm[df_pgm.index <= year]
        if df_pgm.empty:
            df_pgm["Count"] = 0
            df_pgm["Amount"] = 0
        else:
            df_pgm = df_pgm.agg({"Amount": "sum", "Count": "sum"})
            df_pgm.Count = 0 if (num_fac == 0) else df_pgm.Count / num_fac
            df_pgm.Amount = 0 if (num_fac == 0) else df_pgm.Amount / num_fac
    return df_pgm


def get_enforcements(ds, ds_type):
    df0 = ds.results[ds_type].dataframe
    if df0 is None:
        return None
    else:
        df_pgm = df0.copy()
    if len(df_pgm) > 0:
        df_pgm.rename(
            columns={ds.date_field: "Date", ds.agg_col: "Amount"}, inplace=True
        )
        if ds.name == "CWA Penalties":
            df_pgm["Amount"] = df_pgm["Amount"].fillna(0)
            df_pgm["Amount"] += df_pgm["STATE_LOCAL_PENALTY_AMT"].fillna(0)
        df_pgm["Count"] = 1
        df_pgm = df_pgm.groupby(
            pd.to_datetime(df_pgm["Date"], format="%m/%d/%Y", errors="coerce")
        ).agg({"Amount": "sum", "Count": "count"})

        df_pgm = df_pgm.resample("Y").sum()
        df_pgm.index = df_pgm.index.strftime("%Y")
        df_pgm = df_pgm[df_pgm.index >= "2001"]
    else:
        print("No records")
    return df_pgm


def get_ghg_emissions(ds, ds_type):
    df_result = ds.results[ds_type].dataframe
    if df_result is None:
        print("No records")
        return None
    else:
        df_pgm = df_result.copy()
    if df_pgm is not None and len(df_pgm) > 0:
        df_pgm.rename(
            columns={ds.date_field: "Date", ds.agg_col: "Amount"}, inplace=True
        )
        df_pgm = df_pgm.groupby(
            pd.to_datetime(df_pgm["Date"], format=ds.date_format, errors="coerce")
        )[["Amount"]].agg("sum")
        df_pgm = df_pgm.resample("Y").sum()
        df_pgm.index = df_pgm.index.strftime("%Y")
        # df_pgm = df_pgm[ df_pgm.index == '2018' ]
    else:
        print("No records")
    return df_pgm


def get_violations_by_facilities(df, action_field, flag, noncomp_field):
    df = df.loc[df[flag] == "Y"]
    if df.empty:
        return None
    df = df.copy()
    noncomp = df[noncomp_field]
    noncomp_count = noncomp.str.count("S") + noncomp.str.count("V")
    df["noncomp_qtrs"] = noncomp_count
    df = df[["FAC_NAME", "noncomp_qtrs"]]
    df.rename(columns={"FAC_NAME": "num_facilities"}, inplace=True)
    df = df.fillna(0)
    df = df.groupby(["noncomp_qtrs"]).count()
    return df


def get_top_violators(df_active, flag, noncomp_field, action_field, num_fac=10):
    """
    Sort the dataframe and return the rows that have the most number of
    non-compliant quarters.

    Parameters
    ----------
    df_active : Dataframe
        Must have ECHO_EXPORTER fields
    flag : str
        Identifies the EPA programs of the facility (AIR_FLAG, NPDES_FLAG, etc.)
    state : str
        The state
    cd : str
        The congressional district    
    noncomp_field : str
        The field with the non-compliance values, 'S' or 'V'.
    action_field
        The field with the count of quarters with formal actions
    num_fac
        The number of facilities to include in the returned Dataframe

    Returns
    -------
    Dataframe
        The top num_fac violators for the EPA program in the region

    Examples
    --------
    >>> df_violators = get_top_violators( df_active, 'AIR_FLAG', state, region_selected, 
        'CAA_3YR_COMPL_QTRS_HISTORY', 'CAA_FORMAL_ACTION_COUNT', 20 )
    """
    df = df_active.loc[df_active[flag] == "Y"]
    if len(df) == 0:
        return None
    df_active = df.copy()
    noncomp = df_active[noncomp_field]
    noncomp_count = noncomp.str.count("S") + noncomp.str.count("V")
    df_active["noncomp_count"] = noncomp_count
    df_active = df_active[
        ["FAC_NAME", "noncomp_count", action_field, "DFR_URL", "FAC_LAT", "FAC_LONG"]
    ]
    df_active = df_active.sort_values(
        by=["noncomp_count", action_field], ascending=False
    )
    df_active = df_active.head(num_fac)
    return df_active

def build_all_per_1000(total_df):
    """
    Build the ranks for states and percentiles for CDs or counties from total_df.

    Parameters
    ----------
    total_df : DataFrame
        Contains per_1000 figures for all states and CDs in selected years

    Returns
    -------
    tuple
        DataFrame of states, ranked
        DataFrame of CDs or counties, by percentiles
    """
    state_per_1000 = total_df[total_df['Region'] == 'State'].copy()
    state_per_1000['CAA_Insp_Rank'] = (state_per_1000['CAA.Viol.per.1000'] /
                                       state_per_1000['CAA.Viol.per.1000']).rank()
    state_per_1000['CAA_Viol_Rank'] = state_per_1000['CAA.Viol.per.1000'].rank()
    state_per_1000['CAA_Enf_Rank'] = (state_per_1000['CAA.Enf.per.1000'] /
                                      state_per_1000['CAA.Viol.per.1000']).rank()
    state_per_1000['CWA_Insp_Rank'] = (state_per_1000['CWA.Viol.per.1000'] /
                                       state_per_1000['CWA.Viol.per.1000']).rank()
    state_per_1000['CWA_Viol_Rank'] = state_per_1000['CWA.Viol.per.1000'].rank()
    state_per_1000['CWA_Enf_Rank'] = (state_per_1000['CWA.Enf.per.1000'] /
                                      state_per_1000['CWA.Viol.per.1000']).rank()
    state_per_1000['CWA_Enf_Rank'] = state_per_1000['CWA.Enf.per.1000'].rank()
    state_per_1000['RCRA_Insp_Rank'] = (state_per_1000['RCRA.Viol.per.1000'] /
                                        state_per_1000['RCRA.Viol.per.1000']).rank()
    state_per_1000['RCRA_Viol_Rank'] = state_per_1000['RCRA.Viol.per.1000'].rank()
    state_per_1000['RCRA_Enf_Rank'] = (state_per_1000['RCRA.Enf.per.1000'] /
                                       state_per_1000['RCRA.Viol.per.1000']).rank()
    state_per_1000.drop('Region', axis=1, inplace=True)
    state_per_1000.set_index('CD.State')

    cd_per_1000 = total_df[total_df['Region'] == 'Congressional District'].copy()
    cd_per_1000['CAA_Insp_Pct'] = (cd_per_1000['CAA.Insp.per.1000'] /
                                   cd_per_1000['CAA.Viol.per.1000']).rank(pct=True)
    cd_per_1000['CAA_Viol_Pct'] = cd_per_1000['CAA.Viol.per.1000'].rank(pct=True)
    cd_per_1000['CAA_Enf_Pct'] = (cd_per_1000['CAA.Enf.per.1000'] /
                                  cd_per_1000['CAA.Viol.per.1000']).rank(pct=True)
    cd_per_1000['CWA_Insp_Pct'] = (cd_per_1000['CWA.Insp.per.1000'] /
                                   cd_per_1000['CWA.Viol.per.1000']).rank(pct=True)
    cd_per_1000['CWA_Viol_Pct'] = cd_per_1000['CWA.Viol.per.1000'].rank(pct=True)
    cd_per_1000['CWA_Enf_Pct'] = (cd_per_1000['CWA.Enf.per.1000'] /
                                  cd_per_1000['CWA.Viol.per.1000']).rank(pct=True)
    cd_per_1000['RCRA_Insp_Pct'] = (cd_per_1000['RCRA.Insp.per.1000'] /
                                    cd_per_1000['RCRA.Viol.per.1000']).rank(pct=True)
    cd_per_1000['RCRA_Viol_Pct'] = cd_per_1000['RCRA.Viol.per.1000'].rank(pct=True)
    cd_per_1000['RCRA_Enf_Pct'] = (cd_per_1000['RCRA.Enf.per.1000'] /
                                   cd_per_1000['RCRA.Viol.per.1000']).rank(pct=True)
    cd_per_1000.drop('Region', axis=1, inplace=True)
    cd_per_1000.set_index('CD.State')

    county_per_1000 = total_df[total_df['Region'] == 'County'].copy()
    county_per_1000['CAA_Insp_Pct'] = (county_per_1000['CAA.Insp.per.1000'] /
                                       county_per_1000['CAA.Viol.per.1000']).rank(pct=True)
    county_per_1000['CAA_Viol_Pct'] = county_per_1000['CAA.Viol.per.1000'].rank(pct=True)
    county_per_1000['CAA_Enf_Pct'] = (county_per_1000['CAA.Enf.per.1000'] /
                                      county_per_1000['CAA.Viol.per.1000']).rank(pct=True)
    county_per_1000['CWA_Insp_Pct'] = (county_per_1000['CWA.Insp.per.1000'] /
                                       county_per_1000['CWA.Viol.per.1000']).rank(pct=True)
    county_per_1000['CWA_Viol_Pct'] = county_per_1000['CWA.Viol.per.1000'].rank(pct=True)
    county_per_1000['CWA_Enf_Pct'] = (county_per_1000['CWA.Enf.per.1000'] /
                                      county_per_1000['CWA.Viol.per.1000']).rank(pct=True)
    county_per_1000['RCRA_Insp_Pct'] = (county_per_1000['RCRA.Insp.per.1000'] /
                                        county_per_1000['RCRA.Viol.per.1000']).rank(pct=True)
    county_per_1000['RCRA_Viol_Pct'] = county_per_1000['RCRA.Viol.per.1000'].rank(pct=True)
    county_per_1000['RCRA_Enf_Pct'] = (county_per_1000['RCRA.Enf.per.1000'] /
                                       county_per_1000['RCRA.Viol.per.1000']).rank(pct=True)
    county_per_1000.drop('Region', axis=1, inplace=True)
    county_per_1000.set_index('CD.State')

    return state_per_1000, cd_per_1000, county_per_1000
