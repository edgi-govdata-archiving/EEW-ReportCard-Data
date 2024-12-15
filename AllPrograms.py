#!/usr/bin/env python
# coding: utf-8

import pdb
import pandas as pd
import sys, argparse
import urllib
import AllPrograms_db
import AllPrograms_util
from csv import reader
from ECHO_modules.make_data_sets import make_data_sets


def main(argv):
    parser = argparse.ArgumentParser(
        prog="AllPrograms.py",
        description="Get data for all regions passed in, and for the "
                    " focus year specified. "
                    "Write the data into the regions.db SQLite database.",
    )
    parser.add_argument(
        "-b",
        "--database",
        required=True,
        help="The Sqlite3 database",
    )
    parser.add_argument(
        "-f",
        "--focus_year",
        required=True,
        help="The year on which the report will focus",
    )
    parser.add_argument(
        "-s",
        "--focus_state",
        required=True,
        help="The state to work with",
    )
    parser.add_argument(
        "-m",
        "--region_mode",
        required=True,
        help="County or Congressional District",
    )
    # group = parser.add_mutually_exclusive_group()
    my_args = parser.parse_args()

    _database = my_args.database
    _region_mode = my_args.region_mode
    _state = my_args.focus_state
    state_regions = []
    state_counties = pd.DataFrame()
    if _region_mode == 'Congressional District':
        # Read the CDs for the given state (cds_state) from 
        # the region.db table real_cds
        cds = AllPrograms_db.get_real_cds(_database, _state)
        if int(cds[0][0]) == 0:
            # This is a state with only one CD, but the ECHO data
            # may show some incorrect values. Treat this as a full
            # state.
            _region_mode = 'State'
            state_regions.append(None)
        else: # Congressional District
            for cd in cds:
                cd = int(cd[0])
                # pd.concat(state_regions, (_state, cd))
                state_regions.append(cd)
    else:
        url = "https://raw.githubusercontent.com/edgi-govdata-archiving/"
        url += "ECHO_modules/main/data/state_counties_corrected.csv"
        all_counties = pd.read_csv(url)
        state_counties = pd.DataFrame()
        this_state_counties = all_counties[all_counties['FAC_STATE'] == _state]
        state_counties = pd.concat([state_counties, this_state_counties], ignore_index=True)
        state_regions = pd.unique(pd.Series(
            state_counties[['County']].apply(tuple, axis=1).tolist()))
        state_regions = state_regions.tolist()

    exp_to_pgm = {}
    exp_to_pgm["CWA"] = AllPrograms_db.get_exp_pgm("CWA")
    exp_to_pgm["RCRA"] = AllPrograms_db.get_exp_pgm("RCRA")

    """
    The generated database (region.db) will be based on a particular year, generally
    the last year of complete data in ECHO. Set that as the "focus_year".
    """
    focus_year = str(my_args.focus_year)
    AllPrograms_util.set_focus_year(_database, focus_year)

    """
    First get facility information.  If there are no facilities (probably due
    to this being a region marked in error in ECHO) then we will remove the region from
    the list of those that get processed.
    """
    # ### 6. Get the region (zip, CD, watershed, etc.) data only.  The state data can 
    # be constructed from the region data.
    # Ask the database for ECHO_EXPORTER records for facilities in the region.
    # * region_echo_data is a dictionary with the state and region as key and the data as 
    # value, for all records.
    # * region_echo_active is a dictionary for all records in region_echo_data identified 
    # as active.

    state_echo_active = pd.DataFrame()
    region_echo_active = {}
    remove_state_regions = []
    if _region_mode == 'County':
        cds_or_counties = state_counties[state_counties['FAC_STATE'] == _state]
    else:
        cds_or_counties = AllPrograms_util.get_cd118_shapefile(_state)
    try:
        state_echo_active = (
            AllPrograms_db.get_active_facs(_region_mode, _state, cds_or_counties))
        print("Active facilities for {} = {}".format(_state, len(state_echo_active)))
    except pd.errors.EmptyDataError:
        # No facilities in this state.  Mark for removal.
        print("No active facilities in {}".format(_state))
        remove_state_regions.append(region)
    # Remove any (state,region) that had no facilities
    for region in remove_state_regions:
        state_regions.remove(region)

    for region in state_regions:
        if _region_mode == 'County':
            region = region[0]
            region_echo_active[region] = state_echo_active[state_echo_active['FAC_COUNTY'] == region]
            active_facs = {"CAA": AllPrograms_util.program_count(
                region_echo_active[region],
                "CAA", "AIR_FLAG", region
                ), "CWA": AllPrograms_util.program_count(
                region_echo_active[region],
                "CWA", "NPDES_FLAG", region
                ), "RCRA": AllPrograms_util.program_count(
                region_echo_active[region],
                "RCRA", "RCRA_FLAG", region
                ), "GHG": AllPrograms_util.program_count(
                region_echo_active[region],
                "GHG", "GHG_FLAG", region
                )}
        elif _region_mode == 'Congressional District':
            region_echo_active[region] = state_echo_active[state_echo_active['CD118FP'] == region]
            active_facs = {"CAA": AllPrograms_util.program_count(
                region_echo_active[region],
                "CAA", "AIR_FLAG", region
                ), "CWA": AllPrograms_util.program_count(
                region_echo_active[region],
                "CWA", "NPDES_FLAG", region
                ), "RCRA": AllPrograms_util.program_count(
                region_echo_active[region],
                "RCRA", "RCRA_FLAG", region
                ), "GHG": AllPrograms_util.program_count(
                region_echo_active[region],
                "GHG", "GHG_FLAG", region
                )}
        elif _region_mode == 'State':
            active_facs = {"CAA": AllPrograms_util.program_count(
                state_echo_active,
                "CAA", "AIR_FLAG", region
                ), "CWA": AllPrograms_util.program_count(
                state_echo_active,
                "CWA", "NPDES_FLAG", region
                ), "RCRA": AllPrograms_util.program_count(
                state_echo_active,
                "RCRA", "RCRA_FLAG", region
                ), "GHG": AllPrograms_util.program_count(
                state_echo_active,
                "GHG", "GHG_FLAG", region
                )}
        AllPrograms_db.write_active_facs(_database, _region_mode, active_facs, _state, region)

    data_set_list = [
        "RCRA Violations",
        "RCRA Inspections",
        "RCRA Penalties",
        "CAA Violations",
        "CAA Inspections",
        "CAA Penalties",
        "Greenhouse Gas Emissions",
        "CWA Violations",
        "CWA Inspections",
        "CWA Penalties",
    ]

    # ### 4. This cell makes the data sets and stores the results for each of them from the database.
    # This may take some time to run if you are looking at multiple congressional districts.
    # * The data_set_list from cell #3 is given to the make_data_sets() function which creates a DataSet 
    # object for each item in the list.
    # * Go through each of the (state, region) pairs in the state_region list specified in cell #3 and 
    # have the DataSet object store 
    # results returned by the database for that specific state and region.
    # * Also go through each unique state in the list and store data for the entire state.

    data_sets = make_data_sets(data_set_list)
    for ds_key, data_set in data_sets.items():
        print(_state + "-" + ds_key)
        data_set.store_results(
            region_type="State", region_value=None, state=_state
        )


    """
    Combining the calculations that were in cell #9 and cell #17.
    """

    # ### 9. Number of recurring violations - facilities with 3+ quarters out of the last 12 in non-compliance, 
    # by each program.
    # For each unique state and then each region, we look at active records and count facilities that have 
    # 'S' or 'V' violations in 3 or more quarters.  The fields looked at are:
    # * CAA - CAA_3YR_COMPL_QTRS_HISTORY
    # * CWA - CWA_13QTRS_COMPL_HISTORY (Actually 13 quarters instead of 3 years.)
    # * RCRA - RCRA_3   exp_to_pgm = {}
    #
    # * The get_viol_counts() function takes the dataframe passed to it, and looks 
    # for records with 'S' or 'V' violations in more than 3 quarters. It 
    # divides the violations by the number of facilities, returning the raw 
    # count of facilities in violation more than 3 months and the percentage 
    # of facilities.

    # ### 17. Focus year - enforcement counts and amounts per violating facility 
    # - by district 
    # * The get_num_facilities() function combines the violations into years, 
    # then counts the number of facilities with violations for each year.
    # * The get_enf_per_fac() function combines enforcements into years, then 
    # counts the enforcements and sums the amount of penalties, before dividing 
    # by the results from get_num_facilities().
    # * These functions are called for each region, and for CAA, CWA and RCRA.

    # Enforcement counts and amounts per violating facility
    """ 
    This will give more meaningful results if we look at the past 3 years.  
    Doing that, we can get a count of facilities with enforcements from the 
    ECHO_EXPORTER data to measure against the number of enforcements in the 
    past 3 years.
    We give the focus_year to write_enf_per_fac(), but it looks back to get 
    the counts of facilities and enforcements for the focus_year and the two 
    previous years.
    """

    region_data_sets = AllPrograms_util.make_region_sets(_region_mode, 
                                                         _state, 
                                                         data_set_list,
                                                         data_sets, 
                                                         state_echo_active,
                                                         state_regions,
                                                         exp_to_pgm)

    for region in state_regions:
        # region_value = None if region is None else str(region)
        if _region_mode == 'State':
            region_active = state_echo_active
        else:
            if _region_mode == 'County':
                region = region[0]
            region_active = region_echo_active[region]
        ds_type = (_region_mode, region, _state) 
        rowdata_region = []
        rd = AllPrograms_util.get_viol_counts(
            region_active, "CAA_3YR_COMPL_QTRS_HISTORY", "AIR_FLAG"
        )
        rowdata_region.append(["CAA", rd[0], rd[1]])
        num_fac = rd[1]
        message = (
            "CAA Penalties - {} District: {} - {} facilities with violations in {}"
        )
        print(message.format(_state, region, num_fac, focus_year))

        try:
            df_caa = AllPrograms_db.write_enf_per_fac(_database, 
                                                  'CAA', 
                                                  region_data_sets[('CAA Penalties', region)],
                                                  ds_type,
                                                  num_fac, 
                                                  focus_year)
        except KeyError:
            print("No data set for CAA and {region}")

        print("  CWA")
        rd = AllPrograms_util.get_viol_counts(
            region_active, "CWA_13QTRS_COMPL_HISTORY", "NPDES_FLAG"
        )
        rowdata_region.append(["CWA", rd[0], rd[1]])
        num_fac = rd[1]
        message = (
            "CWA Penalties - {} District: {} - {} facilities with violations in {}"
        )
        print(message.format(_state, region, num_fac, focus_year))

        try:
            df_cwa = AllPrograms_db.write_enf_per_fac(_database, 
                                                  "CWA", 
                                                  region_data_sets[("CWA Penalties",region)], 
                                                  ds_type, 
                                                  num_fac, 
                                                  focus_year
            )
        except KeyError:
            print("No data set for CWA and {region}")

        print("  RCRA")
        rd = AllPrograms_util.get_viol_counts(
            region_active, "RCRA_3YR_COMPL_QTRS_HISTORY", "RCRA_FLAG"
        )
        rowdata_region.append(["RCRA", rd[0], rd[1]])
        num_fac = rd[1]
        message = (
            "RCRA Penalties - {} District: {} - {} facilities with violations in {}"
        )
        print(message.format(_state, region, num_fac, focus_year))

        try:
            df_rcra = AllPrograms_db.write_enf_per_fac(_database, 
                                                   "RCRA", 
                                                   region_data_sets[("RCRA Penalties",region)], 
                                                   ds_type, 
                                                   num_fac, 
                                                   focus_year
            )
        except KeyError:
            print("No data set for CWA and {region}")

        AllPrograms_db.write_recurring_violations(_database, 
                                                  _region_mode, 
                                                  _state, 
                                                  region, 
                                                  rowdata_region)

        # Removed total_enf_per_fac.  It can be calculated from the individual
        # program records
        # if ( df_caa is not None or df_cwa is not None or df_rcra is not None ):
        #     df_totals = pd.concat( [df_caa, df_cwa, df_rcra] )
        #     df_totals = df_totals.groupby( df_totals.index ).agg('sum')
        #     print( "Total enforcements for {} district {} in {}".format(
        #                         state,region,focus_year ))
        #     AllPrograms_db.write_total_enf_per_fac( df_totals, ds_type )
        #     print( df_totals )

    # ### 10. Percent change in violations (CWA)
    # For each region and then each unique state,
    # * the quarter is identified in 5 digits, the 1st 4 are year and then
    # * the quarter, as in 20013 for the 3rd quarter of 2001
    # * the quarter is stipped off, so that there will now be 4 records for
    # * the facility for 2001 the values for the 4 types of violations--
    # * NUME90Q,NUMCVDT,NUMSVCD,NUMPSCH--are added together, over all
    # * facilities, to get a single value for the year
    # * The results for the focus year are stored in the dictionary
    # * effluent_violations_focus_year dictionary.  The key for the
    # * dictionary is (state,region).  These will be used in a later cell.

    effluent_violations_focus_year = {}  # For use later

    for region in state_regions:
        if _region_mode == 'Congressional District':
            region = str(region)
        elif _region_mode == 'County':
            region = region[0]
        ds_type = ("{}".format(_region_mode), region, _state)
        print("CWA Violations - {} District: {}".format(_state, region))
        df = region_data_sets[("CWA Violations",region)].get_dataframe()
        if df is None:
            continue
        else:
            df = df.copy()
        effluent_violations_all = AllPrograms_util.get_cwa_df(df, focus_year)
        for idx, row in effluent_violations_all.iterrows():
            if idx == focus_year:
                effluent_violations_focus_year[(_state, region)] = row["Total"]
        AllPrograms_db.write_CWA_violations(_database, effluent_violations_all, 
                                            ds_type)

    # ### 11. Percent change in inspections
    # For each region the date field for that program type is used to count up all 
    # inspections for the year.  (The date field for each data set is identified in 
    # make_data_sets() when the DataSet object is created.  It shows up here as ds.date_field.)
    # ### 12. Percent change in enforcement - penalties and number of enforcements
    # * For each region the number of enforcements and amount of penalty are retrieved
    # * from the agg_col field (specified in make_data_sets() for each DataSet).
    # * The number of penalties and amount are accummulated for each year.

    for region in state_regions:
        if _region_mode == 'County':
            region = region[0]
        ds_type = (_region_mode, str(region), _state)
        print("CAA Inspections - {} District: {}".format(_state, region))
        try:
            df_caa = AllPrograms_db.write_inspections(_database, 
                "CAA", region_data_sets[("CAA Inspections",region)], ds_type
            )
        except KeyError:
            print(f"No CAA inspection dataset for {region}")
        print("CWA Inspections - {} District: {}".format(_state, region))
        try:
            df_cwa = AllPrograms_db.write_inspections(_database, 
                "CWA", region_data_sets[("CWA Inspections",region)], ds_type
            )
        except KeyError:
            print(f"No CWA inspection dataset for {region}")

        print("RCRA Inspections - {} District: {}".format(_state, region))
        try:
            df_rcra = AllPrograms_db.write_inspections(_database, 
                "RCRA", region_data_sets[("RCRA Inspections",region)], ds_type
            )
        except KeyError:
            print(f"No RCRA inspection dataset for {region}")

        print("CAA Violations - {} District: {}".format(_state, region))
        try:
            df_caa = AllPrograms_db.write_violations(_database, 
                "CAA", region_data_sets[("CAA Violations",region)], ds_type
            )
        except KeyError:
            print(f"No CAA violation dataset for {region}")

        print("RCRA Violations - {} District: {}".format(_state, region))
        try:
            df_rcra = AllPrograms_db.write_violations(_database, 
                "RCRA", region_data_sets[("RCRA Violations",region)], ds_type
            )
        except KeyError:
            print(f"No RCRA violation dataset for {region}")

        print("CAA Penalties - {} District: {}".format(_state, region))
        try:
            df_caa = AllPrograms_db.write_enforcements(_database, 
                "CAA", region_data_sets[("CAA Penalties",region)], ds_type, focus_year
            )
        except KeyError:
            print(f"No CAA penalties dataset for {region}")

        print("CWA Penalties - {} District: {}".format(_state, region))
        try:
            df_cwa = AllPrograms_db.write_enforcements(_database, 
                "CWA", region_data_sets[("CWA Penalties",region)], ds_type, focus_year
            )
        except KeyError:
            print(f"No CWA penalties dataset for {region}")

        print("RCRA Penalties - {} District: {}".format(_state, region))
        try:
            df_rcra = AllPrograms_db.write_enforcements(_database, 
                "RCRA", region_data_sets[("RCRA Penalties",region)], ds_type, focus_year
            )
        except KeyError:
            print(f"No RCRA violation dataset for {region}")

    # ### 13.a. Focus year - inspections per regulated facility - by district
    # * For each region the inspections data is again grouped into years.
    # * The get_num_events() function counts all events it gets from
    # * get_events() for the year that is requested, which is focus_year.
    # * This number is divided by the number of facilities in the district, from
    # * the program_count() function of cell #7.
    # Inspections and violations per facility for the focus year

    for region in state_regions:
        if _region_mode == 'County':
            region = region[0]
        if region is None:
            ds_type = ("State", None, _state)
            region_active = state_echo_active
        else:
            ds_type = (_region_mode, str(region), _state)
            region_active = region_echo_active[region]
        pgm_count_caa = AllPrograms_util.program_count(
            region_active, "CAA", "AIR_FLAG", region
        )
        pgm_count_cwa = AllPrograms_util.program_count(
            region_active, "CWA", "NPDES_FLAG", region
        )
        pgm_count_rcra = AllPrograms_util.program_count(
            region_active, "RCRA", "RCRA_FLAG", region
        )
        try:
            num = AllPrograms_util.get_num_events(
                region_data_sets[("CAA Inspections",region)], ds_type, _state, region, focus_year
            )
            if pgm_count_caa > 0 and num is not None:
                num = num / pgm_count_caa
                print("CAA inspections per regulated facilities: ", num)
                AllPrograms_db.write_per_fac(_database, "CAA", 
                                             ds_type, "inspections", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in region CWA data")
        except KeyError:
            print(f"No CAA inspection dataset for {region}")
        try:
            num = AllPrograms_util.get_num_events(
                region_data_sets[("CAA Violations",region)], ds_type, _state, region, focus_year
            )
            if pgm_count_caa > 0 and num is not None:
                num /= pgm_count_caa
                print("CAA violations per regulated facilities: ", num)
                AllPrograms_db.write_per_fac(_database, "CAA", 
                                             ds_type, "violations", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in region CWA data")
        except KeyError:
            print(f"No CWA violation dataset for {region}")
        try:
            num = AllPrograms_util.get_num_events(
                region_data_sets[("CWA Inspections",region)], ds_type, _state, region, focus_year
            )
            if pgm_count_cwa > 0 and num is not None:
                num /= pgm_count_cwa
                print("CWA inspections per regulated facilities: ", num)
                AllPrograms_db.write_per_fac(_database, "CWA", 
                                             ds_type, "inspections", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in region CWA data")
        except KeyError:
            print(f"No CWA inspection dataset for {region}")
        try:
            # Have to handle CWA Violations differently - use saved dictionary from cell 10
            if (_state, region) in effluent_violations_focus_year:
                num = effluent_violations_focus_year[(_state, region)]
                if pgm_count_cwa > 0 and num is not None:
                    num = num / pgm_count_cwa
                    AllPrograms_db.write_per_fac(_database, "CWA", 
                                                 ds_type, "violations", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in state CWA data")
        try:
            num = AllPrograms_util.get_num_events(
                region_data_sets[("RCRA Inspections",region)], ds_type, _state, region, focus_year
            )
            if pgm_count_rcra > 0 and num is not None:
                num /= pgm_count_rcra
                print("RCRA inspections per regulated facilities: ", num)
                AllPrograms_db.write_per_fac(_database, "RCRA", 
                                             ds_type, "inspections", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in region CWA data")
        except KeyError:
            print(f"No RCRA inspection dataset for {region}")
        try:
            num = AllPrograms_util.get_num_events(
                region_data_sets[("RCRA Violations",region)], ds_type, _state, region, focus_year
            )
            if pgm_count_rcra > 0 and num is not None:
                num /= pgm_count_rcra
                print("RCRA violations per regulated facilities: ", num)
                AllPrograms_db.write_per_fac(_database, "RCRA", 
                                             ds_type, "violations", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in region CWA data")
        except KeyError:
            print(f"No RCRA violation dataset for {region}")

    # ### 19.  GHG emissions in these districts and states (2010-2018)
    # For each state and then each region, the get_ghg_emissions() function is called.  
    # It combines emissions records into years and sums the amounts.

    for region in state_regions:
        if _region_mode == 'County':
            region = region[0]
        if ("Greenhouse Gas Emissions", region) not in region_data_sets:
            print("No Greenhouse Gas Emission records for {} District {}".format(
                _state, region))
            continue
        if region is None:
            ds_type = ("State", None, _state)
        else:
            ds_type = (_region_mode, str(region), _state)
        print("Greenhouse Gas Emissions - {} District: {}".format(_state, region))
        df_ghg = AllPrograms_util.get_ghg_emissions(
            region_data_sets[("Greenhouse Gas Emissions",region)], ds_type
        )
        if df_ghg is not None:
            AllPrograms_db.write_ghg_emissions(_database, _region_mode, df_ghg, ds_type)

    # ### 20. Top facilities with compliance problems over the past 3 years
    # * The get_top_violators() function counts non-compliance quarters ('S' and 'V' violations) for facilities and then sorts the facilities.
    # * The chart_top_violators() function draws the chart.
    # * The functions are called for each region.

    for region in state_regions:
        if _region_mode == 'County':
            region = region[0]
        if region is None:
            ds_type = ("State", None, _state)
            df_active = state_echo_active
        else:
            ds_type = (_region_mode, str(region), _state)
            df_active = region_echo_active[region]
        df_violators = AllPrograms_util.get_top_violators(
            df_active,
            "AIR_FLAG",
            "CAA_3YR_COMPL_QTRS_HISTORY",
            "CAA_FORMAL_ACTION_COUNT",
            20,
        )
        if df_violators is not None:
            df_violators.rename(
                columns={"CAA_FORMAL_ACTION_COUNT": "formal_action_count"}, inplace=True
            )
            df_violators = df_violators.fillna(0)
            AllPrograms_db.write_top_violators(_database, _region_mode, df_violators, 
                                               ds_type, "CAA")

        df_violators = AllPrograms_util.get_top_violators(
            df_active,
            "NPDES_FLAG",
            "CWA_13QTRS_COMPL_HISTORY",
            "CWA_FORMAL_ACTION_COUNT",
            20,
        )
        if df_violators is not None:
            df_violators.rename(
                columns={"CWA_FORMAL_ACTION_COUNT": "formal_action_count"}, inplace=True
            )
            df_violators = df_violators.fillna(0)
            AllPrograms_db.write_top_violators(_database, _region_mode, df_violators, 
                                               ds_type, "CWA")

        df_violators = AllPrograms_util.get_top_violators(
            df_active,
            "RCRA_FLAG",
            "RCRA_3YR_COMPL_QTRS_HISTORY",
            "RCRA_FORMAL_ACTION_COUNT",
            20,
        )
        if df_violators is not None:
            df_violators.rename(
                columns={"RCRA_FORMAL_ACTION_COUNT": "formal_action_count"},
                inplace=True,
            )
            df_violators = df_violators.fillna(0)
            AllPrograms_db.write_top_violators(_database, _region_mode, df_violators, 
                                               ds_type, "RCRA")

    # Number of facilities by number of non-compliant quarters over the past 3 years.

    for region in state_regions:
        if _region_mode == 'County':
            region = region[0]
        if region is None:
            ds_type = ("State", None, _state)
        else:
            ds_type = (_region_mode, str(region), _state)
        df_active = state_echo_active
        if not df_active.empty:
            AllPrograms_db.write_violations_by_facilities(
                _database,
                df_active,
                ds_type,
                "CAA",
                "CAA_FORMAL_ACTION_COUNT",
                "AIR_FLAG",
                "CAA_3YR_COMPL_QTRS_HISTORY",
            )
            AllPrograms_db.write_violations_by_facilities(
                _database,
                df_active,
                ds_type,
                "CWA",
                "CWA_FORMAL_ACTION_COUNT",
                "NPDES_FLAG",
                "CWA_13QTRS_COMPL_HISTORY",
            )
            AllPrograms_db.write_violations_by_facilities(
                _database,
                df_active,
                ds_type,
                "RCRA",
                "RCRA_FORMAL_ACTION_COUNT",
                "RCRA_FLAG",
                "RCRA_3YR_COMPL_QTRS_HISTORY",
            )


def usage():
    print("Usage:  AllPrograms.py -b <database> -f <focus_year> -s <state> -f <focus_year> -m <mode>")
    exit


if __name__ == "__main__":
    if len(sys.argv) < 5:
        usage()
    else:
        main(sys.argv[1])
