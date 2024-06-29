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
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-d", "--cds_state", required=False, help="The state to work with")
    group.add_argument("-c", "--county_state", required=False, help="The state to work with")
    my_args = parser.parse_args()

    _database = my_args.database
    _region_mode = 'County'
    state_regions = []
    state_counties = pd.DataFrame()
    if my_args.cds_state:
        _region_mode = 'Congressional District'
        # Read the CDs for the given state (cds_state) from the
        # region.db table real_cds
        """ Old code
        with open(my_args.cds_file, "r") as read_obj:
            csv_reader = reader(read_obj)
            raw_state_regions = list(map(tuple, csv_reader))
        for state, region in raw_state_regions:
        """
        state = my_args.cds_state
        cds = AllPrograms_db.get_real_cds(_database, state)
        if int(cds[0][0]) == 0:
            # This is a state with only one CD, but the ECHO data
            # may show some incorrect values. Treat this as a full
            # state.
            _region_mode = 'State'
            state_regions.append((state, None))
        else: # Congressional District or County
            for cd in cds:
                cd = int(cd[0])
                # pd.concat(state_regions, (state, cd))
                state_regions.append((state, cd))
    elif my_args.county_state:
        _region_mode = 'County'
        url = "https://raw.githubusercontent.com/edgi-govdata-archiving/"
        url += "ECHO_modules/main/data/state_counties_corrected.csv"
        all_counties = pd.read_csv(url)
        state_counties = pd.DataFrame()
        this_state_counties = all_counties[all_counties['FAC_STATE'] == my_args.county_state]
        state_counties = pd.concat([state_counties, this_state_counties], ignore_index=True)
        state_regions = pd.unique(pd.Series(
            state_counties[['FAC_STATE', 'County']].apply(tuple, axis=1).tolist()))
        state_regions = state_regions.tolist()
    else:
        print('Option -c, -d, must be used')
        exit

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

    region_echo_active = {}
    remove_state_regions = []
    if _region_mode == 'County':
        cds_or_counties = state_counties[state_counties['FAC_STATE'] == state]
    else:
        cds_or_counties = AllPrograms_util.get_cd118_shapefile(state)
    for state, region in state_regions:
        try:
            try:
                region_echo_active[(state, region)] = (
                    AllPrograms_db.get_active_facs(_region_mode, state, region, cds_or_counties))
                print("Active facilities for {}-{} = {}".format(state, region,
                                                                len(region_echo_active[(state, region)])))
            except urllib.error.HTTPError:
                print("Database query for county {}-{} failed.".format(state, region))
                remove_state_regions.append((state, region))
        except pd.errors.EmptyDataError:
            # No facilities in this (state,region).  Mark for removal.
            print("No active facilities in {}-{}".format(state, region))
            remove_state_regions.append((state, region))
    # Remove any (state,region) that had no facilities
    for state, region in remove_state_regions:
        state_regions.remove((state, region))

    for state, region in state_regions:
        rowdata = []
        active_facs = {"CAA": AllPrograms_util.program_count(
            region_echo_active[(state, region)], "CAA", "AIR_FLAG", state, region
        ), "CWA": AllPrograms_util.program_count(
            region_echo_active[(state, region)], "CWA", "NPDES_FLAG", state, region
        ), "RCRA": AllPrograms_util.program_count(
            region_echo_active[(state, region)], "RCRA", "RCRA_FLAG", state, region
        ), "GHG": AllPrograms_util.program_count(
            region_echo_active[(state, region)], "GHG", "GHG_FLAG", state, region
        )}
        AllPrograms_db.write_active_facs(_database, _region_mode, active_facs, state, region)

    states = list(
        set([s_region[0] for s_region in state_regions])
    )  # Use conversion to set to make unique

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
    print("{} data sets:".format(_region_mode))
    for state, region in state_regions:
        for ds_key, data_set in data_sets.items():
            print(state + "-" + str(region) + " - " + ds_key)
            data_set.store_results(
                region_type=_region_mode, 
                        region_value=region if region is None else str(region), 
                        state=state
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
    # * RCRA - RCRA_3YR_COMPL_QTRS_HISTORY
    #
    # * The get_rowdata() function takes the dataframe passed to it, and looks 
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

    for state, region in state_regions:
        region_value = None if region is None else str(region)
        ds_type = ("{}".format(_region_mode), region_value, state)
        rowdata_region = []
        rd = AllPrograms_util.get_rowdata(
            region_echo_active[(state, region)], "CAA_3YR_COMPL_QTRS_HISTORY", "AIR_FLAG"
        )
        rowdata_region.append(["CAA", rd[0], rd[1]])
        num_fac = rd[1]
        message = (
            "CAA Penalties - {} District: {} - {} facilities with violations in {}"
        )
        print(message.format(state, region, num_fac, focus_year))
        df_caa = AllPrograms_db.write_enf_per_fac(_database, _region_mode,
            "CAA", data_sets["CAA Penalties"], ds_type, num_fac, focus_year)

        print("  CWA")
        rd = AllPrograms_util.get_rowdata(
            region_echo_active[(state, region)], "CWA_13QTRS_COMPL_HISTORY", "NPDES_FLAG"
        )
        rowdata_region.append(["CWA", rd[0], rd[1]])
        num_fac = rd[1]
        message = (
            "CWA Penalties - {} District: {} - {} facilities with violations in {}"
        )
        print(message.format(state, region, num_fac, focus_year))
        df_cwa = AllPrograms_db.write_enf_per_fac(_database, _region_mode,
            "CWA", data_sets["CWA Penalties"], ds_type, num_fac, focus_year
        )

        print("  RCRA")
        rd = AllPrograms_util.get_rowdata(
            region_echo_active[(state, region)], "RCRA_3YR_COMPL_QTRS_HISTORY", "RCRA_FLAG"
        )
        rowdata_region.append(["RCRA", rd[0], rd[1]])
        num_fac = rd[1]
        message = (
            "RCRA Penalties - {} District: {} - {} facilities with violations in {}"
        )
        print(message.format(state, region, num_fac, focus_year))
        df_rcra = AllPrograms_db.write_enf_per_fac(_database, _region_mode,
            "RCRA", data_sets["RCRA Penalties"], ds_type, num_fac, focus_year
        )

        AllPrograms_db.write_recurring_violations(_database, _region_mode, 
                                                  state, region, rowdata_region)

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

    for state, region in state_regions:
        region_value = region
        if _region_mode != 'State':
            region_value = str(region)
        ds_type = ("{}".format(_region_mode), region_value, state)
        print("CWA Violations - {} District: {}".format(state, region))
        df = (
            data_sets["CWA Violations"]
            .results[("{}".format(_region_mode), region_value, state)]
            .dataframe
        )
        if df is None:
            continue
        else:
            df = df.copy()
        effluent_violations_all = AllPrograms_util.get_cwa_df(df, focus_year)
        for idx, row in effluent_violations_all.iterrows():
            if idx == focus_year:
                effluent_violations_focus_year[(state, region)] = row["Total"]
        AllPrograms_db.write_CWA_violations(_database, _region_mode, 
                                            effluent_violations_all, ds_type)

    # ### 11. Percent change in inspections
    # For each region the date field for that program type is used to count up all 
    # inspections for the year.  (The date field for each data set is identified in 
    # make_data_sets() when the DataSet object is created.  It shows up here as ds.date_field.)
    # ### 12. Percent change in enforcement - penalties and number of enforcements
    # * For each region the number of enforcements and amount of penalty are retrieved
    # * from the agg_col field (specified in make_data_sets() for each DataSet).
    # * The number of penalties and amount are accummulated for each year.

    for state, region in state_regions:
        if region is None:
            ds_type = ("State", None, state)
        else:
            ds_type = (_region_mode, str(region), state)
        print("CAA Inspections - {} District: {}".format(state, region))
        df_caa = AllPrograms_db.write_inspections(_database, _region_mode,
            "CAA", data_sets["CAA Inspections"], ds_type
        )

        print("CWA Inspections - {} District: {}".format(state, region))
        df_cwa = AllPrograms_db.write_inspections(_database, _region_mode,
            "CWA", data_sets["CWA Inspections"], ds_type
        )

        print("RCRA Inspections - {} District: {}".format(state, region))
        df_rcra = AllPrograms_db.write_inspections(_database, _region_mode,
            "RCRA", data_sets["RCRA Inspections"], ds_type
        )

        print("CAA Violations - {} District: {}".format(state, region))
        df_caa = AllPrograms_db.write_violations(_database, _region_mode,
            "CAA", data_sets["CAA Violations"], ds_type
        )

        print("RCRA Violations - {} District: {}".format(state, region))
        df_rcra = AllPrograms_db.write_violations(_database, _region_mode,
            "RCRA", data_sets["RCRA Violations"], ds_type
        )

        print("CAA Penalties - {} District: {}".format(state, region))
        df_caa = AllPrograms_db.write_enforcements(_database, _region_mode,
            "CAA", data_sets["CAA Penalties"], ds_type, focus_year
        )

        print("CWA Penalties - {} District: {}".format(state, region))
        df_cwa = AllPrograms_db.write_enforcements(_database, _region_mode,
            "CWA", data_sets["CWA Penalties"], ds_type, focus_year
        )

        print("RCRA Penalties - {} District: {}".format(state, region))
        df_rcra = AllPrograms_db.write_enforcements(_database, _region_mode,
            "RCRA", data_sets["RCRA Penalties"], ds_type, focus_year
        )

    # ### 13.a. Focus year - inspections per regulated facility - by district
    # * For each region the inspections data is again grouped into years.
    # * The get_num_events() function counts all events it gets from
    # * get_events() for the year that is requested, which is focus_year.
    # * This number is divided by the number of facilities in the district, from
    # * the program_count() function of cell #7.
    # Inspections and violations per facility for the focus year

    for state, region in state_regions:
        if region is None:
            ds_type = ("State", None, state)
        else:
            ds_type = (_region_mode, str(region), state)
        pgm_count_caa = AllPrograms_util.program_count(
            region_echo_active[(state, region)], "CAA", "AIR_FLAG", state, region
        )
        pgm_count_cwa = AllPrograms_util.program_count(
            region_echo_active[(state, region)], "CWA", "NPDES_FLAG", state, region
        )
        pgm_count_rcra = AllPrograms_util.program_count(
            region_echo_active[(state, region)], "RCRA", "RCRA_FLAG", state, region
        )
        try:
            num = AllPrograms_util.get_num_events(
                data_sets["CAA Inspections"], ds_type, state, region, focus_year
            )
            if pgm_count_caa > 0 and num is not None:
                num = num / pgm_count_caa
                print("CAA inspections per regulated facilities: ", num)
                AllPrograms_db.write_per_fac(_database, _region_mode, "CAA", 
                                             ds_type, "inspections", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in region CWA data")
        try:
            num = AllPrograms_util.get_num_events(
                data_sets["CAA Violations"], ds_type, state, region, focus_year
            )
            if pgm_count_caa > 0 and num is not None:
                num /= pgm_count_caa
                print("CAA violations per regulated facilities: ", num)
                AllPrograms_db.write_per_fac(_database, _region_mode, "CAA", 
                                             ds_type, "violations", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in region CWA data")
        try:
            num = AllPrograms_util.get_num_events(
                data_sets["CWA Inspections"], ds_type, state, region, focus_year
            )
            if pgm_count_cwa > 0 and num is not None:
                num /= pgm_count_cwa
                print("CWA inspections per regulated facilities: ", num)
                AllPrograms_db.write_per_fac(_database, _region_mode, "CWA", 
                                             ds_type, "inspections", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in region CWA data")
        try:
            # Have to handle CWA Violations differently - use saved dictionary from cell 10
            if (state, region) in effluent_violations_focus_year:
                num = effluent_violations_focus_year[(state, region)]
                if pgm_count_cwa > 0 and num is not None:
                    num = num / pgm_count_cwa
                    AllPrograms_db.write_per_fac(_database, _region_mode, "CWA", 
                                                 ds_type, "violations", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in state CWA data")
        try:
            num = AllPrograms_util.get_num_events(
                data_sets["RCRA Inspections"], ds_type, state, region, focus_year
            )
            if pgm_count_rcra > 0 and num is not None:
                num /= pgm_count_rcra
                print("RCRA inspections per regulated facilities: ", num)
                AllPrograms_db.write_per_fac(_database, _region_mode, "RCRA", 
                                             ds_type, "inspections", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in region CWA data")
        try:
            num = AllPrograms_util.get_num_events(
                data_sets["RCRA Violations"], ds_type, state, region, focus_year
            )
            if pgm_count_rcra > 0 and num is not None:
                num /= pgm_count_rcra
                print("RCRA violations per regulated facilities: ", num)
                AllPrograms_db.write_per_fac(_database, _region_mode, "RCRA", 
                                             ds_type, "violations", focus_year, num)
        except pd.errors.OutOfBoundsDatetime:
            print("Bad date in region CWA data")

    # ### 19.  GHG emissions in these districts and states (2010-2018)
    # For each state and then each region, the get_ghg_emissions() function is called.  
    # It combines emissions records into years and sums the amounts.

    for state, region in state_regions:
        if region is None:
            ds_type = ("State", None, state)
        else:
            ds_type = (_region_mode, str(region), state)
        print("Greenhouse Gas Emissions - {} District: {}".format(state, region))
        df_ghg = AllPrograms_util.get_ghg_emissions(
            data_sets["Greenhouse Gas Emissions"], ds_type
        )
        if df_ghg is not None:
            AllPrograms_db.write_ghg_emissions(_database, _region_mode, df_ghg, ds_type)

    # ### 20. Top facilities with compliance problems over the past 3 years
    # * The get_top_violators() function counts non-compliance quarters ('S' and 'V' violations) for facilities and then sorts the facilities.
    # * The chart_top_violators() function draws the chart.
    # * The functions are called for each region.

    for state, region in state_regions:
        if region is None:
            ds_type = ("State", None, state)
        else:
            ds_type = (_region_mode, str(region), state)
        df_active = region_echo_active[(state, region)]
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

    for state, region in state_regions:
        if region is None:
            ds_type = ("State", None, state)
        else:
            ds_type = (_region_mode, str(region), state)
        df_active = region_echo_active[(state, region)]
        if not df_active.empty:
            AllPrograms_db.write_violations_by_facilities(
                _database,
                _region_mode,
                df_active,
                ds_type,
                "CAA",
                "CAA_FORMAL_ACTION_COUNT",
                "AIR_FLAG",
                "CAA_3YR_COMPL_QTRS_HISTORY",
            )
            AllPrograms_db.write_violations_by_facilities(
                _database,
                _region_mode,
                df_active,
                ds_type,
                "CWA",
                "CWA_FORMAL_ACTION_COUNT",
                "NPDES_FLAG",
                "CWA_13QTRS_COMPL_HISTORY",
            )
            AllPrograms_db.write_violations_by_facilities(
                _database,
                _region_mode,
                df_active,
                ds_type,
                "RCRA",
                "RCRA_FORMAL_ACTION_COUNT",
                "RCRA_FLAG",
                "RCRA_3YR_COMPL_QTRS_HISTORY",
            )


def usage():
    print("Usage:  AllPrograms.py [-d cds_todo.csv | -c state] -f <focus_year>")
    exit


if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
    else:
        main(sys.argv[1])
