#!/usr/bin/env python
# coding: utf-8

import pandas as pd
import numpy as np
import shutil
import sqlite3
import requests
import csv
import sys, argparse
import AllPrograms_db
import AllPrograms_util
from csv import reader
from datetime import datetime
from pandas.errors import EmptyDataError
from ECHO_modules.get_data import get_echo_data, read_file
from ECHO_modules.make_data_sets import make_data_sets
from ECHO_modules.utilities import make_filename, write_dataset, get_top_violators
# from AllPrograms_util import program_count, get_rowdata, get_cwa_df
# from AllPrograms_util import get_inspections, get_events, get_num_events
# from AllPrograms_util import write_events, get_num_facilities, get_enf_per_fac
# from AllPrograms_util import get_enforcements, get_ghg_emissions


def main( argv ):

    parser = argparse.ArgumentParser( prog='AllPrograms.py',
       description=\
       'Get data for all CDs and States passed in, and for the '\
       ' focus year specified. ' \
       'Write the data into the regions.db SQLite database.' )
    parser.add_argument( '-c', '--cds_file', required=True,
        help='The CDs to work with' )
    parser.add_argument( '-f', '--focus_year', required=True,
        help='The year on which the report will focus' )
    my_args = parser.parse_args()

    should_make_charts = False
    cds_filename = my_args.cds_file
    focus_year = str(my_args.focus_year)
    state_cds = []
    with open( cds_filename, 'r' ) as read_obj:
        csv_reader = reader( read_obj )
        raw_state_cds = list( map( tuple, csv_reader ))
    state_cds = []
    for state, cd in raw_state_cds:
        if ( cd == '0' ):
            cd = None
        else:
            cd = int( cd )
        state_cds.append((state,cd))

    states = list(set([s_cd[0] for s_cd in state_cds]))  #Use conversion to set to make unique

    data_set_list = ['RCRA Violations', 'RCRA Inspections', 'RCRA Penalties',
                 'CAA Violations', 'CAA Inspections', 'CAA Penalties', 'Greenhouse Gas Emissions', 
                 'CWA Violations', 'CWA Inspections', 'CWA Penalties', ] 

    # ### 4. This cell makes the data sets and stores the results for each of them from the database.  
    # This may take some time to run if you are looking at multiple congressional districts.
    # * The data_set_list from cell #3 is given to the make_data_sets() function which creates a DataSet object for each item in the list.
    # * Go through each of the (state, cd) pairs in the state_cd list specified in cell #3 and have the DataSet object store results returned by the database for that specific state and CD.
    # * Also go through each unique state in the list and store data for the entire state.

    data_sets=make_data_sets( data_set_list )
    print( "Congressional District data sets:")
    for state, cd in state_cds:
        if ( cd is None ):
            continue
        for ds_key, data_set in data_sets.items():
            print( state + '-' + str(cd) + ' - ' + ds_key )
            data_set.store_results( region_type='Congressional District', region_value=[cd], state=state )

    print( "State data sets:")
    for state in states:
        for ds_key, data_set in data_sets.items():
            print( state + ' - ' + ds_key )
            data_set.store_results( region_type='State', region_value=None, state=state )

    # ### 6. Get the State data for comparisons
    # Ask the database for ECHO_EXPORTER records for facilities in the state.
    # * state_echo_data is a dictionary with the state name as key and the data as value, for all records.
    # * state_echo_active is a dictionary for all records in state_echo_data identified as active.
    
    state_echo_data = {}
    state_echo_active = {}
    for state in states:
        state_echo_data[state] = read_file( 'ECHO_EXPORTER', 'State', state, None )
        if ( state_echo_data[state] is None ):
            sql = 'select * from "ECHO_EXPORTER" where "FAC_STATE" = \'{}\''.format( state )
            state_echo_data[state] = get_echo_data( sql, 'REGISTRY_ID' )
            write_dataset( state_echo_data[state], 'ECHO_EXPORTER', 'State', state, None )
        state_echo_active[state] = state_echo_data[state].loc[state_echo_data[state]['FAC_ACTIVE_FLAG']=='Y']
        print( 'There are {} active facilities in {}.'.format( 
            str(state_echo_active[state].shape[0]), state))
    
    cd_echo_data = {}
    cd_echo_active = {}
    for state, cd in state_cds:
        rowdata = []    
        if ( cd is None ):
            this_echo_data = state_echo_data[state]
        else:
            this_echo_data = state_echo_data[state].loc[state_echo_data[state]['FAC_DERIVED_CD113'] == cd]
            cd_echo_data[(state,cd)] = this_echo_data
        this_echo_active = this_echo_data.loc[this_echo_data['FAC_ACTIVE_FLAG']=='Y']
        if ( cd is not None ):
            cd_echo_active[(state,cd)] = this_echo_active
        active_facs = {}
        active_facs['CAA'] = AllPrograms_util.program_count( this_echo_active, 
                        'CAA', 'AIR_FLAG', state, cd) 
        active_facs['CWA'] = AllPrograms_util.program_count( this_echo_active, 
                        'CWA', 'NPDES_FLAG', state, cd) 
        active_facs['RCRA'] = AllPrograms_util.program_count( this_echo_active, 
                        'RCRA', 'RCRA_FLAG', state, cd) 
        active_facs['GHG'] = AllPrograms_util.program_count( this_echo_active, 
                        'GHG', 'GHG_FLAG', state, cd) 
        AllPrograms_db.write_active_facs( active_facs, state, cd )
    
    # ### 9. Number of recurring violations - facilities with 3+ quarters out of the last 12 in non-compliance, by each program
    # For each unique state and then each CD, we look at active records and count facilities that have 'S' or 'V' violations in 3 or more quarters.  The fields looked at are:
    # * CAA - CAA_3YR_COMPL_QTRS_HISTORY
    # * CWA - CWA_13QTRS_COMPL_HISTORY (Actually 13 quarters instead of 3 years.)
    # * RCRA - RCRA_3YR_COMPL_QTRS_HISTORY
    # 
    # * The get_rowdata() function takes the dataframe passed to it, and looks for records with 'S' or 'V' violations in more than 3 quarters. It divides the violations by the number of facilities, returning the raw count of facilities in violation more than 3 months and the percentage of facilities.
    
    rowdata_state = {}
    for state in states:
        print( "State: {}".format( state ))
        print( "  CAA")
        rowdata_state[state] = []
        rd = AllPrograms_util.get_rowdata( state_echo_active[state], 'CAA_3YR_COMPL_QTRS_HISTORY', 'AIR_FLAG')
        rowdata_state[state].append([ 'CAA', rd[1]])
        print( "  CWA")
        rd = AllPrograms_util.get_rowdata( state_echo_active[state], 'CWA_13QTRS_COMPL_HISTORY', 'NPDES_FLAG')
        rowdata_state[state].append([ 'CWA', rd[0], rd[1]])
        print( "  RCRA")
        rd = AllPrograms_util.get_rowdata( state_echo_active[state], 'RCRA_3YR_COMPL_QTRS_HISTORY', 'RCRA_FLAG')
        rowdata_state[state].append([ 'RCRA', rd[0], rd[1]])
    
    for state, cd in state_cds:
        rowdata_cd = []
        rd = AllPrograms_util.get_rowdata( cd_echo_active[(state,cd)], 'CAA_3YR_COMPL_QTRS_HISTORY', 'AIR_FLAG')
        rowdata_cd.append([ 'CAA', rd[0], rd[1]])
        print( "  CWA")
        rd = AllPrograms_util.get_rowdata( cd_echo_active[(state,cd)], 'CWA_13QTRS_COMPL_HISTORY', 'NPDES_FLAG')
        rowdata_cd.append([ 'CWA', rd[0], rd[1]])
        print( "  RCRA")
        rd = AllPrograms_util.get_rowdata( cd_echo_active[(state,cd)], 'RCRA_3YR_COMPL_QTRS_HISTORY', 'RCRA_FLAG')
        rowdata_cd.append([ 'RCRA', rd[0], rd[1]])
        AllPrograms_db.write_recurring_violations( state, cd, rowdata_cd )
    for state in states:
        AllPrograms_db.write_recurring_violations( state, None, rowdata_state[state] )
    
    # ### 10. Percent change in violations (CWA)
    # For each CD and then each unique state, 
    # * the quarter is identified in 5 digits, the 1st 4 are year and then 
    # * the quarter, as in 20013 for the 3rd quarter of 2001
    # * the quarter is stipped off, so that there will now be 4 records for 
    # * the facility for 2001 the values for the 4 types of violations--
    # * NUME90Q,NUMCVDT,NUMSVCD,NUMPSCH--are added together, over all 
    # * facilities, to get a single value for the year
    # * The results for the focus year are stored in the dictionary 
    # * effluent_violations_focus_year dictionary.  The key for the 
    # * dictionary is (state,cd).  These will be used in a later cell.
    
    effluent_violations_focus_year = {}  #For use later
    
    for state, cd in state_cds:
        if ( cd is None ):
            continue
        ds_type = ('Congressional District', str(cd).zfill(2), state)
        print( "CWA Violations - {} District: {}".format( state, cd ))
        df = data_sets["CWA Violations"].results[('Congressional District', str(cd), state)].dataframe.copy()
        effluent_violations_all = AllPrograms_util.get_cwa_df( df )
        for idx,row in effluent_violations_all.iterrows():
            if ( idx == focus_year ):
                effluent_violations_focus_year[(state,cd)] = row['Total']
        AllPrograms_db.write_CWA_violations( effluent_violations_all, ds_type )

    for state in states:
        ds_type = ('State', None, state)
        df = data_sets["CWA Violations"].results[('State', None, state)].dataframe.copy()
        effluent_violations_all = AllPrograms_util.get_cwa_df( df )
        for idx,row in effluent_violations_all.iterrows():
            if ( idx == focus_year ):
                effluent_violations_focus_year[(state,None)] = row['Total']
        AllPrograms_db.write_CWA_violations( effluent_violations_all, ds_type )
    
    # ### 11. Percent change in inspections
    # For each CD the date field for that program type is used to count up all inspections for the year.  (The date field for each data set is identified in make_data_sets() when the DataSet object is created.  It shows up here as ds.date_field.)

    for state, cd in state_cds:
        if ( cd is None ):
            ds_type = ('State', None, state)
        else:
            ds_type = ('Congressional District', str(cd), state)
        print( "CAA Inspections - {} District: {}".format( state, cd ))
        df_caa = AllPrograms_db.write_inspections( 'CAA', data_sets["CAA Inspections"], ds_type )
    
        print( "CWA Inspections - {} District: {}".format( state, cd ))
        df_cwa = AllPrograms_db.write_inspections( 'CWA', data_sets["CWA Inspections"], ds_type )
    
        print( "RCRA Inspections - {} District: {}".format( state, cd ))
        df_rcra = AllPrograms_db.write_inspections( 'RCRA', data_sets["RCRA Inspections"], ds_type )
    
        df_totals = pd.concat( [df_caa, df_cwa, df_rcra] )
        df_totals = df_totals.groupby( df_totals.index ).agg('sum')
        AllPrograms_db.write_total_inspections( 'All', df_totals, ds_type )
        print( "Total inspections for {} district {}".format( state,cd ))
        
    for state in states:
        ds_type = ('State', None, state)
        print( "CAA Inspections - {} District: {}".format( state, cd ))
        df_caa = AllPrograms_db.write_inspections( 'CAA', data_sets["CAA Inspections"], ds_type )
    
        print( "CWA Inspections - {} District: {}".format( state, cd ))
        df_cwa = AllPrograms_db.write_inspections( 'CWA', data_sets["CWA Inspections"], ds_type )
    
        print( "RCRA Inspections - {} District: {}".format( state, cd ))
        df_rcra = AllPrograms_db.write_inspections( 'RCRA', data_sets["RCRA Inspections"], ds_type )
    
        df_totals = pd.concat( [df_caa, df_cwa, df_rcra] )
        df_totals = df_totals.groupby( df_totals.index ).agg('sum')
        AllPrograms_db.write_total_inspections( 'All', df_totals, ds_type )
        print( 'Total inspections for {}'.format( state ))
        
    # ### 12. Percent change in enforcement - penalties and number of enforcements
    # * For each CD the number of enforcements and amount of penalty are retrieved 
    # * from the agg_col field (specified in make_data_sets() for each DataSet).  
    # * The number of penalties and amount are accummulated for each year.

    for state, cd in state_cds:
        if ( cd is None ):
            ds_type = ('State', None, state)
        else:
            ds_type = ('Congressional District', str(cd), state)
        print( "CAA Penalties - {} District: {}".format( state, cd ))
        df_caa = AllPrograms_db.write_enforcements( 'CAA', data_sets["CAA Penalties"], ds_type )
    
        print( "CWA Penalties - {} District: {}".format( state, cd ))
        df_cwa = AllPrograms_db.write_enforcements( 'CWA', data_sets["CWA Penalties"], ds_type )
    
        print( "RCRA Penalties - {} District: {}".format( state, cd ))
        df_rcra = AllPrograms_db.write_enforcements( 'RCRA', data_sets["RCRA Penalties"], ds_type )
    
        df_totals = pd.concat( [df_caa, df_cwa, df_rcra] )
        df_totals = df_totals.groupby( df_totals.index ).agg('sum')
        AllPrograms_db.write_total_enforcements( 'All', df_totals, ds_type )
        print( "Total Penalties for {} district {}".format( state,cd ))
        
    for state in states:
        ds_type = ('State', None, state)
        print( "CAA Penalties - {} District: {}".format( state, cd ))
        df_caa = AllPrograms_db.write_enforcements( 'CAA', data_sets["CAA Penalties"], ds_type )
    
        print( "CWA Penalties - {} District: {}".format( state, cd ))
        df_cwa = AllPrograms_db.write_enforcements( 'CWA', data_sets["CWA Penalties"], ds_type )
    
        print( "RCRA Penalties - {} District: {}".format( state, cd ))
        df_rcra = AllPrograms_db.write_enforcements( 'RCRA', data_sets["RCRA Penalties"], ds_type )
    
        df_totals = pd.concat( [df_caa, df_cwa, df_rcra] )
        df_totals = df_totals.groupby( df_totals.index ).agg('sum')
        AllPrograms_db.write_total_enforcements( 'All', df_totals, ds_type )
        print( 'Total Penalties for {}'.format( state ))
        
    
    # ### 13.a. Focus year - inspections per 1000 regulated facilities - by district
    # * For each CD the inspections data is again grouped into years.
    # * The get_num_events() function counts all events it gets from 
    # * get_events() for the year that is requested, which is focus_year.
    # * This number is divided by the number of facilities in the district, from 
    # * the program_count() function of cell #7.
    # * The result is multiplied by 1000, equivalent to dividing the denominator 
    # * (number of facilities) by 1000.
    # Inspections and violations per 1000 facilities for the focus year
    
    for state, cd in state_cds:
        if ( cd is None ):
            continue
        ds_type = ('Congressional District', str(cd), state)
        try:
            num = 1000. * AllPrograms_util.get_num_events( data_sets["CAA Inspections"], 
                    ds_type, state, cd, focus_year ) / AllPrograms_util.program_count( 
                        cd_echo_active[(state,cd)], 'CAA', 'AIR_FLAG', 
                        state, cd)
            print("CAA inspections per 1000 regulated facilities: ", num)
            AllPrograms_db.write_per_1000('CAA', ds_type, 'inspections', 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in cd CWA data")
        try:
            num = 1000. * AllPrograms_util.get_num_events( data_sets["CAA Violations"], 
                    ds_type, state, cd, focus_year ) / AllPrograms_util.program_count( 
                        cd_echo_active[(state,cd)], 'CAA', 'AIR_FLAG', 
                        state, cd)
            print("CAA violations per 1000 regulated facilities: ", num)
            AllPrograms_db.write_per_1000('CAA', ds_type, 'violations', 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in cd CWA data")
        try:
            num = 1000. * AllPrograms_util.get_num_events( data_sets["CWA Inspections"], 
                    ds_type, state, cd, focus_year ) / AllPrograms_util.program_count( 
                        cd_echo_active[(state,cd)], 'CWA', 'NPDES_FLAG', 
                        state, cd)
            print("CWA inspections per 1000 regulated facilities: ", num)
            AllPrograms_db.write_inspections_per_1000('CWA', ds_type, 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in cd CWA data")
        try:
            # Have to handle CWA Violations differently - use saved dictionary from cell 10
            num = effluent_violations_focus_year[(state,cd)]
            num = 1000. * num / AllPrograms_util.program_count( 
                cd_echo_active[(state,cd)], 'CWA', 'NPDES_FLAG', state, cd)
            print("CWA violations per 1000 regulated facilities: ", num)
            AllPrograms_db.write_violations_per_1000('CWA', ds_type, 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in state CWA data")
        try:
            num = 1000. * AllPrograms_util.get_num_events( data_sets["RCRA Inspections"], 
                    ds_type, state, cd, focus_year ) / AllPrograms_util.program_count( 
                        cd_echo_active[(state,cd)], 'RCRA', 'RCRA_FLAG', 
                        state, cd)
            print("RCRA inspections per 1000 regulated facilities: ", num)
            AllPrograms_db.write_inspections_per_1000('RCRA', ds_type, 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in cd CWA data")
        try:
            num = 1000. * AllPrograms_util.get_num_events( data_sets["RCRA Violations"], 
                    ds_type, state, cd, focus_year ) / AllPrograms_util.program_count( 
                        cd_echo_active[(state,cd)], 'RCRA', 'RCRA_FLAG', 
                        state, cd)
            print("RCRA violations per 1000 regulated facilities: ", num)
            AllPrograms_db.write_violations_per_1000('RCRA', ds_type, 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in cd CWA data")
    
    
    for state in states:
        ds_type = ('State', None, state)
        try:
            num = 1000. * AllPrograms_util.get_num_events( data_sets["CAA Inspections"], 
                    ds_type, state, None, focus_year ) / AllPrograms_util.program_count( 
                        state_echo_active[state], 'CAA', 'AIR_FLAG', 
                        state, None)
            print("CAA inspections per 1000 regulated facilities: ", num)
            AllPrograms_db.write_inspections_per_1000('CAA', ds_type, 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in cd CWA data")
        try:
            num = 1000. * AllPrograms_util.get_num_events( data_sets["CAA Violations"], 
                    ds_type, state, None, focus_year ) / AllPrograms_util.program_count( 
                        state_echo_active[state], 'CAA', 'AIR_FLAG', 
                        state, None)
            print("CAA violations per 1000 regulated facilities: ", num)
            AllPrograms_db.write_violations_per_1000('CAA', ds_type, 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in cd CAA data")
        try:
            num = 1000. * AllPrograms_util.get_num_events( data_sets["CWA Inspections"], 
                    ds_type, state, None, focus_year ) / AllPrograms_util.program_count( 
                        state_echo_active[state], 'CWA', 'NPDES_FLAG', 
                        state, None)
            print("CWA inspections per 1000 regulated facilities: ", num)
            AllPrograms_db.write_inspections_per_1000('CWA', ds_type, 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in cd CWA data")
        try:
            # Have to handle CWA Violations differently - use saved dictionary from cell 10
            num = effluent_violations_focus_year[(state,cd)]
            num = 1000. * num / AllPrograms_util.program_count( 
                    state_echo_active[state], 'CWA', 'NPDES_FLAG', state, None)
            print("CWA violations per 1000 regulated facilities: ", num)
            AllPrograms_db.write_violations_per_1000('CWA', ds_type, 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in state CWA data")
        try:
            num = 1000. * AllPrograms_util.get_num_events( data_sets["RCRA Inspections"], 
                    ds_type, state, None, focus_year ) / AllPrograms_util.program_count( 
                        state_echo_active[state], 'RCRA', 'RCRA_FLAG', 
                        state, None)
            print("RCRA inspections per 1000 regulated facilities: ", num)
            AllPrograms_db.write_inspections_per_1000('RCRA', ds_type, 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in cd CWA data")
        try:
            num = 1000. * AllPrograms_util.get_num_events( data_sets["RCRA Violations"], 
                    ds_type, state, None, focus_year ) / AllPrograms_util.program_count( 
                        state_echo_active[state], 'RCRA', 'RCRA_FLAG', 
                        state, None)
            print("RCRA violations per 1000 regulated facilities: ", num)
            AllPrograms_db.write_violations_per_1000('RCRA', ds_type, 0, num )
        except pd.errors.OutOfBoundsDatetime:
            print( "Bad date in cd CWA data")


    # ### 17. Focus year - enforcement counts and amounts per violating facility - by district
    # * The get_num_facilities() function combines the violations into years, then counts the number of facilities with violations for each year.
    # * The get_enf_per_fac() function combines enforcements into years, then counts the enforcements and sums the amount of penalties, before dividing by the results from get_num_facilities().
    # * These functions are called for each CD, and for CAA, CWA and RCRA.

   # Enforcement counts and amounts per violating facility

    for state, cd in state_cds:
        if ( cd is None ):
            continue
        ds_type = ('Congressional District', str(cd), state)
     
        num_fac = AllPrograms_util.get_num_facilities( data_sets, "CAA Violations", 
                                                      ds_type, focus_year )
        message = "CAA Penalties - {} District: {} - {} facilities with violations in {}"
        print( message.format( state, cd, num_fac, focus_year ))
        df_caa = AllPrograms_db.write_enf_per_fac( 'CAA', data_sets['CAA Penalties'],
                                                  ds_type, num_fac, focus_year )
    
        num_fac = AllPrograms_util.get_num_facilities( data_sets, "CWA Violations", 
                                                      ds_type, focus_year )
        message = "CWA Penalties - {} District: {} - {} facilities with violations in {}"
        print( message.format( state, cd, num_fac, focus_year ))
        df_cwa = AllPrograms_db.write_enf_per_fac( 'CWA', data_sets['CWA Penalties'],
                                                  ds_type, num_fac, focus_year )
    
        num_fac = AllPrograms_util.get_num_facilities( data_sets, "RCRA Violations", 
                                                      ds_type, focus_year )
        message = "RCRA Penalties - {} District: {} - {} facilities with violations in {}"
        print( message.format( state, cd, num_fac, focus_year ))
        df_rcra = AllPrograms_db.write_enf_per_fac( 'RCRA', data_sets['RCRA Penalties'],
                                                  ds_type, num_fac, focus_year )
    
        df_totals = pd.concat( [df_caa, df_cwa, df_rcra] )
        df_totals = df_totals.groupby( df_totals.index ).agg('sum')
        print( "Total enforcements for {} district {} in {}".format( 
            state,cd,focus_year ))
        AllPrograms_db.write_total_enf_per_fac( df_totals, ds_type )
        print( df_totals )
    
    for state in states:
        ds_type = ('State', None, state)
    
        num_fac = AllPrograms_util.get_num_facilities( data_sets, "CAA Violations", 
                                                      ds_type, focus_year )
        message = "CAA Penalties - {}  - {} facilities with violations in {}"
        print( message.format( state, num_fac, focus_year ))
        df_caa = AllPrograms_db.write_enf_per_fac( 'CAA', data_sets['CAA Penalties'],
                                                  ds_type, num_fac, focus_year )
    
        num_fac = AllPrograms_util.get_num_facilities( data_sets, "CWA Violations", 
                                                      ds_type, focus_year )
        message = "CWA Penalties - {} - {} facilities with violations in {}"
        print( message.format( state, num_fac, focus_year ))
        df_cwa = AllPrograms_db.write_enf_per_fac( 'CWA', data_sets['CWA Penalties'],
                                                  ds_type, num_fac, focus_year )
    
        num_fac = AllPrograms_util.get_num_facilities( data_sets, "RCRA Violations", 
                                                      ds_type, focus_year )
        message = "RCRA Penalties - {}  - {} facilities with violations in {}"
        print( message.format( state, num_fac, focus_year ))
        df_rcra = AllPrograms_db.write_enf_per_fac( 'RCRA', data_sets['RCRA Penalties'],
                                                  ds_type, num_fac, focus_year )
    
        df_totals = pd.concat( [df_caa, df_cwa, df_rcra] )
        df_totals = df_totals.groupby( df_totals.index ).agg('sum')
        print( "Total enforcements for {}  in {}".format( state,focus_year ))
        AllPrograms_db.write_total_enf_per_fac( df_totals, ds_type )
        print( df_totals )

    
    # ### 19.  GHG emissions in these districts and states (2010-2018)
    # For each state and then each CD, the get_ghg_emissions() function is called.  It combines emissions records into years and sums the amounts.
    for state in states:
        ds_type = ('State', None, state)
        print( "Greenhouse Gas Emissions - State: {}".format( state ))
        df_ghg = AllPrograms_util.get_ghg_emissions( data_sets["Greenhouse Gas Emissions"], ds_type )
        if ( df_ghg is not None ):
            AllPrograms_db.write_ghg_emissions( df_ghg, ds_type )
    
    for state, cd in state_cds:
        if ( cd is None ):
            continue
        ds_type = ('Congressional District', str(cd), state)
        print( "Greenhouse Gas Emissions - {} District: {}".format( state, cd ))
        df_ghg = AllPrograms_util.get_ghg_emissions( data_sets["Greenhouse Gas Emissions"], ds_type )
        if (df_ghg is not None):
            AllPrograms_db.write_ghg_emissions( df_ghg, ds_type )
   
    
    # ### 20. Top facilities with compliance problems over the past 3 years
    # * The get_top_violators() function counts non-compliance quarters ('S' and 'V' violations) for facilities and then sorts the facilities.
    # * The chart_top_violators() function draws the chart.
    # * The functions are called for each CD.

    for state, cd in state_cds:
        if ( cd is None ):
            df_active = state_echo_active[state]
            ds_type = ('State', None, state)
        else:
            df_active = cd_echo_active[(state,cd)]
            ds_type = ('Congressional District', state, cd)
        df_violators = get_top_violators( df_active, 'AIR_FLAG', 
                'CAA_3YR_COMPL_QTRS_HISTORY', 'CAA_FORMAL_ACTION_COUNT', 20 )
        if ( df_violators is not None ):
            df_violators.rename( columns={'CAA_FORMAL_ACTION_COUNT': 'formal_action_count'},
                               inplace=True)
            df_violators = df_violators.fillna(0)
            AllPrograms_db.write_top_violators( df_violators, ds_type, 'CAA' )
        
        df_violators = get_top_violators( df_active, 'NPDES_FLAG', 
                'CWA_13QTRS_COMPL_HISTORY', 'CWA_FORMAL_ACTION_COUNT', 20 )
        if ( df_violators is not None ):
            df_violators.rename( columns={'CWA_FORMAL_ACTION_COUNT': 'formal_action_count'},
                               inplace=True)
            df_violators = df_violators.fillna(0)
            AllPrograms_db.write_top_violators( df_violators, ds_type, 'CWA' )
        
        df_violators = get_top_violators( df_active, 'RCRA_FLAG', 
                'RCRA_3YR_COMPL_QTRS_HISTORY', 'RCRA_FORMAL_ACTION_COUNT', 20 )
        if ( df_violators is not None ):
            df_violators.rename( columns={'RCRA_FORMAL_ACTION_COUNT': 'formal_action_count'},
                               inplace=True)
            df_violators = df_violators.fillna(0)
            AllPrograms_db.write_top_violators( df_violators, ds_type, 'RCRA' )

    # Number of facilities by number of non-compliant quarters over the past 3 years.

    for state, cd in state_cds:
        if ( cd is None ):
            df_active = state_echo_active[state]
            ds_type = ('State', None, state)
        else:
            df_active = cd_echo_active[(state,cd)]
            ds_type = ('Congressional District', state, cd)
        AllPrograms_db.write_violations_by_facilities( df_active, ds_type, 'CAA',
                                                     'CAA_FORMAL_ACTION_COUNT', 
                                                     'AIR_FLAG', 'CAA_3YR_COMPL_QTRS_HISTORY')
        AllPrograms_db.write_violations_by_facilities( df_active, ds_type, 'CWA',
                                                     'CWA_FORMAL_ACTION_COUNT', 
                                                     'NPDES_FLAG', 'CWA_13QTRS_COMPL_HISTORY')
        AllPrograms_db.write_violations_by_facilities( df_active, ds_type, 'RCRA',
                                                     'RCRA_FORMAL_ACTION_COUNT', 
                                                     'RCRA_FLAG', 'RCRA_3YR_COMPL_QTRS_HISTORY')

if __name__ == "__main__":
    main( sys.argv[1] )