#!/usr/bin/env python
#coding: utf-8

import pdb
import sys, argparse
import subprocess
import os.path
import sqlite3
from datetime import datetime

def move_if_exists(filename, timestamp):
    new_name = "{}-{}".format(filename, timestamp)
    if os.path.exists(filename):
        command = "mv {} {}".format(filename, new_name)
        rslt = subprocess.call(command,shell=True)
        if rslt != 0:
            print("{} not found.".format(filename ))

def clean_to_start_state(database, states_file, start_state):
    # Skip down to start_state, where an error may have occurred.
    # Remove database entries for start_state, as they will be
    # re-calculated.
    this_state = states_file.readline().rstrip()
    while this_state != start_state and this_state != "":
        this_state = states_file.readline().rstrip()
    print("got to {}".format(this_state))
    if this_state != "":
        clean_region_db(database, this_state)
    return this_state

def clean_region_db(database, state = None):
    # Only clean the regions table if state is ''
    # *_per_1000 tables are always fully deleted as they are
    # constructed after all states by a separate program.
    conn = sqlite3.connect(database)
    cur = conn.cursor()
    tables = ['active_facilities',
              'violations',
              'per_fac',
              'inspections',
              'recurring_violations',
              'enforcements',
              'ghg_emissions',
              'non_compliants',
              'violations_by_facilities',
              'enf_per_fac',
              ]
    per_1000_tables = ['state_per_1000',
                       'cd_per_1000',
                       'county_per_1000',
                       ]
    for table in tables:
        sql = "delete from {}".format(table)
        if state is not None:
            sql += " where region_id in (select rowid from regions where state = '{}')"
            sql.format(state)
        print(f'Cleaning database {database}: {sql}')
        cur.execute(sql)
    for table in per_1000_tables:
        sql = "delete from {}".format(table)
        cur.execute(sql)
    if state is None:
        sql = "delete from regions"
        cur.execute(sql)
    conn.commit()

def main(argv):
    parser = argparse.ArgumentParser(
        prog="run_AP_counties.py",
        description="Run the AllPrograms to collect data for "
        "all states idenfified. If an optional second argument is given "
        "it is the state in the states-file that we want to start with, "
        "first deleting all current records for that state.",
    )
    parser.add_argument(
        "-b",
        "--database",
        required=True,
        help="The Sqlite3 database",
    )
    parser.add_argument(
        "-y",
        "--focus_year",
        required=True,
        help="The most recent year of complete data",
    )
    parser.add_argument(
        "-f",
        "--states_file",
        required=True,
        help="All the states to be processed",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", "--start_state",
                       required=False,
                       help="Clean the db for this state and start processing with this state")
    my_args = parser.parse_args()

    database = my_args.database
    target_year = my_args.focus_year

    command = ". ~/.profile"
    rslt = subprocess.call(command, shell=True)
    if rslt != 0:
        print(".profile not found")
        exit(-1)
    command = "cd ${EEW_HOME}"
    rslt = subprocess.call(command, shell=True)
    if rslt != 0:
        print("Unable to cd to $EEW_HOME")

    now = datetime.now()
    current_datetime = now.strftime("%d-%m-%Y-%H-%m-%s")
    move_if_exists("AllPrograms.log", current_datetime)
    move_if_exists("AllPrograms.error", current_datetime)
    # Read the states_file
    states_file = open(my_args.states_file)
    if my_args.start_state:
        # Clean the start_state, leave all other data in region_counties.db intact.
        # Restart the run with the start_state.
        print("start_state is {}".format(my_args.start_state))
        next_state = clean_to_start_state(database, states_file, my_args.start_state)
    else:
        # Copy the current region_counties.db, clean data and run all states.
        print("Saving the current region_counties.db")
        save_name = "{}-{}".format(database, current_datetime)
        command = "cp {} {}".format(database, save_name)
        rslt = subprocess.call(command,shell=True)
        if rslt == 0:
            clean_region_db(database)
            next_state = states_file.readline().rstrip()
        else:
            # A new region_counties.db will be needed.
            # The inflation and real_cds tables need to be populated.
            print("region_counties.db not found.")
            command = "sqlite3 {} < region_db.sql".format(database)
            rslt = subprocess.call(command, shell=True)
            if rslt != 0:
                print("ERROR: Unable to create {}".format(database))
                exit(-1)
            command = "sqlite3 {} < real_cds.sql".format(database)
            rslt = subprocess.call(command, shell=True)
            if rslt != 0:
                print("ERROR: Unable to load real_cds table")
            command = "sqlite3 {} < inflation.sql".format(database)
            rslt = subprocess.call(command, shell=True)
            if rslt != 0:
                print("ERROR: Unable to load inflation table")
            next_state = states_file.readline().rstrip()
    log_file = open("AllPrograms.log", "a")
    log_file.write(current_datetime)
    if next_state == "":
        print("End of {}. Exiting.".format(my_args.states_file))
        exit(0)
    print("Continuing with {}".format(next_state))

    while (True):
        command = "python3 AllPrograms.py -b {} -s {} -f {} -m County".format(database,
                                                                    next_state, 
                                                                    target_year)
        rslt = subprocess.call(command, shell=True)
        if rslt != 0:
            print("ERROR: AllPrograms failed on state: {}".format(next_state))
            exit(-1)
        next_state = states_file.readline().rstrip()
        if next_state == "":
            break

def usage():
    print("Usage:  run_AP_counties.py -b <database> -y <focus-year> -f <states_file> [-s <starting_state>]")
    exit


if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
    else:
        main(sys.argv[1])
