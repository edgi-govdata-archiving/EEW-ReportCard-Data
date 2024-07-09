#!/usr/bin/env python
#coding: utf-8

import pdb
import sys, argparse
import subprocess
import os.path
import sqlite3
from datetime import datetime

_database = "test.db"
_target_year = 2023
def move_if_exists(filename, timestamp):
    new_name = "{}-{}".format(filename, timestamp)
    if os.path.exists(filename):
        command = "mv {} {}".format(filename, new_name)
        rslt = subprocess.call(command,shell=True)
        if rslt != 0:
            print("{} not found.".format(filename ))

def clean_to_start_state(states_file, start_state):
    # Skip down to start_state, where an error may have occurred.
    # Remove database entries for start_state, as they will be
    # re-calculated.
    this_state = states_file.readline().rstrip()
    while this_state != start_state and this_state != "":
        this_state = states_file.readline().rstrip()
    print("got to {}".format(this_state))
    if this_state != "":
        clean_region_db(this_state)
    return this_state

def clean_region_db(state = None):
    # Only clean the regions table if state is ''
    # *_per_1000 tables are always fully deleted as they are
    # constructed after all states by a separate program.
    conn = sqlite3.connect(_database)
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
        prog="run_AP_cds.py",
        description="Run the AllPrograms to collect data for "
        "all states idenfified. If an optional second argument is given "
        "it is the state in the states-file that we want to start with, "
        "first deleting all current records for that state.",
    )
    parser.add_argument(
        "-f",
        "--states_file",
        required=True,
        help="All the states to be processed",
    )
    parser.add_argument(
        "-b",
        "--database",
        required=True,
        help="The database to write data into",
    )
    parser.add_argument(
        "-y",
        "--target_year",
        required=True,
        help="The year of the report",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-s", "--start_state",
                       required=False,
                       help="Clean the db for this state and start processing with this state")
    my_args = parser.parse_args()

    _database = my_args.database
    _target_year = my_args.target_year
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
        # Clean the start_state, leave all other data in region_cds.db intact.
        # Restart the run with the start_state.
        print("start_state is {}".format(my_args.start_state))
        next_state = clean_to_start_state(states_file, my_args.start_state)
    else:
        # Copy the current region_cds.db, clean data and run all states.
        print("Saving the current {}".format(_database))
        save_name = "{}-{}".format(_database, current_datetime)
        command = "cp {} {}".format(_database, save_name)
        rslt = subprocess.call(command,shell=True)
        if rslt == 0:
            clean_region_db()
            next_state = states_file.readline().rstrip()
        else:
            # A new region_cds.db will be needed.
            # The inflation and real_cds tables need to be populated.
            print("{} not found.".format(_database))
            command = "sqlite3 {} < region_db.sql".format(_database)
            rslt = subprocess.call(command, shell=True)
            if rslt != 0:
                print("ERROR: Unable to create {}".format(_database))
                exit(-1)
            command = "sqlite3 {} < real_cds.sql".format(_database)
            rslt = subprocess.call(command, shell=True)
            if rslt != 0:
                print("ERROR: Unable to load real_cds table")
            command = "sqlite3 {} < inflation.sql".format(_database)
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
        command = "python3 AllPrograms.py -b {} -s {} -f {} -m \'{}\'".format(_database,
                                                                    next_state, 
                                                                    _target_year,
                                                                    "Congressional District")
        rslt = subprocess.call(command, shell=True)
        if rslt != 0:
            print("ERROR: AllPrograms failed on state: {}".format(next_state))
            exit(-1)
        next_state = states_file.readline().rstrip()
        if next_state == "":
            break

def usage():
    print("Usage:  run_AP_cds.py -f <states_file> [-s <starting_state>]")
    exit


if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
    else:
        main(sys.argv[1])