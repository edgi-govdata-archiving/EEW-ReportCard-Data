#!/usr/bin/env python
# coding: utf-8

import pdb
import sys, argparse
import sqlite3
from Region import Region
from csv import reader

def main(argv):

    parser = argparse.ArgumentParser(
        prog="check_AllPrograms.py",
        description="Check active facilities for each region against"
        " previous for gross discrepancies.",
    )
    parser.add_argument("-c", "--cds_file", required=True, help="The CDs to work with")
    my_args = parser.parse_args()

    found_errors = False

    cds_filename = my_args.cds_file
    state_cds = []
    with open(cds_filename, "r") as read_obj:
        csv_reader = reader(read_obj)
        raw_state_cds = list(map(tuple, csv_reader))
    state_cds = []
    for state, cd in raw_state_cds:
        cd = int(cd)
        state_cds.append((state, cd))

    sql_current = 'select * from active_facilities where region_id={}'
    sql_prev = 'select * from active_facilities_previous where region_id={}'
    conn = sqlite3.connect( "region.db" )
    cursor = conn.cursor()

    # if abs(current-previous)/previous > threshold
    # declare a problem.

    threshold = 0.4

    problem_text = '{} - {} - {} - {} '

    programs = ('CWA','CAA','RCRA')
    for state, cd in state_cds:
        if cd is None:
            type = 'State'
        else:
            type = 'Congressional District'
        region = Region( type=type, value=cd, state=state )
        for program in programs:
            current = region.get_active_facilities( program )
            previous = region.get_active_facilities( program, 
                    'active_facilities_previous' )
            if previous != 0:
                if abs( current - previous ) / previous > threshold:
                    print( problem_text.format( state, cd, program, current ))
                    found_errors = True
    print ( "Found errors? {}".format(found_errors))
    sys.exit(found_errors)

def usage():
    print("Usage:  check_AllPrograms.py -c cds_todo.csv")
    exit


if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
    else:
        main(sys.argv[1])
