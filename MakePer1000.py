#!/usr/bin/env python
# coding: utf-8

import AllPrograms_db
import sys, argparse

def main(argv):
    parser = argparse.ArgumentParser(
        prog="MakePer1000.py",
        description="Build summary data tables state_per_1000 and "
                    "either cd_per_1000 or county_per_1000 "
                    "for the five years ending with the focus year.",
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
        "-m",
        "--region_mode",
        required=True,
        help="County or Congressional District",
    )
    my_args = parser.parse_args()

    _database = my_args.database
    _region_mode = my_args.region_mode
    _state = my_args.focus_state
  
    _focus_year = my_args.focus_year

    AllPrograms_db.make_per_1000(_database, _region_mode, _focus_year)

def usage():
    print("Usage:  MakePer1000.py -b <database> -m <region_mode> -f <focus_year> ")
    exit


if __name__ == "__main__":
    if len(sys.argv) < 4:
        usage()
    else:
        main(sys.argv[1])
