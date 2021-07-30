import json
import urllib.request
import sqlite3
import datetime
from os import path

conn = sqlite3.connect( 'leg_info.db' )
cursor = conn.cursor()

committees_url = 'https://theunitedstates.io/congress-legislators/committees-current.json'
members_url = 'https://theunitedstates.io/congress-legislators/committee-membership-current.json'

cmts = urllib.request.urlopen( committees_url ).read().decode()
obj = json.loads( cmts )

for cmt in obj:
    print( cmt['name'] )
    cmt_url = ''
    jurisdiction = ''
    try: cmt_url = cmt['url']
    except KeyError: pass
    try: jurisdiction = cmt['jurisdiction']
    except KeyError: pass
    cursor.execute( 
        'insert into committees ( committee_type, name, url, ' \
           'committee_id, jurisdiction ) values ( ?,?,?,?,? )',
        ( cmt['type'], cmt['name'], cmt_url, cmt['thomas_id'],
          jurisdiction ))
    try:
        scs = cmt['subcommittees']
        for sc in scs:
            cursor.execute( 
                'insert into sub_committees ( committee_id, name, ' \
                  'subcommittee_id ) values ( ?,?,? )',
                ( cmt['thomas_id'], sc['name'], sc['thomas_id'] ))
    except KeyError:
        pass
    conn.commit()

mbrs = urllib.request.urlopen( members_url ).read().decode()
obj = json.loads( mbrs )

for cmt, mbrs in obj.items():
    cmt_name = cmt[:4]
    sub_name = cmt[4:]
    for mbr in mbrs:
        print( mbr['name'] )
        start = ''
        try:
            start_date = datetime.datetime.strptime( mbr['start_date'], 
                            '%Y-%m-%d' )
            start = start_date.strftime( '%b %-d, %Y' )
        except KeyError:
            pass
        cursor.execute(
          'insert into committee_members ( name, bioguide_id, rank, ' \
             'start, committee_id, subcommittee_id ) ' \
             'values ( ?,?,?,?,?,? )', ( mbr['name'], mbr['bioguide'], 
             mbr['rank'], start, cmt_name, sub_name ))
    conn.commit()
