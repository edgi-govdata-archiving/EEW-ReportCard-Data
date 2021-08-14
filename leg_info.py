import pdb

import datetime
import json
import urllib
import sqlite3
import wget
from os import path

def get_since_fields( the_date ):
    mon = the_date.strftime( "%B" )
    day = the_date.strftime( "%-d" )
    year = the_date.strftime( "%Y" )
    suffix = 'th'
    if ( day == '1' or day == '21' or day == '31' ):
        suffix = 'st'
    elif ( day == '2' or day == '22' ):
        suffix = 'nd'
    elif ( day == '3' or day == '23' ):
        suffix = 'rd'
    return '{} {}{}, {}'.format( mon, day, suffix, year ), year

conn = sqlite3.connect( 'leg_info.db' )
cursor = conn.cursor()

legs_url = 'https://theunitedstates.io/congress-legislators/legislators-current.json'

legs = urllib.request.urlopen( legs_url ).read().decode()
obj = json.loads( legs )

govtrack_base = "https://govtrack.us/congress/members/"
wiki_base = "https://en.wikipedia.org/wiki/"

image_url = "https://govtrack.us/static/legislator-photos/{}-200px.jpeg"

for leg in obj:
    id = leg['id']
    bioguide_id = id['bioguide']
    govtrack_id = id['govtrack']
    first_name = leg['name']['first']
    last_name = leg['name']['last']
    full_name = ''
    try: full_name = leg['name']['official_full']
    except KeyError: 
        full_name = '{} {}'.format( first_name, last_name )
    govtrack_url = '{}{}_{}/{}'.format( govtrack_base, first_name.lower(),
       last_name.lower(), govtrack_id )
    wikipedia_url = ''
    try: wikipedia_url = '{}{}'.format( wiki_base, id['wikipedia'].replace(' ','_'))
    except KeyError: 
        print( 'No wikipedia: {}'.format( full_name ))
        pass
    terms = leg['terms']
    start_date = datetime.date.today()
    party = ''
    sen_rep = ''
    state = ''
    district = ''
    sen_class = ''
    official_url = ''
    image_file = ''
    for term in terms:
        if ( term['type'] != sen_rep or 
              ( term['type'] == 'rep' and term['district'] != district )):
            # Will be true the first time through, and with change rep <--> sen
            sen_rep = term['type']
            start_date = datetime.datetime.strptime( term['start'], '%Y-%m-%d' )
            party = term['party']
            state = term['state']
            if ( sen_rep == 'rep' ):
                district = term['district']
                image_file = '{}{}_rep.jpeg'.format( state, 
                                        str( district ).zfill(2))
            else:
                sen_class = term['class']
                district = ''
                image_file = '{}_sen{}.jpeg'.format( state,
                                              sen_class )
        else:
            this_date = datetime.datetime.strptime( term['start'], '%Y-%m-%d' )
            if ( this_date < start_date ):
                start_date = this_date
            elif ( party != term['party'] ):
                # They changed party after they became sen or rep
                party = term['party']
        try: official_url = term['url']
        except KeyError: pass
    ( since_date, since_year ) = get_since_fields( start_date )
 
    if district == '':
        cd_state = state
    else:
        cd_state = state + str( district ).zfill(2)
    cursor.execute( 
        'insert into legislators ( cd_state, name, party, govtrack_id, ' \
           'bioguide_id, sen_class, since_date, since_year, ' \
           'official_url, govtrack_url, wikipedia_url ) ' \
           'values ( ?,?,?,?,?,?,?,?,?,?,? )',
           ( cd_state, full_name, party, govtrack_id, bioguide_id,
             sen_class, since_date, since_year, official_url, govtrack_url,
             wikipedia_url ))
    conn.commit()

    # pdb.set_trace()
    if image_file != '':
        try:
            this_image_url = image_url.format( govtrack_id )
            wget.download( this_image_url, 'CD_images/{}'.format( 
                                image_file ))
        except urllib.error.HTTPError:
            print( "Photo for {} not available.".format( cd_state ))


