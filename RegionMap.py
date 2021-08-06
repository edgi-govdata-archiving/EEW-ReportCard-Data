import pdb

import pandas as pd
import sys, argparse
import folium
from folium.plugins import FastMarkerCluster
import geopandas
from csv import reader
import time
from selenium import webdriver

driver = webdriver.Chrome()
driver.set_window_size( 558, 620 )


def mapper(df, no_text=False, map_radius=1, map_opacity=.1):
    '''
    Display a map of the Dataframe passed in.
    Based on https://medium.com/@bobhaffner/folium-markerclusters-and-fastmarkerclusters-1e03b01cb7b1

    Parameters
    ----------
    df : Dataframe
        The facilities to map.  They must have a FAC_LAT and FAC_LONG field.
    bounds : Dataframe
        A bounding rectangle--minx, miny, maxx, maxy.  Discard points outside.

    Returns
    -------
    folium.Map
    '''

    # Initialize the map
    m = folium.Map(
        location = [df["FAC_LAT"].mean(), df["FAC_LONG"].mean()]
    )

    # Create the Marker Cluster array
    #kwargs={"disableClusteringAtZoom": 10, "showCoverageOnHover": False}
    mc = FastMarkerCluster("")
 
    # Add a clickable marker for each facility
    for index, row in df.iterrows():
        # pdb.set_trace()
        mc.add_child(folium.CircleMarker(
            location = [row["FAC_LAT"], row["FAC_LONG"]],
            popup = row["NAME"],
            radius = map_radius,
            color = "black",
            weight = 1,
            fill_color = "orange",
            fill_opacity= map_opacity
        ))
    
    m.add_child(mc)
    bounds = m.get_bounds()
    m.fit_bounds(bounds)

    # Show the map
    return m


def usage():
    print( 'Usage: RegionMap.py -c maps_todo.csv' )
    exit


def main( argv ):

    parser = argparse.ArgumentParser(
        prog='RegionMap.py',
        description='Create PNG maps for the cds and states identified in the CSV file.'
    )
    parser.add_argument( "-c", "--csv_file", required=True, help="The CDS and states to map.")
    my_args = parser.parse_args()

    state_cds = []
    with open( my_args.csv_file, 'r' ) as read_obj:
        csv_reader = reader( read_obj )
        raw_state_cds = list( map( tuple, csv_reader ))
    state_cds = []
    for state, cd in raw_state_cds:
        if ( cd == '0' ):
            cd = None
        else:
            cd = int( cd )
        state_cds.append((state,cd))
    # state_cds.extend( [('TX',None),] )
    # state_cds.extend( [('TX',34),] )
    
    for state, cd in state_cds:
        print( 'Map for {} CD {}'.format( state, cd ))
        
        if ( cd is None ):
            # There are too many facilities in most states to successfully plot on a map.
            url = "https://github.com/edgi-govdata-archiving/ECHO-Geo/raw/main/states.geojson"
            filename = '{}_map'.format( state )
        else:
            url = "https://raw.githubusercontent.com/unitedstates/districts/gh-pages/cds/2012/{}-{}/shape.geojson".format( state, str(cd))       
            # f_map.fit_bounds( [[bounds.minx,bounds.miny],[bounds.maxx,bounds.maxy]] )
            filename = '{}{}_map'.format( state, str(cd).zfill(2) )
        map_boundary = geopandas.read_file( url )
        if ( cd is None ):
            map_boundary = map_boundary[ map_boundary['STUSPS'] == state ]
        bounds = map_boundary.bounds
        map_df_data = {'FAC_LAT':[bounds.miny,bounds.miny,bounds.maxy,bounds.maxy],
                       'FAC_LONG':[bounds.minx,bounds.maxx,bounds.minx,bounds.maxx],
                       'NAME':['SW','SE','NW','NE']}
        map_df = pd.DataFrame( map_df_data, columns=['FAC_LAT', 'FAC_LONG', 'NAME'])
        f_map = mapper(df=map_df, no_text=True)
        w = folium.GeoJson( map_boundary, name = "Region Map", ).add_to( f_map ) 
        f_map.save( '/var/www/html/EDGI/{}.html'.format( filename ))
    
        driver.get( 'http://localhost/EDGI/{}.html'.format( filename ))
        time.sleep( 6 )
        driver.save_screenshot( 'Output/CD_maps/{}.png'.format( filename ))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
    else:
        main(sys.argv[1])
