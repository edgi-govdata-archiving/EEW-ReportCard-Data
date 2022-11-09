import geopandas as gpd
import folium
import pandas as pd
from ECHO_modules.get_data import get_echo_data
import sqlite3
from git import Repo


def cd_to_block(state, cd):
    pass

def lookup(state):
    df = pd.read_csv("state-fips-codes.csv")
    df = df[df['State_Code'] == state]
    return str(df['FIPS_Code'].iloc[0]).zfill(2)

def gen_report(state, cd):
    """
    Parameter state: 2 letter str representation of a state
    Paramter cd: str representing the congressional district

    Returns: a dataframe
    """
    # INITIAL OUTLINE
    url = f"https://theunitedstates.io/districts/cds/2012/{state}-{str(cd)}/shape.geojson"
    cd_boundary = gpd.read_file(url)
    bounds = cd_boundary.bounds
    map = folium.Map(location=((bounds.miny+bounds.maxy)/2.,
                     (bounds.minx+bounds.maxx)/2.), zoom_level=4)
    folium.GeoJson(cd_boundary, name="Congressional Districts").add_to(map)

    ################################## TO DO ###################################
    select_columns = '"ID", "P_LDPNT_D2", "P_DSLPM_D2", "P_CANCR_D2", '
    select_columns += '"P_RESP_D2", "P_PTRAF_D2", "P_PWDIS_D2", '
    select_columns += '"P_PNPL_D2", "P_PRMP_D2", "P_PTSDF_D2", '
    select_columns += '"P_OZONE_D2", "P_PM25_D2"'

    sql = 'select {} from "EJSCREEN_2021_USPR" where DIV("ID", 10000000000) = {}'
    sql = sql.format(select_columns, lookup(state))

    ej_state_df = cd_to_block(state, cd)
    # ej_state_df = get_echo_data(sql)
	# Rename the ID field to match the field in the census data block group.
    # ej_state_df.rename(columns={'ID':'GEOID'}, inplace=True)
    # ej_state_df['GEOID'] = ej_state_df['GEOID'].astype(int)

    ############################################################################

    # GET CENSUS
    conn = sqlite3.connect("census-shapefiles/census2010.db")

    bg_point_list = []
    for index, row in ej_state_df.iterrows():
        # Use row['GEOID'] to look for the block group in the census db.
        sql = 'select GEOID, INTPTLAT, INTPTLON from census_block_groups where GEOID=\'{}\''.format(
            row['GEOID'])
        c = conn.cursor()
        c.execute(sql)
        block_group = c.fetchone()
        bg_point_list.append(block_group)
    conn.close()

    # EJ Screen Records
    bg_points_df = pd.DataFrame(bg_point_list, columns=[
                                'GEOID', 'INTPTLAT', 'INTPTLON'])
    bg_points_gdf = gpd.GeoDataFrame(bg_points_df, crs='epsg:4269',
                                     geometry=gpd.points_from_xy(bg_points_df.INTPTLON, bg_points_df.INTPTLAT))
    bg_points_gdf = bg_points_gdf.to_crs(4326)
    within_points = gpd.sjoin(bg_points_gdf, cd_boundary, op='within')

    # PLOT
    for i in range(0, len(within_points)):
        folium.Marker(
            location=[within_points.iloc[i]['INTPTLAT'],
                      within_points.iloc[i]['INTPTLON']],
            popup=within_points.iloc[i]['GEOID'],
        ).add_to(map)
        map

    # FILTER
    ej_columns = ['GEOID', 'P_LDPNT_D2', 'P_DSLPM_D2', 'P_CANCR_D2', 'P_RESP_D2', 'P_PTRAF_D2', 'P_PWDIS_D2',
                  'P_PNPL_D2', 'P_PRMP_D2', 'P_PTSDF_D2', 'P_OZONE_D2', 'P_PM25_D2']

    ej_cd_df = within_points.merge(ej_state_df[ej_columns], on='GEOID')

    # STATISTICS
    columns = ['P_LDPNT_D2', 'P_DSLPM_D2', 'P_CANCR_D2', 'P_RESP_D2', 'P_PTRAF_D2', 'P_PWDIS_D2',
               'P_PNPL_D2', 'P_PRMP_D2', 'P_PTSDF_D2', 'P_OZONE_D2', 'P_PM25_D2']
    ej_cd_df[columns] = ej_cd_df[columns].apply(pd.to_numeric)
    ej_cd_scores = ej_cd_df[columns]
    means = pd.DataFrame(ej_cd_scores.mean())
    new_columns = ['Lead paint', 'Diesel', 'Air toxics cancer', 'Air toxics resp',
                   'Traffic', 'Water discharge', 'NPL sites', 'RMP facilities',
                   'TSDF facilities', 'Ozone', 'PM2.5']
    means = means.set_axis(new_columns, axis=0)

    stdev = pd.DataFrame(ej_cd_scores.std())
    stdev = stdev.set_axis(new_columns, axis=0)
    ej_cd_scores.to_csv('ejscreen-{}{}-percentiles.csv'.format(state, cd))

    df2 = pd.melt(ej_cd_scores)
    df2['value2'] = pd.to_numeric(df2['value'], errors='ignore')
    df2['bins'] = pd.cut(df2['value2'], bins=[0, 5, 10, 15, 20, 25,
                         30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100])

    newdf = pd.DataFrame(columns=['metric', 'value', 'count'])
    for metric in columns:
        tempdf = df2[df2['variable'] == metric]
        for bin in tempdf['bins'].unique():
            try:
                midpoint = (bin.left + bin.right)/2
                tempdf2 = tempdf[tempdf['bins'] == bin]
                count = 2 * tempdf2['value'].count()
                new_row = {'metric': metric,
                           'value': midpoint, 'count': float(count)}
                # breakpoint()
                newdf = newdf.append(new_row, ignore_index=True)
            except AttributeError:
                continue
    return newdf

def clone_repo():
    Repo.clone_from("https://github.com/edgi-govdata-archiving/ECHO_modules", "edgi-govdata-archiving/ECHO_modules")
    
    
