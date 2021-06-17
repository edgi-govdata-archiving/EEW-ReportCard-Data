import pdb
import os
import pandas as pd
import sqlite3
from AllPrograms_db import get_region_rowid


class Region:
    '''
    This class represents the data associated with a particular
    region--a state, congressional district, watershed, etc.

    Attributes
    ----------
    type : str
        One of the supported region types--'State', 
        'Congressional District', 'Watershed', 'Zip Code'
    value : str
        The actual identifier of the region--e.g. the number of
        the congressional district, the watershed name
    state : str
        The two letter state abbreviation
    db_conn : SQLite connection
        The connection to the local SQLite database holding
        the region's data
    data_sets : list
        The DataSet objects for the region 
    '''

    def __init__(self, type, value=None, state=None):

        self.type = type  # Region type
        self.value = value  # Region instance
        self.state = state  # State

        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        self.region_id = get_region_rowid( cursor, self.state, self.value )

    '''
        CREATE TABLE per_fac (
        region_id tinyint,
        program char(20),
        type char(20),
        year tinyint,
        count real,
        unique(region_id,program,type,year));
    '''
    def get_usa_per_1000( self, type, year ):
        # type is 'inspections' or 'violations'
        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        sql = 'select program, sum(count) from active_facilities '
        sql += ' group by program'
        sql = sql.format( year )
        df_fac = pd.read_sql_query( sql,conn )

        sql = 'select program, sum(count) from {} where '
        sql += ' year={} group by program'
        sql = sql.format( type, year )
        df_insp = pd.read_sql_query( sql, conn )

        # df_merged = pd.merge( df_fac, df_insp )
        df_joined = df_fac.join( 
            df_insp.set_index(['program']),
            lsuffix='x',
            rsuffix='y',
            on=['program'])
        type_cap = type.capitalize()
        df_joined.columns = ['Program', 'Facilities', type_cap ]
        df_joined['Per1000'] = 1000. * df_joined[type_cap] / df_joined['Facilities']
        return df_joined

    def get_cwa_per_1000( self, year ):
        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        '''
        real_regions contains all the legitimate CDs for the states
        regions includes all identified in ECHO_EXPORTER, which
        include many CDs that do not really exist.
        real_regions identifies single-cd states by having a cd
        value of 0.
        '''
        sql = 'select state, cd from real_regions'
        df_real = pd.read_sql_query( sql, conn )
        # Results will be dictionary with key=AL01, state/cd,
        # and values a tuple (Num per 1000, Region) where
        # Region is 'Congressional District' or 'State'
        results = {}
        for idx, row in df_real.iterrows():
            cd = row['cd']
            state = row['state']
            key = '{}{}'.format(state, str(cd).zfill(2))
            if ( cd == 0 ):
                # This is a single-cd state.
                # Include all identified cds for the state.
                sql = 'select rowid from regions where state=\'{}\''
                sql = sql.format( state )
                cursor.execute( sql )
                region_ids = cursor.fetchall()
                active = 0
                violations = 0
                for region_id in region_ids:
                    sql = 'select sum(count) from active_facilities'
                    sql += ' where program=\'CWA\' and region_id={}'
                    sql = sql.format( region_id[0] )
                    cursor.execute( sql )
                    fetch = cursor.fetchone()
                    active += fetch[0] if fetch[0] is not None else 0
                    sql = 'select sum(count) from violations where'
                    sql += ' program=\'CWA\' and region_id={} and year={}'
                    sql = sql.format( region_id[0], year )
                    cursor.execute( sql )
                    fetch = cursor.fetchone()
                    violations += fetch[0] if fetch[0] is not None else 0
                per_1000 = 0 if active == 0 else 1000. * violations / active
                results[ key ] = ( per_1000, 'Congressional District' ) 
            else:
                # Get the results for just this single state/cd
                region_id = get_region_rowid( cursor, state, str(cd).zfill(2) )
                sql = 'select count from active_facilities'
                sql += ' where program=\'CWA\' and region_id={}'
                sql = sql.format( region_id )
                cursor.execute( sql )
                fetch = cursor.fetchone()
                active = fetch[0] if fetch[0] is not None else 0
                sql = 'select count from violations where'
                sql += ' program=\'CWA\' and region_id={} and year={}'
                sql = sql.format( region_id, year )
                cursor.execute( sql )
                fetch = cursor.fetchone()
                violations = fetch[0] if fetch and fetch[0] is not None else 0
                per_1000 = 0 if active == 0 else 1000. * violations / active
                results[ key ] = ( per_1000, 'Congressional District' ) 
        # Repeat this for all states
        sql = 'select distinct(state) from real_regions'
        df_real = pd.read_sql_query( sql, conn )
        for idx, row in df_real.iterrows():
            state = row['state']
            # Include all identified cds for the state.
            sql = 'select rowid from regions where state=\'{}\''
            sql = sql.format( state )
            cursor.execute( sql )
            region_ids = cursor.fetchall()
            active = 0
            violations = 0
            for region_id in region_ids:
                sql = 'select sum(count) from active_facilities'
                sql += ' where program=\'CWA\' and region_id={}'
                sql = sql.format( region_id[0] )
                cursor.execute( sql )
                fetch = cursor.fetchone()
                active += fetch[0] if fetch[0] is not None else 0
                sql = 'select sum(count) from violations where'
                sql += ' program=\'CWA\' and region_id={} and year={}'
                sql = sql.format( region_id[0], year )
                cursor.execute( sql )
                fetch = cursor.fetchone()
                violations += fetch[0] if fetch[0] is not None else 0
            per_1000 = 0 if active == 0 else 1000. * violations / active
            results[ state ] = ( per_1000, 'State' )
        return results

    def get_inflation( self, base_year ):
        # base_year is the year for which a dollar is a dollar
        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        sql = 'select year, rate from inflation order by year desc'
        df_fac = pd.read_sql_query( sql,conn )

        inflation_by_year = {}
        calculated_inflation = 1.0
        for idx, row in df_fac.iterrows():
            # pdb.set_trace()
            if row['year'] > base_year:
                continue
            inflation_by_year[row['year']] = calculated_inflation
            if row['year'] <= base_year:
                calculated_inflation *= 1.0 + .01 * row['rate']
        return inflation_by_year

    def get_active_facilities( self ):
        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        sql = 'select program, count from active_facilities where region_id={}'
        sql = sql.format( self.region_id )
        return pd.read_sql_query( sql,conn )

    def get_cwa_violations( self, base_year ):
        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        sql = 'select year, count from violations where region_id={}'
        sql += ' and program=\'CWA\' and year <= {}'
        sql = sql.format( self.region_id, base_year )
        return pd.read_sql_query( sql,conn )

    def get_enforcements( self, base_year ):
        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        sql = 'select year, sum(amount), sum(count) from enforcements'
        sql += ' where region_id={} and year <= {} group by year'
        sql = sql.format( self.region_id, base_year )
        return pd.read_sql_query( sql,conn )


    '''
        # How to do totals for enforcements, violations, inspections.

        if df_caa is not None or df_cwa is not None or df_rcra is not None:
            df_totals = pd.concat([df_caa, df_cwa, df_rcra])
            df_totals = df_totals.groupby(df_totals.index).agg("sum")
            AllPrograms_db.write_total_enforcements("All", df_totals, ds_type)
            print("Total Penalties for {} district {}".format(state, cd))
    '''
