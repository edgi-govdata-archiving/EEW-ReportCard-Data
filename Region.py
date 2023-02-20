import pdb
import os
import pandas as pd
import sqlite3
from AllPrograms_util import get_region_rowid

class Region:
    '''
    This class represents the data associated with a particular
    region--a state, congressional district, watershed, etc.

    Attributes
    ----------
    type : str
        One of the supported region types--'Nation', 'State', 
        'Congressional District', 'Watershed', 'Zip Code'
    value : str
        The actual identifier of the region--e.g. the number of
        the congressional district, the watershed name
    state : str
        The two letter state abbreviation
    programs : str
        The EPA programs
    db_conn : SQLite connection
        The connection to the local SQLite database holding
        the region's data
    data_sets : list
        The DataSet objects for the region 
    '''
    
    def __init__(self, type, value=None, state=None, programs=None):

        self.type = type  # Region type
        self.value = value  # Region instance
        self.state = state  # State
        self.programs = programs # The EPA programs to include

        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        self.region_id = get_region_rowid( cursor, self.state, self.value )
        conn.close()

    '''
        CREATE TABLE per_fac (
        region_id tinyint,
        program char(20),
        type char(20),
        year tinyint,
        count real,
        unique(region_id,program,type,year));
    '''

    def get_cds( self ):
        conn = sqlite3.connect("region.db")
        sql = 'select state, cd from real_cds order by state, cd'
        df = pd.read_sql_query( sql,conn )
        return df

    def get_per_1000( self, type, region, year ):
        # type is 'inspections' or 'violations'
        # region is 'USA', 'State', 'CD'
        # programs is a list of the programs to be included--CAA, CWA, etc.
        if ( region == 'USA' or region == 'State' ):
            return self._get_region_per_1000( type, region, year )
        # For CDs we can just use the per_fac table and
        # active_facilities for the region
        conn = sqlite3.connect("region.db")

        sql = 'select program as Program, 1000. * count as Per1000 from per_fac'
        sql += ' where region_id={} and type=\'{}\' and year={}'
        if ( self.programs is not None ):
            sql += ' and program in (\'{}\')'
            sql = sql.format( self.region_id, type, year, '\',\''.join( self.programs ))
        else:
            sql = sql.format( self.region_id, type, year )
        df = pd.read_sql_query( sql,conn )
        return df

    def _get_region_per_1000( self, type, region, year ):
        # type is 'inspections' or 'violations'
        # region is 'USA', 'State', 'CD'
        conn = sqlite3.connect("region.db")

        sql = 'select program, sum(count) from active_facilities '
        if ( self.programs is not None ):
            sql += ' where program in (\'{}\') '
            sql = sql.format( '\',\''.join(self.programs))
        if ( region == 'State' ):
            sql += ' and region_id in ( select rowid from regions '
            sql += ' where state=\'{}\' )'
            sql = sql.format( self.state )
        sql += ' group by program'
        df_fac = pd.read_sql_query( sql, conn )

        sql = 'select program, sum(count) from {} where year={}'
        if ( region == 'State' ):
            sql += ' and region_id in ( select rowid from regions '
            sql += ' where state=\'{}\' )'
            sql = sql.format( type, year, self.state )
        else:
            sql = sql.format( type, year )
        if ( self.programs is not None ):
            sql += ' and program in (\'{}\')'
            sql = sql.format( '\',\''.join(self.programs) )
        sql += ' group by program'
        df_insp = pd.read_sql_query( sql, conn )

        df_joined = df_fac.join( 
            df_insp.set_index(['program']),
            lsuffix='x',
            rsuffix='y',
            on=['program'])
        type_cap = type.capitalize()
        df_joined.columns = ['Program', 'Facilities', type_cap ]
        df_joined['Per1000'] = 1000. * df_joined[type_cap] / df_joined['Facilities']
        df = df_joined.drop( ['Facilities', type_cap], axis='columns' )
        return df

    def get_all_per_1000( self, year ):
        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        sql = 'select state, cd from real_cds'
        df_real = pd.read_sql_query( sql, conn )
        # Results will be dictionary with key=AL01, state/cd,
        # and values a tuple (Num per 1000, type, Region) where
        # type is 'inspections', 'violations', or 'enforcements' and
        # Region is 'Congressional District' or 'State'
        results = {}
        for idx, row in df_real.iterrows():
            cd = row['cd']
            state = row['state']
            key = '{}{}'.format(state, str(cd).zfill(2))
            num_events = {}
            for program in ['CAA','CWA','RCRA']:
                for type in ['inspections','violations','enforcements']:
                    num_events[(program,type)] = 0
            if ( cd == 0 ):
                # This is a single-cd state.
                # Include all identified cds for the state.
                sql = 'select rowid from regions where state=\'{}\''
                sql = sql.format( state )
                cursor.execute( sql )
                region_ids = cursor.fetchall()
                active = 0
                for program in ['CAA','CWA','RCRA']:
                    for region_id in region_ids:
                        sql = 'select sum(count) from active_facilities'
                        sql += ' where program=\'{}\' and region_id={}'
                        sql = sql.format( program, region_id[0] )
                        cursor.execute( sql )
                        fetch = cursor.fetchone()
                        if fetch:
                            active += fetch[0] if fetch[0] else 0
                        for type in ['inspections','violations','enforcements']:
                            sql = 'select sum(count) from {} where'
                            sql += ' program=\'{}\' and region_id={} and year={}'
                            sql = sql.format( type, program, region_id[0], year )
                            cursor.execute( sql )
                            fetch = cursor.fetchone()
                            if fetch:
                                num_events[(program,type)] += fetch[0] \
                                    if fetch[0] else 0
                    for type in ['inspections','violations','enforcements']:
                        num_events[(program,type)] = 0 if active == 0 \
                                    else 1000. * num_events[(program,type)] / active
                results[ key ] = ( num_events[('CAA','inspections')],
                     num_events[('CAA','violations')], 
                     num_events[('CAA','enforcements')], 
                     num_events[('CWA','inspections')], 
                     num_events[('CWA','violations')], 
                     num_events[('CWA','enforcements')], 
                     num_events[('RCRA','inspections')], 
                     num_events[('RCRA','violations')], 
                     num_events[('RCRA','enforcements')], 
                     'Congressional District' ) 
            else:
                # Get the results for just this single state/cd
                region_id = get_region_rowid( cursor, state, str(cd).zfill(2) )
                for program in ['CAA','CWA','RCRA']:
                    sql = 'select count from active_facilities'
                    sql += ' where program=\'{}\' and region_id={}'
                    sql = sql.format( program, region_id )
                    cursor.execute( sql )
                    fetch = cursor.fetchone()
                    active = 0
                    if fetch:
                        active = fetch[0]
                    for type in ['inspections','violations','enforcements']:
                        sql = 'select count from {} where'
                        sql += ' program=\'{}\' and region_id={} and year={}'
                        sql = sql.format( type, program, region_id, year )
                        cursor.execute( sql )
                        fetch = cursor.fetchone()
                        if fetch:
                            num_events[(program,type)] += fetch[0] if fetch[0] else 0
                    for type in ['inspections','violations','enforcements']:
                        num_events[(program,type)] = 0 if active == 0 \
                               else 1000. * num_events[(program,type)] / active
                results[ key ] = ( num_events[('CAA','inspections')],
                     num_events[('CAA','violations')], 
                     num_events[('CAA','enforcements')], 
                     num_events[('CWA','inspections')], 
                     num_events[('CWA','violations')], 
                     num_events[('CWA','enforcements')], 
                     num_events[('RCRA','inspections')], 
                     num_events[('RCRA','violations')], 
                     num_events[('RCRA','enforcements')], 
                     'Congressional District' ) 
        # Repeat this for all states
        sql = 'select distinct(state) from regions'
        df_real = pd.read_sql_query( sql, conn )
        for idx, row in df_real.iterrows():
            state = row['state']
            # Include all identified cds for the state.
            sql = 'select rowid from regions where state=\'{}\''
            sql = sql.format( state )
            cursor.execute( sql )
            region_ids = cursor.fetchall()
            active = 0
            num_events = {}
            for program in ['CAA','CWA','RCRA']:
                for type in ['inspections','violations','enforcements']:
                    num_events[(program,type)] = 0
            for program in ['CAA','CWA','RCRA']:
                for region_id in region_ids:
                    sql = 'select sum(count) from active_facilities'
                    sql += ' where program=\'{}\' and region_id={}'
                    sql = sql.format( program, region_id[0] )
                    cursor.execute( sql )
                    fetch = cursor.fetchone()
                    active += fetch[0] if fetch[0] is not None else 0
                    for type in ['inspections','violations','enforcements']:
                        sql = 'select sum(count) from {} where'
                        sql += ' program=\'{}\' and region_id={} and year={}'
                        sql = sql.format( type, program, region_id[0], year )
                        cursor.execute( sql )
                        fetch = cursor.fetchone()
                        num_events[(program,type)] += fetch[0] \
                                if fetch[0] is not None else 0
                for type in ['inspections','violations','enforcements']:
                    num_events[(program,type)] = 0 if active == 0 \
                                else 1000. * num_events[(program,type)] / active
            results[ state ] = ( num_events[('CAA','inspections')],
                     num_events[('CAA','violations')], 
                     num_events[('CAA','enforcements')], 
                     num_events[('CWA','inspections')], 
                     num_events[('CWA','violations')], 
                     num_events[('CWA','enforcements')], 
                     num_events[('RCRA','inspections')], 
                     num_events[('RCRA','violations')], 
                     num_events[('RCRA','enforcements')], 
                     'State' )
        conn.close()
        df = pd.DataFrame.from_dict( results, orient='index', 
                 columns=['CAA.Insp.per.1000','CAA.Viol.per.1000','CAA.Enf.per.1000', 
                          'CWA.Insp.per.1000','CWA.Viol.per.1000','CWA.Enf.per.1000', 
                          'RCRA.Insp.per.1000','RCRA.Viol.per.1000','RCRA.Enf.per.1000', 
                          'Region'])
        df.reset_index( inplace=True )
        df = df.rename( columns={ 'index':'CD.State'})
        return df

    def get_recurring_violations( self, program ):
        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        sql = 'select sum(count) from active_facilities where '
        if ( self.state is None ):
            sql += ' program=\'{}\''
            sql = sql.format( program )
        else:
            sql += ' program=\'{}\' and region_id in ( select rowid from regions'
            sql += ' where state=\'{}\' )'
            sql = sql.format( program, self.state )
        cursor.execute( sql )
        fetched = cursor.fetchone()
        state_facilities = fetched[0]
        
        sql = 'select sum(violations) from recurring_violations where '
        if ( self.state is None ):
            sql += ' program=\'{}\''
            sql = sql.format( program )
        else:
            sql += ' program=\'{}\' and region_id in ( select rowid from regions'
            sql += ' where state=\'{}\' )'
            sql = sql.format( program, self.state )
        cursor.execute( sql )
        fetched = cursor.fetchone()
        state_violators = fetched[0]
        data = [{ 'State': self.state, 'Facilities': state_violators,
                    'Percent': 100. * state_violators / state_facilities 
                    if state_facilities > 0 else -1 },
                ]

        if ( self.value is not None ):       
            sql = 'select violations, facilities from recurring_violations '
            sql += ' where program=\'{}\' and region_id={}'
            sql = sql.format( program, self.region_id )
            cursor.execute( sql )
            cd_fac_viol = cursor.fetchone()

            if ( cd_fac_viol is not None ):
                data.append(
                    { 'CD': '{}{}'.format( self.state, self.value ),
                        'Facilities': cd_fac_viol[0],
                    '    Percent': 100. * cd_fac_viol[0] / cd_fac_viol[1] 
                        if cd_fac_viol[1] > 0 else -1 
                    }
                )

        df = pd.DataFrame( data )
        return df

    def get_inflation( self, base_year ):
        # base_year is the year for which a dollar is a dollar
        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        sql = 'select year, rate from inflation order by year desc'
        df_fac = pd.read_sql_query( sql,conn )

        inflation_by_year = {}
        calculated_inflation = 1.0
        for idx, row in df_fac.iterrows():
            if row['year'] > base_year:
                continue
            inflation_by_year[int(row['year'])] = calculated_inflation
            if row['year'] <= base_year:
                calculated_inflation *= 1.0 + .01 * row['rate']
        df = pd.DataFrame.from_dict( inflation_by_year, orient='index' )
        df = df.sort_index()
        df.reset_index( inplace=True )
        # df = df.reindex()
        # df.columns = ['Year', 'rate']
        # df = df.rename( columns=['Year', 'rate'])
        return df

    def get_events( self, type, program, base_year ):
        conn = sqlite3.connect("region.db")

        if ( type == 'inspections' ):
            sql = 'select year Year, sum(count) Count from inspections'
        elif ( type == 'enforcements' ):
            sql = 'select year Year, sum(amount) Amount, sum(count) Count '
            sql += ' from enforcements'
        elif ( type == 'violations' ):
            sql = 'select year Year, sum(count) as Count from violations'
        else:
            return None
        if ( self.value is None ):
            # If state is None, get the count
            # for all regions in the USA
            if ( self.state is None ):
                sql += ' where '
            else:
                sql += ' where region_id in ( select rowid from regions where '
                sql += ' state=\'{}\') and ' 
                sql = sql.format( self.state )
        else:
            sql += ' where region_id={} and '
            sql = sql.format( self.region_id )
        sql += ' year <= {} '
        if ( program != 'All' ):
            sql += ' and program=\'{}\''
        sql += ' group by year'
        if ( program == 'All' ):
            sql = sql.format( base_year )
        else:
            sql = sql.format( base_year, program )
        return pd.read_sql_query( sql,conn )

    def get_non_compliants( self, program ):
        conn = sqlite3.connect("region.db")

        sql = 'select fac_name, noncomp_count, formal_action_count, dfr_url,'
        sql += ' fac_lat, fac_long from non_compliants where program=\'{}\''
        sql += ' and noncomp_count > 0'
        sql = sql.format( program )
        if ( self.value is None ):
            if ( self.state is not None ):
                # Look at the entire state
                sql += ' and region_id in (select rowid from regions where state=\'{}\')'
                sql = sql.format( self.state )
        else:
            sql += ' and region_id={}'
            sql = sql.format( self.region_id )
        return pd.read_sql_query( sql, conn ).sort_values(by='noncomp_count',
                    ascending=False)

    def get_active_facilities( self, program, table='active_facilities' ):
        conn = sqlite3.connect("region.db")
        cursor = conn.cursor()

        if ( self.value is None ):
            if ( self.state is None ):
                # Sum active facilities over all regions in the country
                sql = 'select sum(count) from {} where program=\'{}\''
                sql = sql.format( table, program )
            else:
                # Sum active facilities over all regions in the state
                sql = 'select sum(count) from {} where region_id in ('
                sql += ' select rowid from regions where state=\'{}\') and program=\'{}\''
                sql = sql.format( table, self.state, program )
        else:
            sql = 'select count from {} where region_id={}'
            sql += ' and program=\'{}\''
            sql = sql.format( table, self.region_id, program )
        cursor.execute( sql )
        fetch = cursor.fetchone()
        return fetch[0] if fetch else 0

    def get_ranked(self):
        conn = sqlite3.connect("region.db")

        state_columns = 'CAA_Insp_Rank, CAA_Viol_Rank, CAA_Enf_Rank, '
        state_columns += 'CWA_Insp_Rank, CWA_Viol_Rank, CWA_Enf_Rank, '
        state_columns += 'RCRA_Insp_Rank, RCRA_Viol_Rank, RCRA_Enf_Rank'
        cd_columns = 'CAA_Insp_Pct, CAA_Viol_Pct, CAA_Enf_Pct, '
        cd_columns += 'CWA_Insp_Pct, CWA_Viol_Pct, CWA_Enf_Pct, '
        cd_columns += 'RCRA_Insp_Pct, RCRA_Viol_Pct, RCRA_Enf_Pct'

        sql = ''
        if (self.type == 'State'):
            sql = 'select {} from state_per_1000 where "CD.State" = \'{}\''
            sql = sql.format(state_columns, self.state)
        if (self.type == 'Congressional District'):
            sql = 'select {} from cd_per_1000 where "CD.State" = \'{}{}\''
            sql = sql.format(cd_columns, self.state, self.value)

        return pd.read_sql_query( sql, conn )
