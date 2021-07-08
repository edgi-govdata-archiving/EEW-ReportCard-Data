import pdb
from Region import Region
import pandas as pd

programs = ['CAA', 'CWA', 'RCRA']
region = Region( type='Congressional District', value='34', state='TX',
                programs=programs )

inflation = region.get_inflation( 2019 )
print( "Inflation" )
print( inflation )


df = region.get_per_1000( 'inspections', 'USA', 2020 )
print( "USA inspections" )
print(df)
df = region.get_per_1000( 'violations', 'USA', 2020 )
print( "USA violations" )
print(df)
cwa_per_1000 = region.get_cwa_per_1000( 2020 )
print( "CWA violations per 1000 facilities" )
print( cwa_per_1000 )

print( 'CAA active facilities: {}'.format( region.get_active_facilities('CAA')))
print( 'CWA active facilities: {}'.format( region.get_active_facilities('CWA')))
print( 'RCRA active facilities: {}'.format( region.get_active_facilities('RCRA')))
print( 'GHG active facilities: {}'.format( region.get_active_facilities('GHG')))

'''
CAArecurring = region.get_recurring_violations( 'CAA' )
print( CAArecurring )
CWArecurring = region.get_recurring_violations( 'CWA' )
print( CWArecurring )
RCRArecurring = region.get_recurring_violations( 'RCRA' )
print( RCRArecurring )

violations = region.get_events( 'violations', 'All', 2020 )
print( "Violations" )
print( violations )
CAAviolations = region.get_events( 'violations', 'CAA', 2020 )
print( CAAviolations )
CWAviolations = region.get_events( 'violations', 'CWA', 2020 )
print( CWAviolations )
RCRAviolations = region.get_events( 'violations', 'RCRA', 2020 )
print( RCRAviolations )
enforcement = region.get_events( 'enforcements', 'All', 2020 )
print( "Enforcements" )
print( enforcement )
CAAenforcement = region.get_events( 'enforcements', 'CAA', 2020 )
print( CAAenforcement )
CWAenforcement = region.get_events( 'enforcements', 'CWA', 2020 )
print( CWAenforcement )
RCRAenforcement = region.get_events( 'enforcements', 'RCRA', 2020 )
print( RCRAenforcement )
inspections = region.get_events( 'inspections', 'All', 2020 )
print( "Inspections" )
print( inspections )
CAAinspections = region.get_events( 'inspections', 'CAA', 2020 )
print( CAAinspections )
CWAinspections = region.get_events( 'inspections', 'CWA', 2020 )
print( CWAinspections )
RCRAinspections = region.get_events( 'inspections', 'RCRA', 2020 )
print( RCRAinspections )
'''

print( 'Per 1000 - inspections')
inspectionsper1000_state = region.get_per_1000( 'inspections', 'State', 2020 )
print( inspectionsper1000_state )
inspectionsper1000_cd = region.get_per_1000( 'inspections', 'CD', 2020 )
print( inspectionsper1000_cd )

print( 'Per 1000 - violations')
violationsper1000_state = region.get_per_1000( 'violations', 'State', 2020 )
print( violationsper1000_state )
violationsper1000_cd = region.get_per_1000( 'inspections', 'CD', 2020 )
print( violationsper1000_cd )

CAAbadactors = region.get_non_compliants( 'CAA' )
print( 'CAA Non-compliants' )
print( CAAbadactors[['fac_name', 'noncomp_count']] )
CWAbadactors = region.get_non_compliants( 'CWA' )
print( 'CWA Non-compliants' )
print( CWAbadactors[['fac_name', 'noncomp_count']] )
RCRAbadactors = region.get_non_compliants( 'RCRA' )
print( 'RCRA Non-compliants' )
print( RCRAbadactors[['fac_name', 'noncomp_count']] )