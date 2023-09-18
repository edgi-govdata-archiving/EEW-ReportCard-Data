import pdb
from Region import Region
import pandas as pd

programs = ['CAA', 'CWA', 'RCRA']
region = Region( type='State', state='CO',
                programs=programs )

inflation = region.get_inflation( 2021 )
print( "Inflation" )
print( inflation )


df = region.get_per_1000( 'inspections', 'USA', 2021 )
print( "USA inspections" )
print(df)
df = region.get_per_1000( 'violations', 'USA', 2021 )
print( "USA violations" )
print(df)
df = region.get_per_1000( 'violations', 'State', 2021 )
print( "State violations" )
print(df)

'''
cwa_per_1000 = region.get_cwa_per_1000( 2021 )
print( "CWA violations per 1000 facilities" )
print( cwa_per_1000 )
'''

print( 'CAA active facilities: {}'.format( region.get_active_facilities('CAA')))
print( 'CWA active facilities: {}'.format( region.get_active_facilities('CWA')))
print( 'RCRA active facilities: {}'.format( region.get_active_facilities('RCRA')))
print( 'GHG active facilities: {}'.format( region.get_active_facilities('GHG')))

CAArecurring = region.get_recurring_violations( 'CAA' )
print( CAArecurring )
CWArecurring = region.get_recurring_violations( 'CWA' )
print( CWArecurring )
RCRArecurring = region.get_recurring_violations( 'RCRA' )
print( RCRArecurring )

violations = region.get_events( 'violations', 'All', 2021 )
print( "Violations" )
print( "  All programs" )
print( violations )
CAAviolations = region.get_events( 'violations', 'CAA', 2021 )
print( "  CAA" )
print( CAAviolations )
CWAviolations = region.get_events( 'violations', 'CWA', 2021 )
print( "  CWA" )
print( CWAviolations )
RCRAviolations = region.get_events( 'violations', 'RCRA', 2021 )
print( "  RCRA" )
print( RCRAviolations )

enforcement = region.get_events( 'enforcements', 'All', 2021 )
print( "Enforcements" )
print( "  All Programs" )
print( enforcement )
CAAenforcement = region.get_events( 'enforcements', 'CAA', 2021 )
print( "  CAA" )
print( CAAenforcement )
CWAenforcement = region.get_events( 'enforcements', 'CWA', 2021 )
print( "  CWA" )
print( CWAenforcement )
RCRAenforcement = region.get_events( 'enforcements', 'RCRA', 2021 )
print( "  RCRA" )
print( RCRAenforcement )

inspections = region.get_events( 'inspections', 'All', 2021 )
print( "Inspections" )
print( inspections )
CAAinspections = region.get_events( 'inspections', 'CAA', 2021 )
print( CAAinspections )
CWAinspections = region.get_events( 'inspections', 'CWA', 2021 )
print( CWAinspections )
RCRAinspections = region.get_events( 'inspections', 'RCRA', 2021 )
print( RCRAinspections )

print( 'Per 1000 - inspections')
inspectionsper1000_state = region.get_per_1000( 'inspections', 'State', 2021 )
print( inspectionsper1000_state )

print( 'Per 1000 - violations')
violationsper1000_state = region.get_per_1000( 'violations', 'State', 2021 )
print( violationsper1000_state )

CAAbadactors = region.get_non_compliants( 'CAA' )
print( 'CAA Non-compliants' )
print( CAAbadactors[['fac_name', 'noncomp_count']] )
CWAbadactors = region.get_non_compliants( 'CWA' )
print( 'CWA Non-compliants' )
print( CWAbadactors[['fac_name', 'noncomp_count']] )
RCRAbadactors = region.get_non_compliants( 'RCRA' )
print( 'RCRA Non-compliants' )
print( RCRAbadactors[['fac_name', 'noncomp_count']] )
