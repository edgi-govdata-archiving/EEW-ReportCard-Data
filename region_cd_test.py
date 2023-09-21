import pdb
from Region import Region
import pandas as pd

programs = ['CAA', 'CWA', 'RCRA']

region = Region( type='Congressional District', value='02', state='CO',
                programs=programs )

target_year = 2022


inflation = get_inflation( target_year )
print( "Inflation" )
print( inflation )


df = region.get_per_1000( 'inspections', 'USA', target_year )
print( "USA inspections" )
print(df)
df = region.get_per_1000( 'violations', 'USA', target_year )
print( "USA violations" )
print(df)

cwa_per_1000 = region.get_cwa_per_1000( target_year )
print( "CWA violations per 1000 facilities" )
print( cwa_per_1000['violations'] )
print( "CWA inspections per 1000 facilities" )
print( cwa_per_1000['inspections'] )
print( "CWA enforcements per 1000 facilities" )
print( cwa_per_1000['enforcements'] )

for cd in ['AK00','MT00','ND00']:
    for type in ['inspections','violations','enforcements']:
        print( '{} {}: {}'.format(cd, type, cwa_per_1000[cwa_per_1000['CD.State']=='AK00']))

# print( cwa_per_1000[cwa_per_1000['CD.State']=='DE00'])
# print( cwa_per_1000[cwa_per_1000['CD.State']=='MT00'])
# print( cwa_per_1000[cwa_per_1000['CD.State']=='ND00'])

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

violations = region.get_events( 'violations', 'All', target_year )
print( "Violations" )
print( violations )
CAAviolations = region.get_events( 'violations', 'CAA', target_year )
print( CAAviolations )
CWAviolations = region.get_events( 'violations', 'CWA', target_year )
print( CWAviolations )
RCRAviolations = region.get_events( 'violations', 'RCRA', target_year )
print( RCRAviolations )
enforcement = region.get_events( 'enforcements', 'All', target_year )
print( "Enforcements" )
print( enforcement )
CAAenforcement = region.get_events( 'enforcements', 'CAA', target_year )
print( CAAenforcement )
CWAenforcement = region.get_events( 'enforcements', 'CWA', target_year )
print( CWAenforcement )
RCRAenforcement = region.get_events( 'enforcements', 'RCRA', target_year )
print( RCRAenforcement )
inspections = region.get_events( 'inspections', 'All', target_year )
print( "Inspections" )
print( inspections )
CAAinspections = region.get_events( 'inspections', 'CAA', target_year )
print( CAAinspections )
CWAinspections = region.get_events( 'inspections', 'CWA', target_year )
print( CWAinspections )
RCRAinspections = region.get_events( 'inspections', 'RCRA', target_year )
print( RCRAinspections )

print( 'Per 1000 - inspections')
inspectionsper1000_state = region.get_per_1000( 'inspections', 'State', target_year )
print( inspectionsper1000_state )
inspectionsper1000_cd = region.get_per_1000( 'inspections', 'CD', target_year )
print( inspectionsper1000_cd )

print( 'Per 1000 - violations')
violationsper1000_state = region.get_per_1000( 'violations', 'State', target_year )
print( violationsper1000_state )
violationsper1000_cd = region.get_per_1000( 'inspections', 'CD', target_year )
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
