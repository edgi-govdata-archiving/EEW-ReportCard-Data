import pdb
from Region import Region
import pandas as pd

programs = ['CAA', 'CWA', 'RCRA']
region = Region(db='region_cds.db', type='State', state='AK',
                base_year=2023, programs=programs)

inflation = region.get_inflation()
print("Inflation")
print(inflation)


df = region.get_per_1000('inspections', 'USA')
print("USA inspections")
print(df)
df = region.get_per_1000('violations', 'USA')
print("USA violations")
print(df)
df = region.get_per_1000('violations', 'State')
print("State violations")
print(df)

'''
cwa_per_1000 = region.get_cwa_per_1000(2021)
print("CWA violations per 1000 facilities")
print(cwa_per_1000)
'''

print('CAA active facilities: {}'.format(region.get_active_facilities('CAA')))
print('CWA active facilities: {}'.format(region.get_active_facilities('CWA')))
print('RCRA active facilities: {}'.format(region.get_active_facilities('RCRA')))
print('GHG active facilities: {}'.format(region.get_active_facilities('GHG')))

CAArecurring = region.get_recurring_violations('CAA')
print(CAArecurring)
CWArecurring = region.get_recurring_violations('CWA')
print(CWArecurring)
RCRArecurring = region.get_recurring_violations('RCRA')
print(RCRArecurring)

violations = region.get_events('violations', 'All')
print("Violations")
print("  All programs")
print(violations)
CAAviolations = region.get_events('violations', 'CAA')
print("  CAA")
print(CAAviolations)
CWAviolations = region.get_events('violations', 'CWA')
print("  CWA")
print(CWAviolations)
RCRAviolations = region.get_events('violations', 'RCRA')
print("  RCRA")
print(RCRAviolations)

enforcement = region.get_events('enforcements', 'All')
print("Enforcements")
print("  All Programs")
print(enforcement)
CAAenforcement = region.get_events('enforcements', 'CAA')
print("  CAA")
print(CAAenforcement)
CWAenforcement = region.get_events('enforcements', 'CWA')
print("  CWA")
print(CWAenforcement)
RCRAenforcement = region.get_events('enforcements', 'RCRA')
print("  RCRA")
print(RCRAenforcement)

inspections = region.get_events('inspections', 'All')
print("Inspections")
print(inspections)
CAAinspections = region.get_events('inspections', 'CAA')
print(CAAinspections)
CWAinspections = region.get_events('inspections', 'CWA')
print(CWAinspections)
RCRAinspections = region.get_events('inspections', 'RCRA')
print(RCRAinspections)

print('Per 1000 - inspections')
inspectionsper1000_state = region.get_per_1000('inspections', 'State')
print(inspectionsper1000_state)

print('Per 1000 - violations')
violationsper1000_state = region.get_per_1000('violations', 'State')
print(violationsper1000_state)

CAAbadactors = region.get_non_compliants('CAA')
print('CAA Non-compliants')
print(CAAbadactors[['fac_name', 'noncomp_count']])
CWAbadactors = region.get_non_compliants('CWA')
print('CWA Non-compliants')
print(CWAbadactors[['fac_name', 'noncomp_count']])
RCRAbadactors = region.get_non_compliants('RCRA')
print('RCRA Non-compliants')
print(RCRAbadactors[['fac_name', 'noncomp_count']])
