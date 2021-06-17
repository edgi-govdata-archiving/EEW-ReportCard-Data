import pdb
from Region import Region
import pandas as pd

region = Region( type='Congressional District', value='34', state='TX' )
df = region.get_usa_per_1000( 'inspections', 2020 )
print( "USA inspections" )
print(df)
df = region.get_usa_per_1000( 'violations', 2020 )
print( "USA violations" )
print(df)

inflation = region.get_inflation( 2019 )
print( "Inflation" )
print( inflation )

cwa_per_1000 = region.get_cwa_per_1000( 2020 )
print( "CWA violations per 1000 facilities" )
print( cwa_per_1000 )

active_facilities = region.get_active_facilities()
print( "Active Facilities" )
print( active_facilities )

cwa_violations = region.get_cwa_violations( 2020 )
print( "CWA violations" )
print( cwa_violations )

enforcements = region.get_enforcements( 2020 )
print( "Enforcements" )
print( enforcements )
