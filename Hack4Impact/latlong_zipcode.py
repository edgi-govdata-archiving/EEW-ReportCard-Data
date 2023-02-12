import pandas as pd
import requests as rq


def latlong_to_CD():
    df = pd.read_csv("ECHO_EXPORTER.CSV")
    latlong = df[['FAC_LONG','FAC_LAT']]
    CD = []
    for row in latlong:
      zc = cord_to_zip(zip_to_CD(row[0],row[1]))
      CD.append(zip_to_CD(zc))
    return CD
    

def cord_to_zip(longitude, latitude):

  url = "https://www.melissa.com/v2/lookups/latlngzip4/?lat="+str(latitude)+"&lng="+str(longitude)
  zipcode = rq.get(url)
  return (zipcode)


def zip_to_CD(zipcode):
  df = pandas.read_table('https://www2.census.gov/geo/relfiles/cdsld18/natl/natl_zccd_delim.txt', sep=','
  ,header ='CONGRESSIONAL DISTRICTS BY ZIP CODE TABULATION AREA (ZCTA) (NATIONAL)',names=['State','ZCTA',"Congressional District"])
  if df['ZCTA'==zipcode]:
    return [df['State'],df['Congressional District']]
  
  


testlng = 28.45743
testlat= -82.409148

print(cord_to_zip(testlng, testlat))