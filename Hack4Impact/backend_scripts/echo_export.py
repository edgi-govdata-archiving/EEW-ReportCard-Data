from ECHO_modules.get_data import get_echo_data

def echo_data(sql_string):
    sql = 'select "FAC_NAME", "CWA_NAICS", "CAA_NAICS", "RCRA_NAICS", "FAC_NAICS_CODES", "FAC_LAT", "FAC_LONG" from "ECHO_EXPORTER" where ' + sql_string
    try:
        echo = get_echo_data( sql ) 
    except pandas.errors.EmptyDataError:
        print("\nThere are no records.\n")
    
    echo