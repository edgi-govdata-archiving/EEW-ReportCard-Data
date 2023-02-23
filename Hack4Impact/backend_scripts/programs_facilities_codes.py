# Make selections about programs, active facilities, and NAICS codes
import ipywidgets 
def make_selections():
    programs = ipywidgets.SelectMultiple(
        options=['NPDES_FLAG', 'AIR_FLAG', 'RCRA_FLAG'],
        description='Programs',
        disabled=False
    )
    active = ipywidgets.Checkbox(
        value=False,
        description='Only active facilities?',
        disabled=False
    )
    naics = ipywidgets.Text(
        value='',
        placeholder='Enter NAICS codes here, separated with a comma', #2111,213111,213112
        description='NAICS codes',
        disabled=False
    )
    display(programs, active, naics)
    output = [programs, active, naics]

    return output


