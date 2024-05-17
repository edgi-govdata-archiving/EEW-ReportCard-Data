import pandas as pd
from Region import Region

def create_df(db, target_year, region_type, data_type, y_field, program, 
              states, region_selected = None):
    usa_region = Region(db=db, type='Nation')
    usa_num_facs = usa_region.get_active_facilities(program)
    usa_events = usa_region.get_events( data_type, program)
    usa_events['USA'] = usa_events[y_field]/usa_num_facs
    state_events_dict = {}
    for state in states:
        state_region = Region( db=db, type='State', state=state, 
                    programs=[program,] )
        state_num_facs = state_region.get_active_facilities( program )
        state_events = state_region.get_events( data_type, program)
        state_events[ state ] = state_events[y_field]/state_num_facs
        state_events_dict[ state ] = state_events
    if ( region_type != 'State' ):
        local_region = Region(db=db, type=region_type, state=states[0], 
                              value=region_selected, programs=[program,])
        local_num_facs = local_region.get_active_facilities( program )
        local_events = local_region.get_events( data_type, program)
        local_events[ region_selected ] = local_events[y_field]/local_num_facs
    df_events = usa_events.drop( y_field, axis=1 )
    for state_name,state_events in state_events_dict.items():
        df_events = df_events.merge( state_events[['Year',state_name]] )
    if ( region_type != 'State' ):
        df_events = df_events.merge( local_events[['Year',region_selected]])
    return df_events
  
