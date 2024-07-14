from Region import Region

from create_df import create_df
target_year_int = 2023
this_state = 'NY'
Region_CD = '09'
cwa_insp_years = create_df(db='region_cds.db', target_year=target_year_int,
               region_type='Congressional District', data_type='inspections',
               y_field='Count',program='CWA', states=[this_state,],
               region_selected=Region_CD)
print("cwa_insp_years\n",cwa_insp_years)
cwa_viol_years = create_df(db='region_cds.db', target_year=target_year_int,
               region_type='Congressional District',data_type='violations',
               y_field='Count',program='CWA',states=[this_state,],
               region_selected=Region_CD)
print("cwa_viol_years\n",cwa_viol_years)
cwa_enf_count_years = create_df(db='region_cds.db', target_year=target_year_int,
               region_type='Congressional District',data_type='enforcements',
               y_field='Count',program='CWA',states=[this_state,],
               region_selected=Region_CD)
print("cwa_enf_count_years\n",cwa_enf_count_years)
cwa_enf_count_years = cwa_enf_count_years.drop('Amount', axis=1)
print("cwa_enf_count_years\n",cwa_enf_count_years)
cwa_enf_dollar_years = create_df(db='region_cds.db', target_year=target_year_int,
               region_type='Congressional District',data_type='enforcements',
               y_field='Amount',program='CWA',states=[this_state,],region_selected=Region_CD)
print("cwa_enf_dollar_years\n",cwa_enf_dollar_years)
cwa_enf_dollar_years = cwa_enf_dollar_years.drop('Count', axis=1)
print("cwa_enf_dollar_years\n",cwa_enf_dollar_years)
cwa_enf_dollar_years['USA'] = cwa_enf_dollar_years['USA']/1000.
print("cwa_enf_dollar_years[USA]\n",cwa_enf_dollar_years['USA']/1000.)
cwa_enf_dollar_years[this_state] = cwa_enf_dollar_years[this_state]/1000.
print("cwa_enf_dollar_years[state]\n",cwa_enf_dollar_years[this_state]/1000.)
cwa_enf_dollar_years[Region_CD] = cwa_enf_dollar_years[Region_CD]/1000.
print("cwa_enf_dollar_years[region]\n",cwa_enf_dollar_years[Region_CD]/1000.)
