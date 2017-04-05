import pandas as pdimport os, sysfrom scipy import statsimport numpy as npimport matplotlib.pyplot as pltapi_prefix = "http://api.census.gov/data/2015/acs5?get=B23025_004E,B19001_002E,B02001_003E,B06008_002E&for=zip+code+tabulation+area:"api_surfix = "&key=0ae05f09bcc0b7b3f4f464f6349248a087c877b9"vacant_url = "https://data.cityofchicago.org/resource/yama-9had.json?$where=date_service_request_was_received%3E%222017-01-01%22"sanitation_url = "https://data.cityofchicago.org/resource/kcdz-f29q.json?$where=creation_date%3E%222017-01-01%22"var1 = "B23025_004E" #Employment Status for the Population 16 Years and Overvar2 = "B19001_002E" #Household Income in the Past 12 Months Less than $10,000var3 = "B02001_003E" #Race_Black or African American alonevar4 = "B06008_002E" #Never marriedvacant_df = pd.read_json(vacant_url, orient='records')vacant_df.rename(columns= {"service_request_type":"type_of_service_request"}, inplace=True)vacant_df.rename(columns= {"date_service_request_was_received":"creation_date"}, inplace=True)vacant_tmp = vacant_dfvacant_tmp['creation_month'] = pd.DatetimeIndex(vacant_tmp['creation_date']).monthvacant_tmp['vacant_counts'] = vacant_tmp.groupby(["zip_code","creation_month"])[["type_of_service_request"]].transform("count")vacant_tmp = vacant_tmp[["zip_code",'creation_month',"vacant_counts","type_of_service_request"]].dropna().drop_duplicates()sanitation_df = pd.read_json(sanitation_url, orient='records')sanitation_tmp = sanitation_dfsanitation_tmp['creation_month'] = pd.DatetimeIndex(sanitation_tmp['creation_date']).monthsanitation_tmp['sanitation_counts'] = sanitation_tmp.groupby(["zip_code","creation_month"])[["type_of_service_request"]].transform("count")sanitation_tmp = sanitation_tmp[["zip_code","creation_month", "sanitation_counts", "type_of_service_request"]].dropna().drop_duplicates()df_merged = pd.merge(vacant_tmp, sanitation_tmp, how='outer', on=["zip_code","creation_month", "type_of_service_request"])df_merged['employed'] = 0df_merged['low_income'] = 0df_merged['race'] = 0df_merged['never_married'] = 0for index, row in df_merged.iterrows():	api_url = api_prefix+str(int(row['zip_code']))+api_surfix	result_df = pd.read_json(api_url, orient='records')	df_merged.set_value(index, 'employed', result_df[0].iloc[1])	df_merged.set_value(index, 'low_income', result_df[1].iloc[1])	df_merged.set_value(index, 'race', result_df[2].iloc[1])	df_merged.set_value(index, 'never_married', result_df[3].iloc[1])df_merged = df_merged.sort_values(by = 'vacant_counts', ascending = False)df_merged.fillna(0,inplace=True)df_merged.to_csv("q2_3_vacant.csv")table = pd.pivot_table(df_merged, values='low_income', index=['type_of_service_request', 'zip_code'], columns=['creation_month'], aggfunc=np.sum)table.to_csv("q2_3_pivot.csv")