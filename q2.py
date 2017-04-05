import pandas as pdfrom scipy import statsimport matplotlib.pyplot as plt#call APIapi_prefix = "http://api.census.gov/data/2015/acs5?get=B23025_004E,B19001_002E,B02001_003E,B06008_002E&for=zip+code+tabulation+area:"api_surfix = "&key=0ae05f09bcc0b7b3f4f464f6349248a087c877b9"vacant_url = "https://data.cityofchicago.org/resource/yama-9had.json?$where=date_service_request_was_received%3E%222017-01-01%22"sanitation_url = "https://data.cityofchicago.org/resource/kcdz-f29q.json?$where=creation_date%3E%222017-01-01%22"var1 = "B23025_004E" #Employment Status for the Population 16 Years and Overvar2 = "B19001_002E" #Household Income in the Past 12 Months Less than $10,000var3 = "B02001_003E" #Race_Black or African American alonevar4 = "B06008_002E" #Never married#merge two 311 requests data frame by zipcode vacant_df = pd.read_json(vacant_url, orient='records')vacant_df.rename(columns= {"date_service_request_was_received":"creation_date"}, inplace=True)vacant_tmp = vacant_dfvacant_df.rename(columns= {"service_request_type":"type_of_service_request"}, inplace=True)vacant_tmp['vacant_counts'] = vacant_tmp.groupby("zip_code")[["type_of_service_request"]].transform("count")vacant_tmp = vacant_tmp[["zip_code","vacant_counts"]].dropna().drop_duplicates()sanitation_df = pd.read_json(sanitation_url, orient='records')sanitation_tmp = sanitation_dfsanitation_tmp['sanitation_counts'] = sanitation_tmp.groupby("zip_code")[["type_of_service_request"]].transform("count")sanitation_tmp = sanitation_tmp[["zip_code","sanitation_counts"]].dropna().drop_duplicates()df_merged = vacant_tmp.merge(sanitation_tmp, left_on='zip_code', right_on='zip_code', how='outer')#merge four variable datadf_merged['employed'] = 0df_merged['low_income'] = 0df_merged['race'] = 0df_merged['never_married'] = 0for index, row in df_merged.iterrows():    api_url = api_prefix+str(int(row['zip_code']))+api_surfix    result_df = pd.read_json(api_url, orient='records')    df_merged.set_value(index, 'employed', result_df[0].iloc[1])    df_merged.set_value(index, 'low_income', result_df[1].iloc[1])    df_merged.set_value(index, 'race', result_df[2].iloc[1])    df_merged.set_value(index, 'never_married', result_df[3].iloc[1])#compare 4 different variables on two 311 request data separatelydf_merged = df_merged.sort_values(by = 'vacant_counts', ascending = False)df_merged.fillna(0,inplace=True)df_merged.to_csv("q2_vacant.csv")df_merged = df_merged.sort_values(by = 'sanitation_counts', ascending = False)df_merged.fillna(0,inplace=True)df_merged.to_csv("q2_sanitation_counts.csv")#Types of blocks get "Vacant and Abandoned Buildings Reported"slope, intercept, r_value, p_value, std_err = stats.linregress(df_merged["low_income"],df_merged["vacant_counts"])solution1 = slope, std_err, p_valueprint solution1#slope = 0.01103305292065862, std_err = 0.002776819146512283, p_value = 0.00021936322134368024slope, intercept, r_value, p_value, std_err = stats.linregress(df_merged["employed"],df_merged["vacant_counts"])solution2 = slope, std_err, p_valueprint solution2#-0.00029749054251681486, 0.00031107494530383927, 0.34333025296268405slope, intercept, r_value, p_value, std_err = stats.linregress(df_merged["race"],df_merged["vacant_counts"])solution3 = slope, std_err, p_valueprint solution3#0.0013070388061900114, 0.00011794012821450985, 2.6556335510808864e-15slope, intercept, r_value, p_value, std_err = stats.linregress(df_merged["never_married"],df_merged["vacant_counts"])solution4 = slope, std_err, p_valueprint solution4#0.00069379456333982408, 0.00038281512805191884, 0.07570757341422453#Types of blocks get "Sanitation Code Complaints"slope, intercept, r_value, p_value, std_err = stats.linregress(df_merged["employed"],df_merged["sanitation_counts"])solution5 = slope, std_err, p_valueprint solution5#0.00074999334630390169, 0.00012906031558856392, 3.8194802067341948e-07slope, intercept, r_value, p_value, std_err = stats.linregress(df_merged["low_income"],df_merged["sanitation_counts"])solution6 = slope, std_err, p_valueprint solution6#0.0042969964843696919, 0.0015650557702498835, 0.0082752620226636368slope, intercept, r_value, p_value, std_err = stats.linregress(df_merged["race"],df_merged["sanitation_counts"])solution7 = slope, std_err, p_valueprint solution7#0.00018105968534159984, 0.00011143023601367033, 0.11023682850445485slope, intercept, r_value, p_value, std_err = stats.linregress(df_merged["never_married"],df_merged["sanitation_counts"])solution8 = slope, std_err, p_valueprint solution8#0.0010936182379454019, 0.00014307485595210499, 4.6713084371325362e-10