
import pandas as pd

#download the data frame
graffiti_url = "https://data.cityofchicago.org/resource/cdmx-wzbz.json?$where=creation_date%3E%222016-04-01%22"
vacant_url = "https://data.cityofchicago.org/resource/yama-9had.json?$where=date_service_request_was_received%3E%222016-04-01%22"
potholes_url = "https://data.cityofchicago.org/resource/787j-mys9.json?$where=creation_date%3E%222016-04-01%22"
sanitation_url = "https://data.cityofchicago.org/resource/kcdz-f29q.json?$where=creation_date%3E%222016-04-01%22"

graffiti_df = pd.read_json(graffiti_url, orient='records')
vacant_df = pd.read_json(vacant_url, orient='records')
vacant_df.rename(columns= {"service_request_type":"type_of_service_request"}, inplace=True)
vacant_df['street_address'] = vacant_df[['address_street_number','address_street_direction','address_street_name','address_street_suffix']].apply(lambda x : '{} {} {} {}'.format(x[0],x[1],x[2],x[3]), axis=1)
potholes_df = pd.read_json(potholes_url, orient='records')
potholes_df.rename(columns= {"zip":"zip_code"}, inplace=True)
sanitation_df = pd.read_json(sanitation_url, orient='records')

#combine four data frames and summary statistics
df_origin = pd.concat([graffiti_df,vacant_df,potholes_df,sanitation_df], axis=0)

#7500 S Wolcott Ave
df_origin_tmp = df_origin
df_origin_tmp = df_origin_tmp[df_origin_tmp['street_address'] == "7500 S WOLCOTT AVE"]
df_origin_tmp['counts'] = df_origin_tmp.groupby("type_of_service_request")[["ward"]].transform("count")
df_origin_tmp = df_origin_tmp[["type_of_service_request","counts"]].dropna().drop_duplicates()
df_origin_tmp.to_csv("q3_Wolcott.csv")


#uptown & Lawndale
graffiti_tmp = graffiti_df
graffiti_tmp = graffiti_tmp[graffiti_tmp['community_area'].isin([3,29,30])]
graffiti_tmp['counts'] = graffiti_tmp.groupby("community_area")[["type_of_service_request"]].transform("count")
graffiti_tmp = graffiti_tmp[["community_area","counts"]].dropna().drop_duplicates()
graffiti_tmp.to_csv("q3_graffiti.csv")

