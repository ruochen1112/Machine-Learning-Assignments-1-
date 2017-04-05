import pandas 
import datetime



#download the data frame
graffiti_url = "https://data.cityofchicago.org/resource/cdmx-wzbz.json?$where=creation_date%3E%222016-04-01%22"
vacant_url = "https://data.cityofchicago.org/resource/yama-9had.json?$where=date_service_request_was_received%3E%222016-04-01%22"
potholes_url = "https://data.cityofchicago.org/resource/787j-mys9.json?$where=creation_date%3E%222016-04-01%22"
sanitation_url = "https://data.cityofchicago.org/resource/kcdz-f29q.json?$where=creation_date%3E%222016-04-01%22"

graffiti_df = pandas.read_json(graffiti_url, orient='records')
vacant_df = pandas.read_json(vacant_url, orient='records')
vacant_df.rename(columns= {"service_request_type":"type_of_service_request"}, inplace=True)
vacant_df['street_address'] = vacant_df[['address_street_number','address_street_direction','address_street_name','address_street_suffix']].apply(lambda x : '{} {} {} {}'.format(x[0],x[1],x[2],x[3]), axis=1)
vacant_df.rename(columns= {"date_service_request_was_received":"creation_date"}, inplace=True)
potholes_df = pandas.read_json(potholes_url, orient='records')
potholes_df.rename(columns= {"zip":"zip_code"}, inplace=True)
sanitation_df = pandas.read_json(sanitation_url, orient='records')

#graffiti by month
graffiti_df_tmp = graffiti_df
graffiti_df_tmp['creation_month'] = pandas.DatetimeIndex(graffiti_df_tmp['creation_date']).month
graffiti_df_tmp['graffiti'] = graffiti_df_tmp.groupby("creation_month")[["zip_code"]].transform("count")
graffiti_df_tmp = graffiti_df_tmp[["creation_month","graffiti"]].dropna().drop_duplicates().sort_values(by = 'creation_month', ascending = True)

#vacant by month
vacant_df_tmp = vacant_df
vacant_df_tmp['creation_month'] = pandas.DatetimeIndex(vacant_df_tmp['creation_date']).month
vacant_df_tmp['vacant'] = vacant_df_tmp.groupby("creation_month")[["zip_code"]].transform("count")
vacant_df_tmp = vacant_df_tmp[["creation_month","vacant"]].dropna().drop_duplicates().sort_values(by = 'creation_month', ascending = True)


#potholes by month
potholes_df_tmp = potholes_df
potholes_df_tmp['creation_month'] = pandas.DatetimeIndex(potholes_df_tmp['creation_date']).month
potholes_df_tmp['potholes'] = potholes_df_tmp.groupby("creation_month")[["zip_code"]].transform("count")
potholes_df_tmp = potholes_df_tmp[["creation_month","potholes"]].dropna().drop_duplicates().sort_values(by = 'creation_month', ascending = True)


#sanitation by month
sanitation_df_tmp = sanitation_df
sanitation_df_tmp['creation_month'] = pandas.DatetimeIndex(sanitation_df_tmp['creation_date']).month
sanitation_df_tmp['sanitation'] = sanitation_df_tmp.groupby("creation_month")[["zip_code"]].transform("count")
sanitation_df_tmp = sanitation_df_tmp[["creation_month","sanitation"]].dropna().drop_duplicates().sort_values(by = 'creation_month', ascending = True)

#merge to compare
merge1 = graffiti_df_tmp.merge(vacant_df_tmp, left_on='creation_month', right_on='creation_month', how='outer')
merge2 = potholes_df_tmp.merge(sanitation_df_tmp, left_on='creation_month', right_on='creation_month', how='outer')
merge_total = merge1.merge(merge2,left_on='creation_month', right_on='creation_month', how='outer').sort_values(by = 'creation_month', ascending = True)

merge_total.to_csv("q1_requests_by_month.csv")
plot = merge_total.plot(kind = "bar", x = "creation_month")
plot.get_figure().savefig('q1_requests_by_month.pdf', bbox_inches='tight')



#combine four data frames and summary statistics
df_origin = pandas.concat([graffiti_df,vacant_df,potholes_df,sanitation_df], axis=0)
df_statistics = df_origin.describe()
df_statistics.to_csv("q1_statistics.csv")


# count service type
df_origin_tmp = df_origin
df_origin_tmp['counts'] = df_origin_tmp.groupby("type_of_service_request")[["zip_code"]].transform("count")
df_origin_tmp = df_origin_tmp[["type_of_service_request","counts"]].dropna().drop_duplicates().sort_values(by = 'counts', ascending = False)
df_origin_tmp.to_csv("q1_type_of_service_request.csv")


# count graffiti sub type
graffiti_tmp = df_origin
graffiti_tmp.rename(columns= {"what_type_of_surface_is_the_graffiti_on_":"subtype"}, inplace=True)
graffiti_tmp['counts'] = graffiti_tmp.groupby("subtype")[["type_of_service_request"]].transform("count")
graffiti_tmp = graffiti_tmp[["subtype","counts"]].dropna().drop_duplicates().sort_values(by = 'counts', ascending = False)
graffiti_tmp.to_csv("q1_subtype_counts.csv")


# count zipcode
df_origin_tmp = df_origin
df_origin_tmp['counts'] = df_origin_tmp.groupby("zip_code")[["type_of_service_request"]].transform("count")
df_origin_tmp = df_origin_tmp[["zip_code","counts"]].dropna().drop_duplicates().sort_values(by = 'counts', ascending = False)
df_origin_tmp.to_csv("q1_neighborhood_counts.csv")
df_origin_tmp = df_origin_tmp.head(10)
ax = df_origin_tmp.plot(kind = "bar", x = "zip_code", y = "counts")
ax.get_figure().savefig('q1_zip_code_counts.pdf', bbox_inches='tight')

#response time
df_origin_tmp = df_origin
df_origin_tmp['response_time'] = pandas.to_datetime(df_origin_tmp['completion_date'],infer_datetime_format=True) - pandas.to_datetime(df_origin_tmp['creation_date'],infer_datetime_format=True)
df_origin_tmp['counts'] = df_origin_tmp.groupby("response_time")[["type_of_service_request"]].transform("count")
df_origin_tmp = df_origin_tmp[["response_time","counts"]].dropna().drop_duplicates().sort_values(by = 'counts', ascending = False)
df_origin_tmp.to_csv("q1_response_time.csv")


# count neighborhood
df_origin_tmp = df_origin
df_origin_tmp['counts'] = df_origin_tmp.groupby("street_address")[["type_of_service_request"]].transform("count")
df_origin_tmp = df_origin_tmp[["street_address","counts"]].dropna().drop_duplicates().sort_values(by = 'counts', ascending = False)
df_origin_tmp.to_csv("q1_street_address_counts.csv")








