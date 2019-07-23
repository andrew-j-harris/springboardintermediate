#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import requests 
import json

# import 100000 entry sample for preliminary investigation and cleaning

url = 'https://data.cityofchicago.org/resource/m6dm-c72p.json?$limit=100000&$offset=0' 

r = requests.get(url)

json_data = r.json()


# In[2]:


# save the file so that it doesn't need to be downloaded again.

with open('data.json', 'w') as outfile:
    json.dump(json_data, outfile)


# In[39]:


# read into pandas data frame.

sample_json_df = pd.read_json('data.json')


# In[40]:


# reindex the columns so that they are ordered the same as on the online data portal
# so that the dataframe is easier to work with.

columnsTitles = ['trip_id', 
                 'trip_start_timestamp', 
                 'trip_end_timestamp', 
                 'trip_seconds',
                 'trip_miles',
                 'pickup_census_tract',
                 'dropoff_census_tract',
                 'pickup_community_area',
                 'dropoff_community_area',
                 'fare',
                 'tip',
                 'additional_charges',
                 'trip_total',
                 'shared_trip_authorized',
                 'trips_pooled',
                 'pickup_centroid_latitude',
                 'pickup_centroid_longitude',
                 'pickup_centroid_location',
                 'dropoff_centroid_latitude',
                 'dropoff_centroid_longitude',
                 'dropoff_centroid_location']

sample_json_df = sample_json_df.reindex(columns=columnsTitles)


# In[41]:


# check to see that the order has been correctly implemented.

sample_json_df.columns


# In[43]:


# Print out of info() to see where the NaN values are present and to see data types
# Number of NaN values in each entry - easier to read than info()

sample_json_df.info()
sample_json_df.isna().sum()


# In[ ]:


# Path Forward for Data Cleaning:
# Investigation of NaN and correction of NaN values will be performed for all columns.
# Then the numerical data itself will be assessed for outliers and general cohesion.


# In[45]:


# investigation of the trip_seconds column which contains NaN values.
# this code returns a data frame of the rows where trip_seconds is NaN

timeissue = sample_json_df[sample_json_df['trip_seconds'].isna()]


# In[46]:


# dataframe created so that the trip_seconds durations which are NaN can be compared to the time stamps.

timeissue=timeissue[['trip_start_timestamp','trip_end_timestamp','trip_seconds']]


# In[47]:


# visual inspection indicates that trip_end_timestamp values occur before trip_start_timestamp values
# it is observed that all of these timestamps occur on November 4, 2019 between midnight and 2:00 AM

timeissue


# In[48]:


# conversion of the timestamp columns to datetime and subtraction operation performed

timeissue['trip_start_timestamp']=pd.to_datetime(timeissue['trip_start_timestamp'], format='%Y-%m-%dT%H:%M:%S.000')
timeissue['trip_end_timestamp']=pd.to_datetime(timeissue['trip_end_timestamp'], format='%Y-%m-%dT%H:%M:%S.000')
delta=timeissue['trip_end_timestamp']-timeissue['trip_start_timestamp']


# In[49]:


# Print out of differences show that differences between timestamps are either 15 minutes, 30 minutes, 45 minutes or 1 hour.

delta


# In[50]:


# application of datetime change to the dataframe being cleaned.

sample_json_df['trip_start_timestamp']=pd.to_datetime(sample_json_df['trip_start_timestamp'], format='%Y-%m-%dT%H:%M:%S.000')
sample_json_df['trip_end_timestamp']=pd.to_datetime(sample_json_df['trip_end_timestamp'], format='%Y-%m-%dT%H:%M:%S.000')


# In[51]:


# Decision is made to just delete these rows as this issue is confined to a 2 hour time range and only affects a small 
# number of 100000 trips.

timecolumn = sample_json_df['trip_seconds']
timecolumn = pd.DataFrame(timecolumn)
logictime = timecolumn.isnull().all(1)
logictime = pd.DataFrame(logictime)
logictime.rename(columns={0:'boolval'}, inplace=True)
logictime = logictime.reindex(logictime['boolval'])
sample_json_df = sample_json_df[logictime.index==False]
sample_json_df = sample_json_df.reset_index(drop=True)
sample_json_df.info()
sample_json_df.isna().sum()


# In[52]:


# decision is made to change the trip_seconds column to trip_minutes and present this column as minutes instead of seconds
# we will also put the dataframe in chronological order based on trip_start_timestamp.

sample_json_df['trip_seconds']=sample_json_df['trip_seconds']/60
sample_json_df=sample_json_df.rename(columns = {'trip_seconds':'trip_minutes'})
sample_json_df=sample_json_df.sort_values(['trip_start_timestamp', 'trip_end_timestamp'], ascending=[True, True])
sample_json_df=sample_json_df.reset_index(drop=True)
sample_json_df.info()
sample_json_df.isna().sum()


# In[53]:


# As per the .info() above, there are NaN values present in the following ten "location" columns:
#
#'dropoff_census_tract','dropoff_community_area','dropoff_centroid_latitude','dropoff_centroid_longitude',
#'dropoff_centroid_location','pickup_census_tract','pickup_community_area','pickup_centroid_latitude',
#'pickup_centroid_longitude', 'pickup_centroid_location'
#
# It should be noted that the spreadsheet CensusTractsTIGER2010 maps the centroid pairs to census tracts and maps 
# the census tracts to community areas. There are 76 community areas and the boundaries are shown on a map in
# the chicago data portal. Thus, any rows with missing community areas can be determined if centroid or census tract
# information is present.
#
# It should also be noted that in the chicago data portal, it is stated that for any trips that either start or end outside of
# Chicago, the "dropoff" fields and "pickup" fields will be left blank.

# The first step of location data cleaning will be to delete any rows that have no location data accross the
# five dropoff columns as this will eliminate trips that ended outside of chicago.


# In[54]:


# creation of dropoff data frame and count of the NaN entries.

samplerD=sample_json_df[['dropoff_census_tract','dropoff_community_area','dropoff_centroid_latitude','dropoff_centroid_longitude',
'dropoff_centroid_location']]
samplerD.info()
samplerD.isna().sum()


# In[55]:


# Removal of rows for which there is no dropoff data. 

dropoff = samplerD.isnull().all(1)
dropoff = pd.DataFrame(dropoff)
dropoff.rename(columns={0:'boolval'}, inplace=True)
dropoff = dropoff.reindex(dropoff['boolval'])
sample_json_df = sample_json_df[dropoff.index==False]
sample_json_df = sample_json_df.reset_index(drop=True)
sample_json_df.info()
sample_json_df.isna().sum()


# In[56]:


# Apply same method to pickup data. Creation of pickup data frame and count of the NaN entries.

samplerP=sample_json_df[['pickup_census_tract','pickup_community_area','pickup_centroid_latitude','pickup_centroid_longitude',
'pickup_centroid_location']]
samplerP.info()
samplerP.isna().sum()


# In[57]:


# Removal of rows for which there is no pickup data. 

pickup = samplerP.isnull().all(1)
pickup = pd.DataFrame(pickup)
pickup.rename(columns={0:'boolval'}, inplace=True)
pickup = pickup.reindex(pickup['boolval'])
sample_json_df = sample_json_df[pickup.index==False]
sample_json_df = sample_json_df.reset_index(drop=True)
sample_json_df.info()
sample_json_df.isna().sum()


# In[58]:


# Next we will examine the pickup_community_area and dropoff_community_area NaN values to see if they can be assigned
# based on the census tract values or centroid values using the spreadsheet previously described


# In[59]:


# check to see if the NaN dropoff_community_area rows have a value for dropoff_census_tract

samplerD=sample_json_df[['dropoff_census_tract','dropoff_community_area','dropoff_centroid_latitude','dropoff_centroid_longitude',
'dropoff_centroid_location']]
cutD=samplerD[samplerD['dropoff_community_area'].isna()== True]
cutD.info()
cutD


# In[60]:


# From visual inspection, it appears to be the same census tract and centroid coresponding to each NaN dropoff community area.
# we will change the formatting of numbers so that we can see all digits and confirm.

cutD['dropoff_census_tract'] = cutD.apply(lambda x: "{:.0f}".format(x['dropoff_census_tract']), axis=1)
cutD['dropoff_centroid_latitude'] = cutD.apply(lambda x: "{:.20f}".format(x['dropoff_centroid_latitude']), axis=1)
cutD['dropoff_centroid_longitude'] = cutD.apply(lambda x: "{:.20f}".format(x['dropoff_centroid_longitude']), axis=1)


# In[26]:


cutD


# In[61]:


# thedropoff_census_tract, dropoff_centroid_latitude and dropoff_centroid_longitude are the same for all rows.
# The dropoff_census_tract is not reported in the spreadsheet. So it can't be mapped to a community area.
# The dropoff_centroid_longitude is not matched in the spreadsheet. closest match is to -87.877
# The dropoff_centroid_latitude is not matched in the spreadsheet. closest match is to 41.982775
# Decision is made to drop these rows

sampler = sample_json_df['dropoff_community_area']
sampler = pd.DataFrame(sampler)
dropoff = sampler.isnull().all(1)
dropoff = pd.DataFrame(dropoff)
dropoff.rename(columns={0:'boolval'}, inplace=True)
dropoff = dropoff.reindex(dropoff['boolval'])
sample_json_df = sample_json_df[dropoff.index==False]
sample_json_df = sample_json_df.reset_index(drop=True)
sample_json_df.info()
sample_json_df.isna().sum()


# In[62]:


# check to see if the NaN pickup_community_area rows have a value for pickup_census_tract

samplerP=sample_json_df[['pickup_census_tract','pickup_community_area','pickup_centroid_latitude','pickup_centroid_longitude',
'pickup_centroid_location']]
cutP=samplerP[samplerP['pickup_community_area'].isna()== True]
cutP.info()
cutP


# In[63]:


# From visual inspection, it appears to be the same census tract coresponding to each NaN dropoff community area.
# we will change the formatting of dropoff_census_tract so that we can see all digits and confirm.

cutP['pickup_census_tract'] = cutP.apply(lambda x: "{:.0f}".format(x['pickup_census_tract']), axis=1)
cutP['pickup_centroid_latitude'] = cutP.apply(lambda x: "{:.20f}".format(x['pickup_centroid_latitude']), axis=1)
cutP['pickup_centroid_longitude'] = cutP.apply(lambda x: "{:.20f}".format(x['pickup_centroid_longitude']), axis=1)
cutP


# In[64]:


# same issue as for dropoff data. These rows will be deleted.

sampler = sample_json_df['pickup_community_area']
sampler = pd.DataFrame(sampler)
pickup = sampler.isnull().all(1)
pickup = pd.DataFrame(pickup)
pickup.rename(columns={0:'boolval'}, inplace=True)
pickup = pickup.reindex(pickup['boolval'])
sample_json_df = sample_json_df[pickup.index==False]
sample_json_df = sample_json_df.reset_index(drop=True)
sample_json_df.info()
sample_json_df.isna().sum()


# In[65]:


# as we are using the community areas as the location information source, we no longer need the census_tract columns.
#census tract columns will be deleted. centroid columns will be retained for the time being.

sample_json_df = sample_json_df.drop(['pickup_census_tract','dropoff_census_tract'],axis=1)
sample_json_df.info()
sample_json_df.isna().sum()


# In[66]:


# a quick assessment of the values present in the fare, tip, additional_charges, trip_total, shared_trip_authorized,
# trips_pooled columns will be performed to assess the integrity of the data and to address any NaN values

trips = sample_json_df[['fare','tip','additional_charges', 'trip_total','shared_trip_authorized','trips_pooled']]
trips = trips[trips['fare'].isna()== True]
trips


# In[67]:


# Delete this row from data frame

sample_json_df=sample_json_df.drop(62722,axis=0)
sample_json_df = sample_json_df.reset_index(drop=True)
sample_json_df.info()
sample_json_df.isna().sum()


# In[68]:


# As per the tables above, we now have a data set with the appropriate data types that is free of NaN values.
# The next step is to assess the trip_start_timestamp, trip_end_timestamp and trip_minutes data.


# In[70]:


# We will start by assessing the trip durations by comparing the time stamps and the durations.
# As per the Chicago Data Portal, the time stamps are rounded to the nearest 15 minutes.

start = sample_json_df['trip_start_timestamp']
end = sample_json_df['trip_end_timestamp']
start.describe()


# In[71]:


# From these statistics, it is clear that all trips obtained for the 100,000 trips sample fall within the first two months of 
# available data. Given that six months of data with a total of approximately 45.3 Million trips, the interval of the trips
# is not proportionate to the amount of trips requested for download. All data will need to be downloaded, then filtered for
# specific start and end times to obtain full dataset for desired time interval.

end.describe()


# In[72]:


# Calculation of trip duration statistics based on timestamp columns.
# As per the summary statistics below, most trips have a time stamp difference of 15 minutes.
# Greater than 75% of trips are between 7.5 and 22.5 minutes in duration.

delta=sample_json_df['trip_end_timestamp']-sample_json_df['trip_start_timestamp']
delta.describe()


# In[75]:


# The median duration matches the mean duration very closely as expected from the percentile statistics above.

delta.median()


# In[73]:


# Value Counts of the trip durations. Zero duration corresponds to trips less than 7.5 minutes in duration.

delta.value_counts().sort_values(ascending=False)


# In[74]:


# trip_minutes statistics have been generated for comparison against the durations obtained by taking the difference of the
# time stamps. The statistics for trip_minutes are aligned with the statistics for the timestamp delta

sample_json_df['trip_minutes'].describe()


# In[86]:


# omitting all trips with a time stamp delta greater that 1 hour would still retain 99.55% of the trips.

print((47308 + 20103 + 13607+3542+1031))
print((47308 + 20103 + 13607+3542+1031)/85970)

#omitting all trips with a time stamp greater than 45 minutes would still retain 98.36% of the trips. 

print((47308 + 20103 + 13607+3542))
print((47308 + 20103 + 13607+3542)/85970)

# Will make a decision regarding cutoff point when performing exploratory data analysis with complete dataset for desired
# duration. Need to further evaluate the correlation between revenue and trip length. The trips 1 hour or greater are low
# probability but the profitability needs to be assessed to see if they are more profitable on a time basis.
# These longer trips could possibly be resulting in greater tips and they keep the driver earning money rather than sitting
# idle.


# In[111]:


# There is only one zero minute trip. It has values for trip_total indicating that money was made from travelling within a
# community zone. The zero minute is likely just an error. This trip will be retained for the time being.

ZeroMinutes=sample_json_df[sample_json_df['trip_minutes'] == 0]
ZeroMinutes


# In[ ]:


# Assessment of the trip_miles is performed.


# In[105]:


# The distance of the trip needs to be assessed. If zero distance trips exist, they may need to be addressed.

sample_json_df['trip_miles'].describe()


# In[106]:


# The number of unique distances 85956 in value_counts output below indicates that trips distances are not being rounded 
# to zero

sample_json_df['trip_miles'].value_counts()


# In[108]:


# All of the zero distance trips listed below have values for trip_total indicating that money was made from travelling from 
#one community zone to another. The zero miles is likely just an error. These trips wil be retained for the time being.

ZeroDist = sample_json_df[sample_json_df['trip_miles'] == 0]
ZeroDist


# In[ ]:


# Next we perform an assessment of the revenue columns and the pooled/shared trip columns.


# In[103]:


trips = sample_json_df[['fare','tip','additional_charges', 'trip_total','shared_trip_authorized','trips_pooled']]


# In[104]:


trips.describe()


# In[115]:


ZeroFare = sample_json_df[trips['fare']==0]
ZeroFare


# In[117]:


# As per the table below, the additional_charges and tip for these trips contains data which is consistent 
# with the greater dataset.

ZeroFare[[trip_minutes','tip','additional_charges','trip_total']].describe()


# In[122]:


# As per below, the start and end timestamp values for which there zero fare trips covers the whole range of the timestamp
# occurances. The greatest frequency is 5. It may be possible to use the duration and community zone infomation to
# calculate what the fare should have been based on similar trips in the main dataset. These entries will be retained for now.

ZeroFare[['trip_start_timestamp','trip_end_timestamp']].describe()


# In[ ]:


# Summary:
# In order to get rid of NaN values, 14,030 trips were removed from the 100,00 trip dataset.
# With the remaining data, trips with zero fare, zero duration and zero distance were uncovered.
# how to treat these zero entries will be determined during the exploratory data analysis phase
# when a greater understanding of the data is obtained. Outliers may also be determined at that time as well.

