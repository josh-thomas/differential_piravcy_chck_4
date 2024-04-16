import snsql, os
from snsql import Privacy
from pandasql import sqldf
from random_test import *
import pandas as pd
import string
import matplotlib.pyplot as plt
import time

'''
read_files

Input: String of folder path
Returns: Pandas Dataframe
Description: Takes a folder path as a string. Opens all csv files in the folder
             and reads them into dataframes. Then returns one dataframe of all the
             information concatenated together.
'''
def read_files(path):
    frames   = []
    for file_name in os.listdir(path):
        file    = os.path.join(path, file_name)
        print(file)
        df = pd.read_csv(file)
        frames.append(df)
    all_data = pd.concat(frames, ignore_index=True)
    return all_data

'''
noisy_dataframe

Input:         Pandas DataFrame
Returns:       Pandas DataFame
Description:   This function take a dataframe of infromation for trusted sources. It creates
               a new dataframe with added noise both Laplace and radmonized selection to the
               data given
'''
def noisy_dataframe(df):
    reader = snsql.from_df(df, privacy=privacy, metadata=meta_path)
    laplace_columns    = ["Passenger_count", "Trip_distance", 
                          "PULocationID", "DOLocationID", "Fare_amount", "Extra", "Tip_amount", "Tolls_amount"]
    df_noisy           = df.copy(deep=True)
    for column in laplace_columns:
        query              = f"SELECT {column}, row_id FROM public.small_trips group by {column}, row_id order by row_id"
        # print(query)
        noisy_laplace_data = reader.execute(query)
        #print(reader.get_privacy_cost(query))
        #print(noisy_laplace_data)
        noisy_laplace_data = noisy_laplace_data[1:]
        # print(df["passenger_count"])
        df_noisy[column] = [row[0] for row in noisy_laplace_data]

    
    # Recalculate Summary Statsitics from noisy data
    noisy_response(df_noisy)
    return df_noisy

privacy = Privacy(epsilon=1.41, delta=0.1)

csv_path  = 'small_trips.csv'
meta_path = 'small_trips.yaml'
#print("hello")

df = pd.read_csv(csv_path, nrows=100000)
#print(df)
df_noisy = copy_noisy_response(df)

# #print(noisy_df)
nonnoise_times = []
noise_times = []
non_noise_queries = ['select PULocationID, count(PULocationID) as bye FROM df group by PULocationID order by count(PULocationID) DESC LIMIT 10',
                     'select DOLocationID, count(DOLocationID) as bye FROM df group by DOLocationID order by count(DOLocationID) DESC LIMIT 10',
                     'select Payment_type, count(Payment_type) FROM df group by Payment_type order by count(Payment_type) DESC', 
                     'select avg(Trip_distance) FROM df', 'select avg(Fare_amount) FROM df']
noise_queries= ['select PULocationID, count(PULocationID) as bye FROM public.small_trips group by PULocationID order by count(PULocationID) DESC LIMIT 10',
                     'select DOLocationID, count(DOLocationID) as bye FROM public.small_trips group by DOLocationID order by count(DOLocationID) DESC LIMIT 10',
                     'select Payment_type, count(Payment_type) FROM public.small_trips group by Payment_type order by count(Payment_type) DESC', 
                     'select avg(trip_distance) FROM public.small_trips', 'select avg(Fare_amount) FROM public.small_trips'] 
noise_dfs = []
non_noise_dfs = []
for i in range(5):
    start_nonnoise = time.time()
    non_noise_dfs.append(pd.DataFrame(data = sqldf(non_noise_queries[i])))
    end_nonnoise = time.time()
    nonnoise_times.append(start_nonnoise - end_nonnoise)
    reader = snsql.from_df(df_noisy, privacy=privacy, metadata=meta_path)
    start_noise = time.time()
    #print(noise_queries[i])
    noise_dfs.append(pd.DataFrame(data=reader.execute(noise_queries[i])))
    end_noise = time.time()
    noise_times.append(start_noise - end_noise)

titles = ['Frequency of NYC Taxi Zones as Pickup Locations', 
       'Noisy Frequency of NYC Taxi Zones as Pickup Locations',
       'Frequency of NYC Taxi Zones as Dropoff Locations',
       'Noisy Frequency of NYC Taxi Zones as Dropoff Locations',
       'Frequency of Payment Types for NYC Taxis',
       'Noisy Frequency of Payment Types for NYC Taxis']
k = 0
m = 0
for j in range(5):
    
    if j % 2 != 0:
        ndf = noise_dfs[k]
        file = 'noise' + str(k) + '.png'
        k += 1
    else:
        ndf = non_noise_dfs[m]
        file = 'no_noise' + str(m) + '.png'
        m += 1
    print(f'j is {j}, k is {k}, m is {m}')
    x = ((ndf.iloc[:,0]).astype(object)).astype(str)
    fig, ax = plt.subplots(figsize = (8,8))
    print(ndf)
    print((noise_dfs[j]))
    ax.bar(x, ndf.iloc[:,1], width= .8)
    ax.xaxis.set_tick_params(pad = 5)
    ax.yaxis.set_tick_params(pad = 10)
    ax.autoscale(enable=True)
    ax.set_title(titles[j],loc ='center' )

    plt.savefig(file)

# print(results2.dtypes)
# results1.plot(kind = 'hist')
# results2.plot(kind = 'hist')