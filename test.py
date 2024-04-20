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
    all_data['tpep_pickup_datetime'] = pd.to_datetime(all_data['tpep_pickup_datetime'])
    all_data['tpep_dropoff_datetime'] = pd.to_datetime(all_data['tpep_dropoff_datetime'])
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
    laplace_columns    = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "Passenger_count", "Trip_distance", 
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

csv_path  = 'csv_files/sample.csv'
meta_path = 'small_trips.yaml'
#print("hello")


# df = pd.read_csv(csv_path)?e(df['tpep_dropoff_datetime'])
df = pd.read_csv(csv_path, nrows=100000)
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
df['drop_off'] =  df['tpep_dropoff_datetime'].dt.hour
# print(df)
df_noisy = copy_noisy_response(df)

# #print(noisy_df)

# Select z.hour, count(z.hour) FROM (SELECT EXTRACT(HOUR FROM df.TPEP_PICKUP_DATETIME) as hour FROM ONLY DECTRIPS) as Z group by z.hour order by count(z.hour) desc

# select hour, count(hour) FROM (select (extract (HOUR FROM(select tpep_pickup_time from df))) as hour) group by hour, order by count(hour) desc

nonnoise_times = []
noise_times = []
non_noise_queries = ['select pickup_hour, count(pickup_hour) FROM df group by pickup_hour order by pickup_hour',
                     'select drop_off, count(drop_off) FROM df group by drop_off order by drop_off',
                     'select PULocationID, count(PULocationID) as bye FROM df group by PULocationID order by count(PULocationID) DESC LIMIT 10',
                     'select DOLocationID, count(DOLocationID) as bye FROM df group by DOLocationID order by count(DOLocationID) DESC LIMIT 10',
                     'select Payment_type, count(Payment_type) FROM df group by Payment_type order by count(Payment_type) DESC']
noise_queries     = ['select pickup_hour, count(pickup_hour) FROM public.small_trips group by pickup_hour order by pickup_hour',
                     'select drop_off, count(drop_off) FROM public.small_trips group by drop_off order by drop_off',
                     'select PULocationID, count(PULocationID) as bye FROM public.small_trips group by PULocationID order by count(PULocationID) DESC LIMIT 10',
                     'select DOLocationID, count(DOLocationID) as bye FROM public.small_trips group by DOLocationID order by count(DOLocationID) DESC LIMIT 10',
                     'select Payment_type, count(Payment_type) FROM public.small_trips group by Payment_type order by count(Payment_type) DESC'] 
noise_dfs = []
non_noise_dfs = []
all_errors = [[], [], [], [], []]
epsilon_vals = [.25,.5,1.0,2.0,4.0,8.0,16.0,32.0,64.0,128.0]
num_queries = len(noise_queries)
all_non = []
all_times = [[], [], [], [], []]

for i in range(5):
        start_nonnoise = time.time()
        non_temp = pd.DataFrame(data = sqldf(non_noise_queries[i]))
        end_nonnoise = time.time()
        non_temp = non_temp.apply(pd.to_numeric)
        non_noise_dfs.append(non_temp)
        
        nonnoise_times.append(start_nonnoise - end_nonnoise)
        all_non.append(non_temp)

for value in epsilon_vals:
    privacy = Privacy(epsilon=value, delta=0.5)
    error_rate    = []
    noise_times   = []
    #print(f"Trying with epsilon: {value}")
    for i in range(num_queries):
        reader = snsql.from_df(df_noisy, privacy=privacy, metadata=meta_path)
        start_noise = time.time()
        #print(noise_queries[i])
        temp = pd.DataFrame(data=reader.execute(noise_queries[i]))
        # print(f'temp b4 clip is {temp}')
        end_noise = time.time()
        temp = temp.drop([0])
        temp = temp.apply(pd.to_numeric)
        noise_dfs.append(temp)
        
        all_times[i].append(end_noise - start_noise)
        max_noise = float(temp.max().max())
        max_non   = float(all_non[i].max().max())
        #print(f"noise max is: {max_noise} , non_max is: {max_non}")
        all_errors[i].append(abs(((max_noise)-(max_non)) / (max_non)))
        #print("current error is", (((max_noise)-(max_non))))
    #all_times.append(noise_times)
    #all_errors.append(error_rate)
    
titles = ['Top Pickup Times Difference',
          'Pickup Times Error',
          'Top Dropoff Times Difference',
          'Dropoff Times Error',
          'Frequency of NYC Taxi Zones as Pickup Locations Difference', 
          'Frequency of NYC Taxi Zones as Pickup Locations Error',
          'Frequency of NYC Taxi Zones as Dropoff Locations Difference',
          'Frequency of NYC Taxi Zones as Dropoff Locations Error',
          'Frequency of Payment Types for NYC Taxis Difference',
          'Frequency of Payment Types for NYC Taxis Error']
k = 0
m = 0
loops = len(titles)
string_vals = [str(x) for x in epsilon_vals]
for j in range(loops):
    
    if j % 2 != 0:
        curr_list = all_times[k]
        file = 'error' + str(k) + '.png'
        k += 1
        #print('doing time')
    else:
        curr_list = all_errors[m]
        file = 'time' + str(m) + '.png'
        m += 1
        #print('error mode')
    # print(f'j is {j}, k is {k}, m is {m}')
    # x = ((ndf.iloc[:,0]).astype(object)).astype(str)
    fig, ax = plt.subplots(figsize = (8,8))
    #
    #print((noise_dfs[j]))
    #print(curr_list)
    ax.bar(string_vals, curr_list, width= .8)
    ax.xaxis.set_tick_params(pad = 5)
    ax.yaxis.set_tick_params(pad = 10)
    ax.autoscale(enable=True)
    ax.set_title(titles[j],loc ='center' )

    plt.savefig(file)
# print(f'non_noise 4 is {non_noise_dfs[4]}')
# print(f'noise 4 is {noise_dfs[4]}')

'''
Calc Error 
'''

