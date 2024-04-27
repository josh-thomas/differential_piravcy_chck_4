import snsql
from snsql import Privacy
from pandasql import sqldf
from random_test import *
import pandas as pd
import matplotlib.pyplot as plt
import time
import random
import numpy as np


privacy = Privacy(epsilon=1.41, delta=0.1)

csv_path  = 'csv_files/sample.csv'
meta_path = 'small_trips.yaml'


df = pd.read_csv(csv_path, nrows=100000)
df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour
df['drop_off'] =  df['tpep_dropoff_datetime'].dt.hour
df_noisy = copy_noisy_response(df)

nonnoise_times = []
noise_times = []
# non_noise_queries = ['select pickup_hour, count(pickup_hour) FROM df group by pickup_hour order by pickup_hour',
#                      'select drop_off, count(drop_off) FROM df group by drop_off order by drop_off',
#                      'select PULocationID, count(PULocationID) as bye FROM df group by PULocationID order by count(PULocationID) DESC LIMIT 10',
#                      'select DOLocationID, count(DOLocationID) as bye FROM df group by DOLocationID order by count(DOLocationID) DESC LIMIT 10',
#                      'select Payment_type, count(Payment_type) FROM df group by Payment_type order by count(Payment_type) DESC']
# noise_queries     = ['select pickup_hour, count(pickup_hour) FROM public.small_trips group by pickup_hour order by pickup_hour',
#                      'select drop_off, count(drop_off) FROM public.small_trips group by drop_off order by drop_off',
#                      'select PULocationID, count(PULocationID) as bye FROM public.small_trips group by PULocationID order by count(PULocationID) DESC LIMIT 10',
#                      'select DOLocationID, count(DOLocationID) as bye FROM public.small_trips group by DOLocationID order by count(DOLocationID) DESC LIMIT 10',
#                      'select Payment_type, count(Payment_type) FROM public.small_trips group by Payment_type order by count(Payment_type) DESC'] 

# non_noise_queries = ['select count(*) FROM public']
noise_queries     = ['select count(*) FROM public.small_trips', 'select count(*) FROM public.small_trips WHERE PULocationID = 1 ']

noise_dfs = []
non_noise_dfs = []
all_errors = [[], [], [], [], []]
# epsilon_vals = [1.4]
num_queries = len(noise_queries)
all_non = []
pre_times = []
fix_all = []
fix_one = []
debug_all =[]
debug_one = []
# for i in range(5):
#         start_nonnoise = time.time()
#         non_temp = pd.DataFrame(data = sqldf(non_noise_queries[i]))
#         end_nonnoise = time.time()
#         non_temp = non_temp.apply(pd.to_numeric)
#         non_noise_dfs.append(non_temp)
        
#         nonnoise_times.append(start_nonnoise - end_nonnoise)
#         all_non.append(non_temp)

# for value in epsilon_vals:
#     privacy = Privacy(epsilon=value, delta=0.5)
#     error_rate    = []
#     noise_times   = []
#     #print(f"Trying with epsilon: {value}")
#     for i in range(num_queries):
#         reader = snsql.from_df(df_noisy, privacy=privacy, metadata=meta_path)
#         start_noise = time.time()
#         #print(noise_queries[i])
#         temp = pd.DataFrame(data=reader.execute(noise_queries[i]))
#         # print(f'temp b4 clip is {temp}')
#         end_noise = time.time()
#         temp = temp.drop([0])
#         temp = temp.apply(pd.to_numeric)
#         noise_dfs.append(temp)
        
#         all_times[i].append(end_noise - start_noise)
#         max_noise = float(temp.max().max())
#         max_non   = float(all_non[i].max().max())
#         #print(f"noise max is: {max_noise} , non_max is: {max_non}")
#         all_errors[i].append(abs(((max_noise)-(max_non)) / (max_non)))
#         #print("current error is", (((max_noise)-(max_non))))
#     #all_times.append(noise_times)
#     #all_errors.append(error_rate)


#Pre fix
for i in range(num_queries):
    reader = snsql.from_df(df_noisy, privacy=privacy, metadata=meta_path)
    start_noise = time.time()
    #print(noise_queries[i])
    temp = pd.DataFrame(data=reader.execute(noise_queries[i]))
    # print(f'temp b4 clip is {temp}')
    end_noise = time.time()
    total_time = end_noise-start_noise
    #wait_time = np.random.laplace(loc=total_time, scale=1, size=1)
    temp = temp.drop([0])
    temp = temp.apply(pd.to_numeric)
    noise_dfs.append(temp)
    pre_times.append(total_time)
    # all_times.append(abs(wait_time[0]))
    # time.sleep(float(abs(wait_time-total_time)))
    
    # max_noise = float(temp.max().max())
    # max_non   = float(all_non[i].max().max())
    #print(f"noise max is: {max_noise} , non_max is: {max_non}")
    # all_errors[i].append(abs(((max_noise)-(max_non)) / (max_non)))

#Post Fix
for k in range(10):
    #temp_times = []
    for i in range(num_queries):
        print(f'i is: {i}')
        reader = snsql.from_df(df_noisy, privacy=privacy, metadata=meta_path)
        start_noise = time.time()
        #print(noise_queries[i])
        temp = pd.DataFrame(data=reader.execute(noise_queries[i]))
        # print(f'temp b4 clip is {temp}')
        end_noise = time.time()
        total_time = end_noise-start_noise
        wait_time = np.random.laplace(loc=0, scale = 0.01, size=1)
        sleep_time = abs(wait_time[0])
        #sleep_time = float(abs(abs(wait_time[0]) - total_time))
        
        #randi = random.randint(1, 5)
        #if sleep_time > (total_time * 5):
        #    sleep_time = total_time * randi
        #print(f'Sleep time is: {sleep_time}')
        #print(f'Total time is: {total_time}')
        # all_times.append(abs(wait_time[0]))
        time.sleep(sleep_time)
        final_noise = time.time()
        final_time = final_noise - start_noise
        #print(f'b4 time is: {total_time}')
        #print(f'Final time is: {final_time}')

        if i == 0: 
            fix_all.append(final_time)
            debug_all.append(('i is ' + str(i)))
        else:
            fix_one.append(final_time)
            debug_one.append(('i is ' +  str(i)))
        temp = temp.drop([0])
        temp = temp.apply(pd.to_numeric)
        noise_dfs.append(temp)
    #fix_times.append(temp_times)

title = ['Time Differences Between All and Pickup 1 Without Noise', 
        'Time Differences Between All and Pickup 1 With Noise' ]
        #   Top Pickup Times Difference',
        #   'Pickup Times Error',
        #   'Top Dropoff Times Difference',
        #   'Dropoff Times Error',
        #   'Frequency of NYC Taxi Zones as Pickup Locations Difference', 
        #   'Frequency of NYC Taxi Zones as Pickup Locations Error',
        #   'Frequency of NYC Taxi Zones as Dropoff Locations Difference',
        #   'Frequency of NYC Taxi Zones as Dropoff Locations Error',
        #   'Frequency of Payment Types for NYC Taxis Difference',
        #   'Frequency of Payment Types for NYC Taxis Error']
k = 0
m = 0
string_vals = ['All', 'Pickup 1']


fig, ax = plt.subplots(figsize = (8,8))
#
#print((noise_dfs[j]))
#print(curr_list)
ax.bar(string_vals, pre_times, width= .8)
ax.xaxis.set_tick_params(pad = 5)
ax.yaxis.set_tick_params(pad = 10)
ax.autoscale(enable=True)
ax.set_title(title[0],loc ='center' )

plt.savefig('diff_before_fix.png')

# fig, ax = plt.subplots(figsize = (8,8))
# #
# #print((noise_dfs[j]))
# #print(curr_list)
# ax.bar(string_vals, fix_times, width= .8)
# ax.xaxis.set_tick_params(pad = 5)
# ax.yaxis.set_tick_params(pad = 10)
# ax.set_title(title[1],loc ='center' )

# plt.savefig('diff_after_fix.png')

print(debug_all)
print(debug_one)

index = np.arange(10)
bar_width = 0.35

fig, ax = plt.subplots()
all = ax.bar(index, fix_one, bar_width, label="All", color = '#3E8EDE')

zone_1 = ax.bar(index+bar_width, fix_all, bar_width, label="Zone 1", color = '#512C1D')

ax.set_xlabel('Query')
ax.set_ylabel('Time in Seconds')
ax.set_title('Time Differences Between All and Pickup 1 With Noise')
ax.set_xticks(index + bar_width / 2)
ax.set_xticklabels(['Trial ' + str(x + 1) for x in range (10)])
ax.legend()

plt.savefig('diff_after_fix.png')
# loops = len(titles)
# string_vals = [str(x) for x in epsilon_vals]
# for j in range(loops):
    
#     if j % 2 != 0:
#         curr_list = all_times[k]
#         file = 'error' + str(k) + '.png'
#         k += 1
#         #print('doing time')
#     else:
#         curr_list = all_errors[m]
#         file = 'time' + str(m) + '.png'
#         m += 1
#         #print('error mode')
#     # print(f'j is {j}, k is {k}, m is {m}')
#     # x = ((ndf.iloc[:,0]).astype(object)).astype(str)
#     fig, ax = plt.subplots(figsize = (8,8))
#     #
#     #print((noise_dfs[j]))
#     #print(curr_list)
#     ax.bar(string_vals, curr_list, width= .8)
#     ax.xaxis.set_tick_params(pad = 5)
#     ax.yaxis.set_tick_params(pad = 10)
#     ax.autoscale(enable=True)
#     ax.set_title(titles[j],loc ='center' )

#     plt.savefig(file)
# print(f'non_noise 4 is {non_noise_dfs[4]}')
# print(f'noise 4 is {noise_dfs[4]}')

