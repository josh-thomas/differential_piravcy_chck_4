import snsql, os
from snsql import Privacy
import pandas as pd

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
        # df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')
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
def noisy_dataframe(df, meta_path):
    reader = snsql.from_df(df, privacy=privacy, metadata=meta_path)
    laplace_columns    = ["passenger_count", "trip_distance", 
                          "PULocationID", "DOLocationID", "fare_amount", "extra", "tip_amount", "tolls_amount"]
    df_noisy           = df.copy(deep=True)
    for column in laplace_columns:
        query              = 'SELECT %s, COUNT(*) FROM MySchema.MyTable GROUP BY %s' % (column, column)
        noisy_laplace_data = reader.execute(query)
        noisy_laplace_data = noisy_laplace_data[1:]
        df_noisy[column]   = [row[0] for row in noisy_laplace_data]
    
    # Recalculate Summary Statsitics from noisy data

    
    return df_noisy

privacy = Privacy(epsilon=1.0, delta=0.1)

csv_path  = 'csv_files'
meta_path = 'lib/schema.yaml'
print("hello")

df = read_files(csv_path)
print(df)


noisy_df = noisy_dataframe(df, meta_path)

print(noisy_df)
