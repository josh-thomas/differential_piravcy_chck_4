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
    laplace_columns    = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "Passenger_count", "Trip_distance", 
                          "PULocationID", "DOLocationID", "Fare_amount", "Extra", "Tip_amount", "Tolls_amount"]
    df_noisy           = df.copy(deep=True)
    for column in laplace_columns:
        query              = f"SELECT {column} FROM {df}"
        noisy_laplace_data = reader.execute(query)
        df_noisy[column]   = noisy_laplace_data
    
    # Recalculate Summary Statsitics from noisy data
    
    return df_noisy

privacy = Privacy(epsilon=1.0, delta=0.1)

csv_path  = 'csv_files'
meta_path = ''
print("hello")

df = read_files(csv_path)
print(df)


noisy_df = noisy_dataframe(df)

print(noisy_df)