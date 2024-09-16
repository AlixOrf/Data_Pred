import pandas as pd 
import pyarrow as py

df_parquet = pd.read_parquet('Ressources/weather.parquet')

print(df_parquet)