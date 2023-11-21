import pandas as pd

df = pd.read_parquet('./wiki_dataset/wiki_parquet/00000010.parquet',engine='pyarrow')
print(df.head())

