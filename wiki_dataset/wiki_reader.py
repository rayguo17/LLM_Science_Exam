import pandas as pd
import os


path = "./wiki_dataset/wiki_parquet"
import chromadb
chroma_client = chromadb.PersistentClient("./wiki_dataset/chroma.db")
collection = chroma_client.create_collection('wiki_embedding')
dir_list = os.listdir(path)
for file_name in dir_list:
    df = pd.read_parquet(os.path.join(path,file_name),engine="pyarrow")
    print(df.head())
    ids =df['index'].apply(lambda x: str(x)).to_list()
    meta_list = list(df.filter(['title','index']).T.to_dict().values())
    article_list =  df['article'].to_list()
    collection.add(
        ids=ids,
        documents=article_list,
        metadatas=meta_list
    )
print(len(dir_list))
# df = pd.read_parquet('./wiki_dataset/wiki_parquet/00000010.parquet',engine='pyarrow')
# print(df.head())

