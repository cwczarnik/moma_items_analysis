import sqlite3
import pandas as pd

conn = sqlite3.connect('moma.db')

moma_iter = pd.read_csv('moma.csv',chunksize=1000)

for chunk in moma_iter:
    chunk.to_sql('exhibitions', conn,if_exists = 'append',index=False)

results_df = pd.read_sql('PRAGMA table_info(exhibitions)',conn)
print(results_df)

df = pd.read_csv('moma.csv')
moma_iter = pd.read_csv('moma.csv',chunksize=1000)

for chunk in moma_iter:
    chunk['ExhibitionSortOrder'] = pd.to_numeric(chunk['ExhibitionSortOrder'])
    chunk.to_sql('exhibitions', conn,if_exists = 'append',index=False)
    
    
results_df = pd.read_sql('PRAGMA table_info(exhibitions)',conn)
print(results_df)

conn = sqlite3.connect('moma.db')

eid_counts = pd.read_sql('''select exhibitionID, count(*) as counts from exhibitions group by 1 order by 2 desc;''', conn)


exhibition_df = pd.read_sql('select exhibitionID from exhibitions;', conn)
eid_pandas_counts = exhibition_df['ExhibitionID'].value_counts()
print(eid_pandas_counts[:10])


conn = sqlite3.connect('moma.db')

q = 'select exhibitionid from exhibitions;'
chunk_iter = pd.read_sql(q, conn, chunksize=100)
array =[]

moma_iter = pd.read_csv('moma.csv',chunksize=1000)

for chunk in moma_iter:
    chunk.to_sql('exhibitions', conn,if_exists = 'append',index=False)
    
    
for chunk in chunk_iter:
    array.append(chunk['ExhibitionID'].value_counts())
    
final_df = pd.concat(array)

output=final_df.groupby(final_df.index).sum()