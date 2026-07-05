import pandas as pd

usecols = ['Class']
df = pd.read_csv('data/source/creditcard.csv', usecols=usecols)
df['transaction_id'] = df.index.map(lambda i: f"txn_{i}")
df = df[['transaction_id','Class']]
df = df.rename(columns={'Class':'label'})


df.to_csv('data/source/labels.csv', index=False)