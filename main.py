import pandas as pd
df = pd.read_csv(r'F:\self-projects\data_pipeline\data.csv', encoding='cp1252')
print(df.head())


print(df.describe())

print(df.info())

print(df.isnull().sum())

cols = ['ADDRESSLINE2', 'STATE', 'TERRITORY']
df[cols] = df[cols].fillna(0)
df = df.dropna()
print(df.isnull().sum())
print(df.describe())

df = df.drop_duplicates(subset='ORDERNUMBER')
print(df.describe())

df.to_csv('F:/self-projects/data_pipeline/cleaned_data.csv', index=False)
print(df.shape)
print('Data leaned successfully')