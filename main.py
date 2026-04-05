import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv

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
print('Data cleaned successfully')

load_dotenv()

df = pd.read_csv("cleaned_data.csv")
df["ORDERDATE"] = pd.to_datetime(df["ORDERDATE"]).dt.date
df = df.where(pd.notnull(df), None)


conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT")
)
cur = conn.cursor()
row = df.iloc[0]

cur.execute("""
INSERT INTO data_table (
    ordernumber, quantityordered, priceeach, orderlinenumber,
    sales, orderdate, status, qtr_id, month_id, year_id, productline, msrp,
    productcode, customername, phone,
    addressline1, addressline2, city,
    state, postalcode, country,
    territory, contactlastname,
    contactfirstname, dealsize
)
VALUES (100, 110, 60.6, 105, 50.5, '2026-10-05', 'good', 'good', 'good', 'good', 'good', 45, 'good', 'good', 'good', 'good', 'good', 'good', 'good', 'good', 'good', 'good', 'good', 'good','good')
""", (
    row["ORDERNUMBER"],
    row['QUANTITYORDERED'],
    row['PRICEEACH'],
    row['ORDERLINENUMBER'],
    row["SALES"],
    row["ORDERDATE"],
    row['STATUS'],
    row['QTR_ID'],
    row['MONTH_ID'],
    row['YEAR_ID'],
    row['PRODUCTLINE'],
    row['MSRP'],
    row["PRODUCTCODE"],
    row["CUSTOMERNAME"],
    row["PHONE"],
    row["ADDRESSLINE1"],
    row["ADDRESSLINE2"],
    row["CITY"],
    row["STATE"],
    row["POSTALCODE"],
    row["COUNTRY"],
    row["TERRITORY"],
    row["CONTACTLASTNAME"],
    row["CONTACTFIRSTNAME"],
    row["DEALSIZE"]
))
print("Connected successfully")
conn.commit()
cur.close()
conn.close()