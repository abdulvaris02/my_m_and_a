import pandas as pd
import numpy as np
import sqlite3
import re
import my_ds_babel
import warnings
warnings.filterwarnings('ignore')


def clean_data_one(data):
  df = pd.read_csv(data, sep=',')
  df.Country = 'USA'
  df.Gender = df.Gender.replace({'0':"Female", '1':'Male','F':"Female", 'M':'Male'})
  df.FirstName = df.FirstName.str.replace(r'[^0-9a-zA-Z:,]+', '').str.title()
  df.LastName = df.LastName.str.replace(r'[^0-9a-zA-Z:,]+', '').str.title()
  df.Email = df.Email.str.lower()
  df.City = df.City.str.replace(r'[^0-9a-zA-Z:,]+', ' ').str.title()
  df.drop(['UserName'], axis=1, inplace=True)
  return df

def clean_data_two(data):
  column_name=['Age','City','Gender','Full_name', 'Email']
  df = pd.read_csv(data, sep=';', names=column_name)
  df['Country'] = 'USA'
  df.Gender = df.Gender.replace({'F':"Female", 'M':'Male','0':"Female", '1':'Male'})
  df[['FirstName', 'LastName']] = df.Full_name.str.split(expand=True)
  df.drop(['Full_name'], axis=1, inplace=True)
  df.FirstName = df.FirstName.str.replace(r'[^0-9a-zA-Z:,]+', '').str.title()
  df.LastName = df.LastName.str.replace(r'[^0-9a-zA-Z:,]+', '').str.title()
  df.Email = df.Email.str.lower()
  df.City = df.City.str.replace(r'[^0-9a-zA-Z:,]+', ' ').str.title()
  return df


def clean_data_three(data):
  df = pd.read_csv(data, sep='\t|,', engine='python')
  df = df.replace({'string_':'','boolean_':'','integer_':'','years':'','character_':''}, regex=True)

  df['Country'] = 'USA'
  df.Gender = df.Gender.replace({'F':"Female", 'M':'Male','0':"Female", '1':'Male'})
  df[['FirstName', 'LastName']] = df.Name.str.split(expand=True)
  df.drop(['Name'], axis=1, inplace=True)
  df.FirstName = df.FirstName.str.replace(r'[^0-9a-zA-Z:,]+', '').str.title()
  df.LastName = df.LastName.str.replace(r'[^0-9a-zA-Z:,]+', '').str.title()
  df.Email = df.Email.str.lower()
  df.City = df.City.str.replace(r'[^0-9a-zA-Z:,]+', ' ').str.title()
  df.Age = df.Age.str.replace(r'\D+', '').str.title()
  return df

def join_three(df1, df2, df3):
  df = pd.concat([df1, df2, df3], ignore_index=True)
  return df

def csv_to_sql(df, database, table_name):
    conn = sqlite3.connect(database)
    # df = pd.read_csv(csv_content)
    df = df.drop_duplicates()
    df.to_sql(table_name, conn, if_exists='append', index=False)

def my_m_and_a(df1, df2, df3):
    df1c = clean_data_one(df1)
    df2c = clean_data_two(df2)
    df3c = clean_data_three(df3)
    merged_csv = join_three(df1c, df2c, df3c)
    merged_csv = merged_csv[['Gender', 'FirstName', 'LastName','Email','Age','City','Country']]
    merged_csv['FirstName'] = merged_csv['FirstName'].astype('str')
    merged_csv['LastName'] = merged_csv['LastName'].astype('str')
    merged_csv['Age'] = merged_csv['Age'].astype('str')
    return merged_csv
