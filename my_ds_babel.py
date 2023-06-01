import pandas as pd
import numpy as np
import sqlite3
import csv


def sql_to_csv(database, table_name):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")

    with open("out_r.csv", 'w',newline='') as csv_file: 
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([i[0] for i in cursor.description]) 
        csv_writer.writerows(cursor)
    conn.close()
    with open('out_r.csv') as file:
        df = file.read()
    return df[:-1]

def csv_to_sql(csv_content, database, table_name):
    conn = sqlite3.connect(database)
    df = pd.read_csv(csv_content)
    # df = df.drop_duplicates()
    df.to_sql(table_name, conn, if_exists='append', index=False)

# def csv_to_sql(csv_content, database, table_name):
#     conn = sqlite3.connect(database)
#     cursor = conn.cursor()
#     with open(csv_content, 'r') as file:
#         for row in file:
#             cursor.execute(f'INSERT INTO {table_name} VALUES (?,?,?,?,?,?)', row.split(','))
#             conn.commit()
#     conn.close()

def main():
    print(sql_to_csv('all_fault_line.db','fault_lines'))
    csv_content = open("list_volcano.csv")
    csv_to_sql(csv_content, 'list_volcanos.db','volcanos')


if __name__ == '__main__':
    main()



    
import sqlite3
import pandas as pd
import csv

def sql_to_csv(database, table_name):
    conn = sqlite3.connect(database)
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")

    with open("out_r.csv", 'w',newline='') as csv_file: 
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([i[0] for i in cursor.description]) 
        csv_writer.writerows(cursor)
    conn.close()
    with open('out_r.csv') as file:
        df = file.read()
    return df[:-1]
