#!/usr/local/bin/python3

# for loading cell_line
import regex as re
import sqlite3 as sql
import pandas as pd

# create database in the base final project folder and connect
conn = sql.connect('C:/Users/tuann/OneDrive - Johns Hopkins/School/AS.410.712 (Advanced Practical Computer Concepts for Bioinformatics/Final Project/base/test.db')
# conn = sql.connect('final.db')
curs = conn.cursor()

# queries for creating the tables in the db
qry_create_drug_table = """
    CREATE TABLE drug (
        drug_id varchar(200),
        drug_name varchar(200)
    );
"""
qry_create_drug_alt_table = """
    CREATE TABLE drug_name_alt (
        drug_id varchar(200),
        drug_name_alt varchar(200)
    );
"""
qry_create_cell_line_table = """
    CREATE TABLE cell_line (
        cell_line_id varchar(200),
        cell_line_name varchar(200),
        tissue_desc varchar(200)
    );
"""
qry_create_pathway_table = """
    CREATE TABLE pathway (
        path_id varchar(200),
        target_id varchar(200),
        drug_target varchar(200),
        drug_target_pathway varchar(200),
        ic_50 varchar(200)
    );
"""
qry_create_keys_table = """
    CREATE TABLE keys (
        entry_num varchar(200),
        drug_id varchar(200),
        cell_line_id varchar(200),
        path_id varchar(200)
    );
"""
curs.execute(qry_create_drug_table)
curs.execute(qry_create_drug_alt_table)
curs.execute(qry_create_cell_line_table)
curs.execute(qry_create_pathway_table)
curs.execute(qry_create_keys_table)

# read in the data from CSV/xlsx
# GDSC 1/2: cell line name, drug name, drug target, pathway, IC50
# df = pd.read_excel("C:\\Users\\tuann\\OneDrive - Johns Hopkins\\School\\AS.410.712 (Advanced Practical Computer Concepts for Bioinformatics\\Final Project\\base\\data\\test_file.xlsx")
df_GDSC1 = pd.read_excel("C:\\Users\\tuann\\OneDrive - Johns Hopkins\\School\\AS.410.712 (Advanced Practical Computer Concepts for Bioinformatics\\Final Project\\base\\data\\GDSC1_fitted_dose_response_24Jul22.xlsx", usecols = ['CELL_LINE_NAME','DRUG_NAME','PUTATIVE_TARGET','PATHWAY_NAME','LN_IC50'])
# df_GDSC2 = pd.read_excel("C:\\Users\\tuann\\OneDrive - Johns Hopkins\\School\\AS.410.712 (Advanced Practical Computer Concepts for Bioinformatics\\Final Project\\base\\data\\GDSC2_fitted_dose_response_24Jul22.xlsx", usecols = ['CELL_LINE_NAME','DRUG_NAME','PUTATIVE_TARGET','PATHWAY_NAME','LN_IC50'])
df_drug_names = pd.read_csv("C:\\Users\\tuann\\OneDrive - Johns Hopkins\\School\\AS.410.712 (Advanced Practical Computer Concepts for Bioinformatics\\Final Project\\base\\data\\screened_compounds_rel_8.4.csv", usecols = ['DRUG_NAME','SYNONYMS'])

print(df_GDSC1.head())
print(df_drug_names.head())
print(df_drug_names)
    
# split the alternate drug names by commas
# df_drug_names['SYNONYMS'] = df_drug_names['SYNONYMS'].str.split(',')
x = df_drug_names['SYNONYMS'].str.split(',')
print(x)

counter = 0
row_num = []
for row in x:
    if counter < 5:
        row_num.append(row.index)
        print(row_num)
        counter += 1


    

# merge the df
df_GDSC1 = pd.merge(df_GDSC1, df_drug_names, on='DRUG_NAME', how='left')

# print(df_GDSC1['SYNONYMS'])
# print(df_GDSC1)

# drop the dupes from the cols
drop_dupe_name = df_GDSC1['DRUG_NAME'].drop_duplicates()
drop_dupe_id = df_GDSC1['drug_id'].drop_duplicates()

# Add entry_id as a new column with unique values
df_GDSC1['entry_num'] = range(1, len(df_GDSC1) + 1)
# df_GDSC2['entry_num'] = range(1, len(df_GDSC2) + 1)  
#print(df_GDSC1[:10])

# add drug id for each unique drug in drug column
df_GDSC1['drug_id'] = df_GDSC1.groupby('DRUG_NAME').ngroup()
# df_GDSC2['drug_id'] = df_GDSC2.groupby('DRUG_NAME').ngroup()
df_drug_names['drug_id'] = df_drug_names.groupby('DRUG_NAME').ngroup()

# sort values by drug id
df_GDSC1_sort = df_GDSC1.sort_values(by=['drug_id'])
# df_GDSC2_sort = df_GDSC2.sort_values(by=['drug_id'])
#print(df_GDSC2_sort)
df_name_sort = df_drug_names.sort_values(by=['drug_id'])
#print(df_name_sort)

# add path_id for each unique pathway in pathway column
df_GDSC1['path_id'] = df_GDSC1.groupby('PATHWAY_NAME').ngroup()
# df_GDSC2['path_id'] = df_GDSC2.groupby('PATHWAY_NAME').ngroup()

# add cell_line_id for each unique cell line in cell line column
df_GDSC1['cell_line_id'] = df_GDSC1.groupby('CELL_LINE_NAME').ngroup()
# df_GDSC2['cell_line_id'] = df_GDSC2.groupby('CELL_LINE_NAME').ngroup()

# add targete_id for each unique target in target column
df_GDSC1['target_id'] = df_GDSC1.groupby('PUTATIVE_TARGET').ngroup()
# df_GDSC2['target_id'] = df_GDSC2.groupby('PUTATIVE_TARGET').ngroup()

# Insert data into the drug table
df_drug = df_GDSC1[['drug_id', 'drug_name', 'drug_name_alt']].drop_duplicates()
df_drug.to_sql('drug', conn, if_exists='append', index=False)

# Insert data into the cell_line table
df_cell_line = df_GDSC1[['cell_line_id', 'cell_line_name', 'tissue_desc']].drop_duplicates()
df_cell_line.to_sql('cell_line', conn, if_exists='append', index=False)

# Insert data into the pathway and keys tables
df_pathway = df_GDSC1[['path_id', 'target_id', 'drug_target', 'drug_target_pathway', 'ic_50']]
df_pathway.to_sql('pathway', conn, if_exists='append', index=False)
df_keys = df_GDSC1[['entry_num', 'drug_id', 'cell_line_id', 'path_id']]
df_keys.to_sql('keys', conn, if_exists='append', index=False)

# Iterate over the rows of the df_drug_names DataFrame
for index, row in df_drug_names.iterrows():
    # Insert the drug name into the drug table
    qry_insert_drug = f"INSERT INTO drug (drug_name) VALUES ('{row['DRUG_NAME']}')"
    conn.execute(qry_insert_drug)
    drug_id = row['drug_id']
    
    # Insert each synonym into the drug_name_alt table
    for synonym in row['SYNONYMS']:
        qry_insert_alt = f"INSERT INTO drug_name_alt (drug_id, drug_name_alt) VALUES ({drug_id}, '{synonym.strip()}')"
        conn.execute(qry_insert_alt)

#iterate through the entry_num column of the df and insert into db either null if blank or the entry#
qry_insert_GDSC1 = """
    INSERT INTO keys (entry_num)
    VALUES (?)

"""

for entry in df_GDSC1['entry_num']:
    if entry == 'NaN' or '':
        curs.execute(qry_insert_GDSC1, 'NULL')
    else:
        curs.execute(qry_insert_GDSC1, [entry])

conn.commit()
curs.close()
conn.close()

    
    
# sql query for html
qry_select = """
SELECT p.drug_target, p.drug_target_pathway, p.ic_50, c.cell_line_name, c.tissue_desc
FROM pathway p
    JOIN keys k ON p.path_id=k.path_id
    JOIN cell_line c ON k.cell_line_id=c.cell_line_id
    JOIN drug d ON k.drug_id=d.drug_id
WHERE d.drug_name LIKE %s;
"""    
