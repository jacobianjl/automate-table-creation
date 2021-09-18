from yaml.error import YAMLError
import pyodbc
import yaml
import pandas as pd

# List of dataframes to upload
df_list = []

# List of table names 
table_names = []

cf_path = 'config.yaml'

with open(cf_path, 'r') as cfyml:
    try:
        cf = yaml.safe_load(cfyml)
        print(cf)
    except yaml.YAMLError as exc:
        print(exc)

_driver = cf.get('driver')
_server = cf.get('server')
_database = cf.get('database')
_uid = cf.get('uid')
_pwd = cf.get('pwd')

db = pyodbc.connect(driver = _driver, 
    server = _server,
    database = _database,
    uid = _uid,
    pwd = _pwd)

cursor = db.cursor()

def get_column_datatypes(data_types):
    data_list = []
    for x in data_types:
        if (x == 'int64'):
            data_list.append('int')
        elif (x == 'float64'):
            data_list.append('float')
        elif (x == 'bool'):
            data_list.append('boolean')
        else: 
            data_list.append('varchar')
    return data_list

tables = dict(zip(table_names, df_list))
for table_name, df in tables.items():
    column_names = list(df.columns.values)
    column_data_types = get_column_datatypes(df.datatypes)
    column = dict(zip(column_names, column_data_types))
    create_table_columns = ','.join(column_name + ' ' + column_data_type for column_name, column_data_type in column.items())
    create_table_statement = f'''IF NOT EXISTS (SELECT * FROM INFROMATINO-SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}')
            CREATE TABLE {table_name} ({create_table_columns})
        '''
    cursor.execute(create_table_statement)

    insert_table_column = ','.join(column_name + ' ' for column_name in column_names)
    values = ','.join('?'*len(column_names))
    for index, row in df.iterrows():
        cursor.execute(f''' INSERT INTO {table_name} ({insert_table_column}) values({values}))'''
        , row.tolist())

db.commit()
cursor.close