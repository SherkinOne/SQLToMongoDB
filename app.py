import argparse
import sys

import mysql.connector

from mysql.connector import errorcode
from pymongo import MongoClient
from datetime import *
import re
from bson import Decimal128 
import decimal

# THIS FILE KEEPS A LIST OF THE DATATBASES THAT ARE MIGRATED TO STOP DUPLICATION
# DELETE IF NECESSARY
MIGRATION_FILE_NAME: str = 'migrated.txt'


def db_migrate():
    mysqldb_host: str = input('Enter MySQL DB Host:')
    mysqldb_port: str = input('Enter MySQL DB Port:')
    mysqldb_user: str = input('Enter MySQL DB User:')
    mysqldb_password: str = input('Enter MySQL DB Password:')
    mysqldb_database: str = input('Enter MySQL DB Database name:')
    mongodb_connection_uri: str = input('Enter MongoDB Connection URI:')
    mongodb_database: str = input('Enter MongoDB DB Database name:')
 

    # connect to MySQL DB
    try:
        mysqldb = mysql.connector.connect(
            host=mysqldb_host,
            port=mysqldb_port,
            user=mysqldb_user,
            password=mysqldb_password,
            database=mysqldb_database
          
            #, ssl_disabled =  True
            # // THE FOLLOWING IS NEEDED TO CONNECT TO REMOTE COMPUTER (CA.PEM TAKEN FROM MYSQL DIR) - DEPENDING ON REMOTE SETUP //

            # ,ssl_ca = './ca.pem',
            # ssl_verify_cert = True,
            # auth_plugin = 'sha256_password'
           
        )
        print('Connection to MySQL DB successful...')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print('Invalid username or password for MYSQL')
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print('MYSQL database does not exist')
        else:
            print(err)
        sys.exit(1)


    # connect to MongoDB
    try :
        mongodb_client = MongoClient(mongodb_connection_uri)
        # mongodb_client = MongoClient("mongodb://localhost:27017/")

        # get or create database in mongodb
        if mongodb_database =="" : 
            mongodb_database= mysqldb_database
        
        mongodb_client_db = mongodb_client[mysqldb_database]

        print("MongoDB database connected")
    except : 
        print("MongoDB error")
        sys.exit(1)

    # instantiate MySQL DB cursor
    cursor = mysqldb.cursor()

    # execute query
    cursor.execute('SHOW TABLES')

    # fetch all tables
    tables = cursor.fetchall()
    print(f'All Tables ====> {tables}')

    if tables:
        for table_name in tables:
            table_name = table_name[0]
            print(f'Processing table ====> {table_name}\n')

            # SQL query to count the total number of rows
            count_query = f"SELECT COUNT(*) FROM {table_name}"

            cursor.execute(count_query)
            # Fetch the count result
            total_rows = cursor.fetchone()[0]
            print(f"{total_rows} records found in the {table_name} table")
            # THIS IS A RECORD OF MIGRATIONS
            try:
                with open(f'{MIGRATION_FILE_NAME}') as f:
                    file_read = f.read()
                    if table_name in file_read:
                        print(f'{table_name} already migrated successfully!')
                        continue
            except FileNotFoundError:
                pass

            # create and populate the collections
            collection = mongodb_client_db[table_name]

            cursor2 = mysqldb.cursor(dictionary=True)

            offset: int = 0
            page_size: int = 100

            if not total_rows:
                continue
            while offset < total_rows:
                cursor2.execute(f"SELECT * FROM {table_name} LIMIT {page_size} OFFSET {offset}")
                results = cursor2.fetchall()
                # print(results)
                print(f'{offset} - {offset + page_size} of {total_rows} migrated...')
                offset += page_size
                # // remove datetime
                try :
                # bulk insert data into mongodb
                    collection.insert_many(results)
                except  Exception as e:
                    date_pattern = r'\b(\d{4}-\d{2}-\d{2})\b'
                    for item in results :
                        for key, value in item.items():
                            # Check value to see that it complies with Mongodb variables
                            if re.search(date_pattern, str(value)):
                                item[key] =  datetime.combine(value, datetime.min.time())
                            elif isinstance(value, decimal.Decimal) :
                                 item[key] =  Decimal128(str(value))
                    try :
                        collection.insert_many(results)
                    except Exception as new_error:
                           print(f"Error while handling the original exception: {e}")
                    

            print(f'{total_rows} records fully migrated for {table_name}...\n')
            total_documents = collection.count_documents({})
            print(f'Total documents in the collection {table_name}: {total_documents}\n')

            with open(f'{MIGRATION_FILE_NAME}', 'a') as f:
                f.write(f'{table_name},')

            # reset the connection
            cursor.reset(free=True)

            # close cursor2 connection
            cursor2.close()
    else:
        print('No tables found. \n Exiting...')

    # close connections
    cursor.close()
    mysqldb.close()
    print(f"Check {MIGRATION_FILE_NAME} for tables migrated ")


if __name__ == '__main__':
    db_migrate()
