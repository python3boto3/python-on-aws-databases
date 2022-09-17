# install mysql - requires pip install mysql-connector-python

import mysql.connector as mariadb

# create mysql connection and define cursor

mariadb_connection = mariadb.connect(
    user='test',
    password='password',
    host='localhost',
    port='3306'
)

create_cursor = mariadb_connection.cursor()

# create database

create_cursor.execute("CREATE DATABASE test_db")

for x in create_cursor:
    print(x)

# create table

create_cursor.execute(
    "CREATE_TABLE python_created_table (COLUMN1 VARCHAR(2), COLUMN2 int)"
    )

# show tables

create_cursor.execute("SHOW TABLES")

for x in create_cursor:
    print(x)
