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

# insert into mysql table

sql_statement = "INSERT INTO python_created_table \
(COLUMN1, COLUMN2) VALUES (%s, %s)"
items_to_insert = ('hi', 4)

create_cursor.execute(sql_statement, items_to_insert)

mariadb_connection.commit()


# select items from mysql

sql_statement = 'SELECT * from python_creation_table'

create_cursor.execute(sql_statement)

myresult = create_cursor.fetchall()  # fetchnone();

print(myresult)


# use of where clause

sql_statement = 'SELECT * from python_creation_table where COLUMN2 = 3'

myresult = create_cursor.fetchall()  # fetchnone();

print(myresult)


# more complicated

sql_statement = 'SELECT sum(COLUMN2) as sum_col_2 from python_creation_table \
where COLUMN2 < 3 group by COLUMN2 order by 1'

create_cursor.execute(sql_statement)

myresult = create_cursor.fetchall()  # fetchnone();

print(myresult)

# drop

create_cursor.execute("DROP TABLE python_created table")

create_cursor.execute("SHOW tables")

for x in create_cursor:
    print(x)

# close connection

mariadb_connection.close()
