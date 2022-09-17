import pymysql


# Connect to MySQL DB

def getConnection():
    connection = pymysql.connect(
        host="localhost", user="root", passwd="", database="py-to-mysql")
    return connection

# Create table
connection = pymysql.connect(
    host="localhost", user="root", passwd="", database="py-to-mysql")
cursor = connection.cursor()
sql_create_table = "create table person(id int(28) primary key auto_increment, \
first_name char(28) not null,last_name char(28) not null)"
cursor.execute(sql_create_table)
connection.close()

# Update Data
conn = getConnection()
cursor = conn.cursor()
sql_update_record = "update person set first_name='John' where id =1607"
cursor.execute(sql_update_record)
conn.commit()
cursor.close()
print("File Updated Successfully....")

# Delete  Data
conn = getConnection()
cursor = conn.cursor()
sql_delete_record = "delete from person where id =1607"
cursor.execute(sql_delete_record)
conn.commit()
cursor.close()
print("Record Deleted Successfully....")

# Fetch Data
conn = getConnection()
cursor = conn.cursor()
sql_fetch_persons = "select * from person"
cursor.execute(sql_fetch_persons)
for row in cursor.fetchall():
    print(row)
cursor.close()
