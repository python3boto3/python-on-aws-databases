import cx_Oracle


# Database connection

def getConnection():
    connection = cx_Oracle.connect('xRay/xRay@localhost:1521/xOr')
    return connection

# Insert Data
conn = getConnection()
cursor = conn.cursor()
i1 = "insert into persons values( \
    1092, 'Dave', 'Smith', 'Leeds', 'West Yorkshire')"
i2 = "insert into persons values( \
    1092, 'Mike', 'Fielding', 'Oak Avenue', 'Portsmouth')"
cursor.execute(i1)
cursor.execute(i2)
conn.commit()
cursor.close()

# Update Data
conn = getConnection()
cursor = conn.cursor()
sql_update_record = "update persons \
    set first_name='Dave' where person_id =1092"
cursor.execute(sql_update_record)
conn.commit()
cursor.close()
print("File Updated Successfully....")

# Delete  Data
conn = getConnection()
cursor = conn.cursor()
sql_delete_record = "delete from persons where person_id =1092"
cursor.execute(sql_delete_record)
conn.commit()
cursor.close()
print("Record Deleted Successfully....")

# Fetch Data
sql = "select * from persons"
conn = getConnection()
cursor = conn.cursor()
cursor.execute(sql)
for result in cursor:
    print(result)
conn.commit()
cursor.close()
