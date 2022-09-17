import psycopg2


# Connect to postgresql db

con = psycopg2.connect(

    host="xRay",
    database="xDB",
    user="rsmith2",
    password="WEy8hiu$I#M")

# Add cursor

cur = con.cursor()

# Execute query

cur.execute("select id, name from xTable")

x_rows = cur.fetchall()

for x in x_rows:
    print (f"id {x[0]} name {x[1]}")

# close the cursor

cur.close()

# close the connection
con.close()
