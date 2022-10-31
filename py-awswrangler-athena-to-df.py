import awswrangler as wr  
df = wr.athena.read_sql_query(sql="SELECT * FROM <table_name_in_Athena>", database="<database_name>")