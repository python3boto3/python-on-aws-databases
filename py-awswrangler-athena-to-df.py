import awswrangler as wr  
import boto3
from io import StringIO 


df = wr.athena.read_sql_query(sql="SELECT * FROM <table_name_in_Athena>", database="<database_name>")

# copy to csv

session = boto3.Session(profile_name="red")
s3_res = session.resource('s3')

csv_buffer = StringIO()

df.to_csv(csv_buffer)

bucket_name = 'stackvidhya'

s3_object_name = 'df.csv'


s3_res.Object(bucket_name, s3_object_name).put(Body=csv_buffer.getvalue())

print("Dataframe is saved as CSV in S3 bucket.")

