import boto3
import psycopg2

# https://aws.amazon.com/blogs/big-data/

# See the section on :
# using-the-amazon-redshift-data-api-
# to-interact-with-amazon-redshift-clusters/

# session = boto3.session.Session(profile_name="red")
# rsd = session.client('redshift-data', region_name='us-east-1')

rsd = boto3.client('redshift-data', region_name='us-east-1')

resp = rsd.execute_statement(
    Database="dev",
    ClusterIdentifier='redshift-cluster-1',
    DbUser="redshift_data_api_user",
    Sql="DELETE FROM public.cqpocsredshiftdemo",
    WithEvent=True
)
