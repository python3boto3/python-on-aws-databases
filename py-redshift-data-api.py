from tarfile import PAX_NAME_FIELDS
from typing import Any
import boto3
import psycopg2
import pandas
import pandas_redshift as pr
import pandas as pd
import logging


# https://aws.amazon.com/blogs/big-data/

# See the section on :
# using-the-amazon-redshift-data-api-
# to-interact-with-amazon-redshift-clusters/

session = boto3.session.Session(profile_name="red")
# 
rsd = session.client('redshift-data', region_name='us-east-1')

#rsd = boto3.client('redshift-data', region_name='us-east-1')

resp = rsd.execute_statement(
    Database="dev",
    ClusterIdentifier='redshift-cluster-1',
    DbUser="awsuser",
    Sql="SELECT * FROM public.cqpocsredshiftdemo",
    WithEvent=True
)

# Credentials can be set using different methodologies. For this test,
# I ran from my local machine which I used cli command "aws configure"
# to set my Access key and secret access key
 
client2 = session.client(service_name='redshift',
                      region_name='us-east-1')
#
#Using boto3 to get the Database password instead of hardcoding it in the code
#
cluster_creds = client2.get_cluster_credentials(
                         DbUser='awsuser',
                         DbName='dev',
                         ClusterIdentifier='redshift-cluster-1',
                         AutoCreate=False)
 
try:
    # Database connection below that uses the DbPassword that boto3 returned
    conn = psycopg2.connect(        
                host = 'redshift-cluster-1.cjqx1l9dqfg2.us-east-1.redshift.amazonaws.com',
                port = '5439',
                user = cluster_creds['DbUser'],
                password = cluster_creds['DbPassword'],
                database = 'dev'
                )
    # Verifies that the connection worked
    cursor = conn.cursor()
    cursor.execute("SELECT VERSION()")
    results = cursor.fetchone()
    ver = results[0]
    if (ver is None):
        print("Could not find version")
    else:
        print("The version is " + ver)
 
except:
   print('Failed to open database connection.')

