import json
import boto3
import psycopg2
import pandas as pd


# Local test development only. 
# To allow redshift accessibility outside the vpc: add an elastic address (EIP)
# # On the cluster console, add Actions: "Modify publicly accessible setting".
# Enable .. and reference the EIP
# Wait 10 minutes to allow the cluster to modify. 

# https://aws.amazon.com/blogs/big-data/

# See the section on :
# using-the-amazon-redshift-data-api-
# to-interact-with-amazon-redshift-clusters/

# local session variable only
session = boto3.session.Session(profile_name="red")
# 
# Credentials can be set using different methodologies, secrets, credential tokens, etc. For this test,
# I ran from my local machine which I used cli command "aws configure" using RBAC to access via IAM role policies, 
# combined with get_cluster_credentials hich returns an RBAC generated token.
 
client = session.client(service_name='redshift',
                      region_name='us-east-1')                                                      # # # # # # # # # # # # # 
#
# Therefore using boto3 to get the Database password token instead of hardcoding it in the code. Secrets mgr also preferable. 
# Currently no access to that...
#
cluster_creds = client.get_cluster_credentials(
                         DbUser='awsuser',                                                          # # # # # # # # # # # # # 
                         DbName='dev',                                                              # # # # # # # # # # # # # 
                         ClusterIdentifier='redshift-cluster-1',                                    # # # # # # # # # # # # # 
                         AutoCreate=False)

try:
    # Database connection below that uses the DbPassword that boto3 returned
    conn = psycopg2.connect(        
                host = 'redshift-cluster-1.cjqx1l9dqfg2.us-east-1.redshift.amazonaws.com',          # # # # # # # # # # # # # 
                port = '5439',
                user = cluster_creds['DbUser'],
                password = cluster_creds['DbPassword'],
                database = 'dev'
                )                

    # Verifies that the connection worked
    cursor = conn.cursor()
    cursor.execute("select airlineorderinfo FROM dev.public.category")                                       # # # # # # # # # # # # # 
    results = cursor.fetchall()
    sqlResult = results
    if (results is None):    
        print("Could not find results")
    else:
        # returns the collection of all rows. 
        print(sqlResult)

 
except:
   print('Failed to open database connection.')



'''
(Ex-VPC):

Local test development only. 

To allow Redshift accessibility outside the vpc: add an elastic address (EIP)

On the cluster console, add Actions: Modify publicly accessible setting.

Enable .. and reference the EIP

Wait 10 minutes to allow the cluster to modify. 


ignore this step for the Redshift on Shift4 because it communicates within the vpc.

-------------------------------------------------------

(In-VPC):

Create  Lambda-layer. On Cloud9 only

https://towardsdatascience.com/python-packages-in-aws-lambda-made-easy-8fbc78520e30


mkdir folder
cd folder
virtualenv v-env
source ./v-env/bin/activate             
pip install psycopg2-binary      <-- The official library
deactivate


mkdir python
cd python
cp -r ../v-env/lib64/python3.7/site-packages/* .
cd ..
zip -r psycopg2_layer.zip python
aws lambda publish-layer-version --layer-name psycopg2 --zip-file fileb://psycopg2_layer.zip --compatible-runtimes python3.7



Add 30 seconds, 1024MB memory 

and these permissions to the lambda: 

AWSLambdaBasicExecutionRole
AmazonRedshiftFullAccess	
AmazonRedshiftAllCommandsFullAccess
AmazonRedshiftDataFullAccess
'''







