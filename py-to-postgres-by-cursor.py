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

'''

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