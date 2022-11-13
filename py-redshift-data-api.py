
import boto3
import psycopg2
import pandas as pd
from numpy.random import randint
from io import StringIO

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
session = boto3.session.Session(profile_name="x")
# 
# Credentials can be set using different methodologies, secrets, credential tokens, etc. For this test,
# I ran from my local machine which I used cli command "aws configure" using RBAC to access via IAM role policies, 
# combined with get_cluster_credentials hich returns an RBAC generated token.

#s3 object placeholders, old series r, new serices c
s3r = session.resource('s3', 'us-east-1')
s3c = session.client('s3', 'us-east-1')

# define, instantiate the dataframe
df = pd.DataFrame()

# reset all counters
results=""
rows = ""

#list the fieldnames.
fieldnames = ['za', 'zb', 'zc', 'zd']
#default null on all field values
za_ = "null" 
zb_ = "null"
zc_ = "null"

rows2 = ""

#open the redshift connection
client = session.client(service_name='redshift',
                        region_name='us-east-1')                                                                    

# using boto3 to get the Database token instead of hardcoding it in the code

cluster_creds = client.get_cluster_credentials(
                            DbUser='awsuser',                                                                          
                            DbName='dev',                                                                               
                            ClusterIdentifier='redshift-cluster-1',                                                     
                            AutoCreate=False)

try:
    # Database connection below that uses the DbPassword that boto3 returned
    conn = psycopg2.connect(        
                host = 'redshift-cluster-1.abcd1m2abcd2.us-east-1.redshift.amazonaws.com',                              
                port = '5439',
                user = cluster_creds['DbUser'],
                password = cluster_creds['DbPassword'],
                database = 'dev'
                )                

    # Verifies that the connection worked
    cursor = conn.cursor()
    #select fields FROM input"
    cursor.execute("select distinct za, zb FROM dev.public.source_tbl")                  
    results = cursor.fetchall()
    sqlResult = results
    if (results is None):    
        print("Could not find results")
    else:
        # returns the collection of all rows and offer replacement formatting values. 

        # CAUTION - think twice before removing...

        # The reason for replacements below: one of the airlineorderinfo columns contains 
        # many !@##$% typical special characters ... the ones chosen below are more inaccessible 
        # as chrs 191, 218 are on the extended ascii characterset, they do not interfere with the pattern.

        # By doing so, we can navigate rows, and navigate field key pairs within each row. 

        # replaces key pair separator with ascii 254
        sqlResult2 = str(sqlResult).replace("%3D","■")

        # replaces field end with ascii char 186
        sqlResult2 = str(sqlResult2).replace("%0D%0A","║")

        # create row start markers, replace apostrophe
        sqlResult2 = str(sqlResult2).replace("'Ticket", "┌Ticket")
        sqlResult2 = str(sqlResult2).replace("[(┌","")

        # create row end markers
        # replaces first record start character ' [  with ascii char 217, 218
        # replaces record all end characters except final with ascii char 218
        #sqlResult2 = str(sqlResult2).replace("',)]","┘")
        #sqlResult2 = str(sqlResult2).replace(",), (┌","┘┌")
        #sqlResult2 = str(sqlResult2).replace("'┘","┘")

        sqlResult2 = str(sqlResult2).replace(", ┌","║")
        
        sqlResult2 = str(sqlResult2).replace("[(","┌ReplicationTimeStamp■")
        sqlResult2 = str(sqlResult2).replace(" ("," (┌ReplicationTimeStamp■")
        sqlResult2 = str(sqlResult2).replace(", '","║")

        sqlResult2 = str(sqlResult2).replace("')","┘')")
        sqlResult2 = str(sqlResult2).replace('║")','║┘")')

        sqlResult2 = str(sqlResult2).replace('║"┘','║┘"')

        # Interesting malformation, some occasionally come in with DBL QUOTES, spaces and commas...
        sqlResult2 = str(sqlResult2).replace('"za', 'za')
        sqlResult2 = str(sqlResult2).replace(', za', '║za')
        sqlResult2 = str(sqlResult2).replace(',za', '║za')
        
        # calculate end rows count for chr 217 ┘ 
        x = sqlResult2.count("┘")

        entireLen = len(sqlResult2)
        #'15,519'
        beginRowchar=0
        # print(entireLen)
        i = 0 # initial value
        # (i) now = the counter for the number of rows (x))
        # null strings

        za_ = 0

        zb_ = 0

        zc__ = 0

        zd_ = 0

        # compound string

        sqlResult2 = str(sqlResult2).replace("┌","")

        # row data calcs ...............................................................................


        for i in range(0, x):            
            #print(x)# max rows
            #print(i)# current row
            # find the start char (beg) and ending char (end) for the row
            lastPos = entireLen-beginRowchar
            endRowChar = sqlResult2.find('┘', beginRowchar)
            currentRowStr = sqlResult2[beginRowchar:endRowChar]
            # print(currentRowStr)
            #print(" ")
            # calculate number of fields in row 1 
            currentRowFieldCount = str(currentRowStr).count("║")
            #print(currentRowFieldCount)
            beginFieldchar = 0

            # field data calcs...........................................................................
            

            beginRowchar = endRowChar+1

            endFieldCharNum = 0
            beginFieldchar = 0
            midDefNum = 0
            v = 0
            currentRowFieldCount = currentRowFieldCount

            tableDestRowStr1 = "NULL"
            tableDestRowStr2 = "NULL"
            

            for v in range(0, currentRowFieldCount):
                #print(i)

                #Field finder

                endFieldCharNum = str(currentRowStr).find('║', beginFieldchar)
                midDefNum = str(currentRowStr).find('■', beginFieldchar)

                #Field split character

                totField = currentRowStr[beginFieldchar:endFieldCharNum]
                #print(totField)

                midFieldNum = str(totField).find('■')

                #Header

                headerStr = totField[0:midFieldNum]

                #print(headerStr)

                #Value

                valueStr = totField[midFieldNum+1:]

                #print(valueStr)

                # increment of the counter position based on the last found position

                beginFieldchar = endFieldCharNum+1

                #valueStrX = valueStrX + ", " + valueStr
                #headerStrX = headerStrX + ", " + headerStr

                headerStr = headerStr.lower()

                keyPair = keyPair + headerStr + ": " + valueStr + "\r\n"

                gapIntStr = ": "

                if "za" in headerStr:
                    za_ = valueStr

                if "zb" in headerStr:
                    zb_ = valueStr
                
                if "zc" in headerStr:
                    zc_ = valueStr                
                    
                if "zd" in headerStr:
                    zd_ = valueStr

            # ensure integer, 7 characters for the replicationtimestamp
            replicationtimestamp1 = str(replicationtimestamp_)
            replicationtimestamp1 = int(replicationtimestamp1[:7])

            # alternative trim function, cleans trailing spaces and any special characters
            replicationtimestamp_ = replicationtimestamp1

            # Use each stripped value and add to the relevant columns in the dataframe

            dict = {'za': za_, 'zb': zd_, 'zc': zc__, 'zd': zd_}
            # append each of the 313 test items

            df = df.append(dict, ignore_index = True)    
            #print(passengernamerecord_)
            #print(df)

        # after the last loop, declare the table values. 
        # print(df)

        # post these to S3 as csv for the upload to redshift
        csv_buffer = StringIO()

        # redshift expects a headerless import, passing the dataframe to csv. 
        df.to_csv(csv_buffer, header=False, index=False)

        # adding a key name
        key1 = "x4.csv"
        # target bucket (alter)
        bucket4 = "output-bucket-101"
        # put csv
        s3r.Object(bucket4, key1).put(Body=csv_buffer.getvalue())

        # dump the same dataframe to snappy-parquet
        df.to_parquet('parquet.gzip', engine='auto', compression='snappy')
        key1 = "sp4.gz"
        # target bucket
        bucket4 = "output-bucket-101"
        s3r.Object(bucket4, key1).put(Body=csv_buffer.getvalue())

        # upload into redshift 
        sql_query = """Truncate "dev"."public"."output-table";"""
        cursor = conn.cursor()
        cursor.execute(sql_query)
        conn.commit()

        # Copy data
        sql_query = """COPY dev.public.output-table FROM 's3://output-bucket-101/x4.csv' IAM_ROLE 'arn:aws:iam::333322221111:role/service-role/AmazonRedshift-CommandsAccessRole-20221031T150852' FORMAT AS CSV DELIMITER ',' REGION AS 'us-east-1';"""
        cursor = conn.cursor()
        cursor.execute(sql_query)
        conn.commit()
        conn.close() 

except:

    conn.close()

    print('Failed to open database connection.')

conn.close()

