import boto3
import psycopg2


def lambda_handler(event, context):

    host = 'redshift-cluster-1.cjqx1l9dqfg2.us-east-1.redshift.amazonaws.com'
    port = 5439
    db_name = 'dev'

    cluster_identifier = 'redshift-cluster-1'
    rs_user = 'awsuser'

    try:
        cr = boto3.client('redshift', region_name='us-east-1')
        # get cluster credentials and temp username and password
        cluster_creds = cr.get_cluster_credentials(
            DbUser=rs_user,
            DbName=db_name,
            ClusterIdentifier=cluster_identifier,
            AutoCreate=False
            )
        temp_user = cluster_creds['DbUser']
        temp_pswd = cluster_creds['DbPassword']
        conn = psycopg2.connect(
            db_name,
            host=host,
            port=port,
            user=temp_user,
            password=temp_pswd
            )

        conn.close
    except:
        print(1)
