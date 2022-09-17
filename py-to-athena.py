import boto3
import time


def lambda_handler(event, context):

    AWS_REGION = "us-east-1"
    SCHEMA_NAME = "schema_label"
    S3_STAGING_DIR = "s3://s3-east-1-bucket/resultset/"

    athena_client = boto3.client(
        "athena", region_name=AWS_REGION,
    )

    query_response = athena_client.start_query_execution(
        QueryString="SELECT * FROM table",
        QueryExecutionContext={"Database": SCHEMA_NAME},
        ResultConfiguration={
            "OutputLocation": S3_STAGING_DIR,
            "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
        },
    )
    while True:
        try:
            # This function only loads the first 1000 rows
            boto3.client.get_query_results(
                QueryExecutionId=query_response["QueryExecutionId"]
            )
            break
        except Exception as err:
            if "not yet finished" in str(err):
                time.sleep(0.001)
            else:
                raise err
