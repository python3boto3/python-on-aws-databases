# Creating secret in AWS Secrets Manager
import boto3
import json


client = boto3.client('secretsmanager')

response = client.create_secret(
    Name='DatabaseProdSecrets',
    SecretString='{"username": "prod", "password": "hello-world-prod"}'
)

# List secrets in AWS Secrets Manager

response = client.list_secrets()

print(response['SecretList'])


# Retrieve a secret value from AWS Secrets Manager

response = client.get_secret_value(
    SecretId='DatabaseProdSecrets'
)

database_secrets = json.loads(response['SecretString'])

print(database_secrets['password'])


# Updating an existing secret in AWS Secrets Manager - put_secret_value method (remove and re-add)

response = client.put_secret_value(
    SecretId='DatabaseProdSecrets',
    SecretString='{"username": "prod", "password": "hello-world-updated2"}'
)

print(response)


# Updating an existing secret in AWS Secrets Manager  - update_secret method

response = client.update_secret(
    SecretId='DatabaseProdSecrets',
    Description='Description updated'
)

print(response)


# delete a secret

response = client.delete_secret(
    SecretId='DatabaseProdSecrets',
    RecoveryWindowInDays=10,
    ForceDeleteWithoutRecovery=False
)

print(response)


# Restore deleted AWS Secrets Manager secret

import boto3

client = boto3.client('secretsmanager')

response = client.restore_secret(
    SecretId='DatabaseProdSecrets'
)

print(response)

