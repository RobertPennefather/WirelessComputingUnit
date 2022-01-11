import boto3

# ------------------------------
# Mobile and Wireless Networks
# Group 4
#
# get_item_dynamodb.py
#
# Creates a table in AWS DynamoDB
#
# ------------------------------ 

TABLE_NAME = 'MQTT'

# Get the service resource.
dynamodb_client = boto3.client('dynamodb')

# Scan the DynamoDB table.
response = dynamodb_client.scan(TableName=TABLE_NAME)

# Print out some data about the table.
print("Table Count: "+str(response['Count']))
#print(response)

print("done")
