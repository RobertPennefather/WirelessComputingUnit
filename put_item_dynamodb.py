# ------------------------------
# Mobile and Wireless Networks
# Group 4
#
# put_item_dynamodb.py
#
# Creates a table in AWS DynamoDB
#
# ------------------------------ 
#
# Date format -> YYYY-MM-DD HH:MM:SS
#
# ------------------------------ 

import boto3
from datetime import datetime

TABLE_NAME = 'MQTT'

# Get the service resource.
dynamodb_client = boto3.client('dynamodb')

# Put item in the DynamoDB table.
print("Storing message...")
date = datetime.now().__str__()
response = dynamodb_client.put_item(
	TableName=TABLE_NAME,
	Item={
		'Topic': {'S': 'topic'}, # Hash key
		'Date': {'S': date}, #Sort Key
		'Qos': {'N': '1'},
		'Payload': {'S': 'payload'},
	}
)
print("Message stored in table: "+TABLE_NAME+" (DynamoDB). "+date)

# Print out some data about the table.
#print("Table Count: "+str(response['Count']))
print(response)

print("done")
