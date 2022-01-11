# ------------------------------
# Mobile and Wireless Networks
# Group 4
#
# get_item_dynamodb.py
#
# queries items from a table in AWS DynamoDB
# argv[1] = topic to query (compare equals)
# argv[2] = datetime format YYYY-MM-DD HH:MM:SS (compare begins with)
#
# ------------------------------ 

import sys
import getopt
import boto3
TABLE_NAME = 'MQTT'

# Get the service resource.
dynamodb_client = boto3.client('dynamodb')

#### Query items from the DynamoDB table ####

# Query by topic and date
if len(sys.argv) == 3:
	print("Searching Topic: "+sys.argv[1]+" with Datetime beginning with: "+sys.argv[2])
	response = dynamodb_client.query(
		TableName=TABLE_NAME,
		KeyConditions={
			'Topic': {
			    'AttributeValueList': [
				{
				    'S': sys.argv[1],
				},
			    ],
			    'ComparisonOperator': 'EQ'
			},
			'Datetime': {
			    'AttributeValueList': [
				{
				    'S': sys.argv[2],
				},
			    ],
			    'ComparisonOperator': 'BEGINS_WITH'
			}
		    },
		)
	# Response contains the list of items found
	
	# Print the number of items found
	print("Query by topic and date. Items found count: "+str(len(response['Items'])))


# Query only by topic
elif len(sys.argv) == 2:
	print("Searching Topic: "+sys.argv[1])
	response = dynamodb_client.query(
		TableName=TABLE_NAME,
		KeyConditions={
			'Topic': {
			    'AttributeValueList': [
				{
				    'S': sys.argv[1],
				},
			    ],
			    'ComparisonOperator': 'EQ'
			}
		    },
		)
	# Response contains the list of items found
	
	# Print the number of items found
	print("Query by topic. Items found count: "+str(len(response['Items'])))

else:
	print("No arguments received to complete the query")

print("Query completed")
