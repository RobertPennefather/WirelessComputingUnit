# ------------------------------
# Mobile and Wireless Networks
# Group 4
#
# suscriber_storage.py
#
#
# boto3 package neeeded, if you don't have it please install it
# 
# ------------------------------ 

#import context  # Ensures paho is in PYTHONPATH
import paho.mqtt.client as mqtt
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

# Dynamo DB table name to store the msg
TABLE_NAME = 'MQTT'

dynamodb_client = boto3.client('dynamodb')

def on_connect(mqttc, obj, flags, rc):
	print("rc: " + str(rc))


def on_message(mqttc, obj, msg):
	print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload, 'utf-8'))
	# Write the incoming msg into DB
	store_msg(msg)


def on_publish(mqttc, obj, mid):
	print("mid: " + str(mid))


def on_subscribe(mqttc, obj, mid, granted_qos):
	print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(mqttc, obj, level, string):
	print(string)

def store_msg(msg):
	try:
		print("Storing message...")
		# Stores the msg into Dynamo DB
		date = datetime.now().__str__()
		item_response = dynamodb_client.put_item(
			TableName=TABLE_NAME,
			Item={
				'Topic': {'S': msg.topic}, # Hash key
				'Datetime': {'S': date}, #Sort Key
				'Qos': {'N': str(msg.qos)},
				'Payload': {'S':str(msg.payload, 'utf-8')},
			}
		)
		print("Message stored in table: "+TABLE_NAME+" (DynamoDB). "+date)
	except ClientError as e:
		print(e)    		


# If you want to use a specific client id, use
# mqttc = mqtt.Client("client-id")
# but note that the client id must be unique on the broker. Leaving the client
# id parameter empty will generate a random id for you.
mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe
# Uncomment to enable debug messages
# mqttc.on_log = on_log
mqttc.connect("127.0.0.1", 1883, 60)
mqttc.subscribe("#", 0)

try:
	mqttc.loop_forever()
except:
	print("Exit...")
