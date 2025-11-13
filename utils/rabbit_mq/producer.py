import aio_pika
import json
import uuid
from datetime import datetime, timezone
from utils.redis.redis_utils import set_notification
import logging

EXCHANGE_NAME = "notifications.direct"

logger = logging.getLogger(__name__)
 
async def publish_email_message(channel: aio_pika.Channel, json_string_payload: str, priority_level: int, tracking_id: str = None):
	"""Publish a message to the email priority queue"""
	# Use provided tracking_id or generate new one
	if tracking_id is None:
		tracking_id = str(uuid.uuid4())
	
	# Prepare the message to be sent to the email priority queue
	
	# Deserialize the incoming payload from JSON to Dict Object
	# By doing so i can tag onto it, the notification payload

	payload = json.loads(json_string_payload)

	# Generate a UUID for uniquely identifying each incoming notification
	# This is to enable tracking notification status
	current_timestamp = datetime.now(timezone.utc).isoformat()

	# Notification Status payload construction
	notification_status = {
		"notification_id": tracking_id,
		"status": "pending",
		"timestamp": current_timestamp,
		"error": ""
	}

	# The key 'tracking_metadata' is assigned to the notification_status
	# upon the usage of json.loads(), the key can be used to extrack and update
	# the notification status and sent back to the API Gateway Servive.
	payload["tracking_metadata"] = notification_status
	message = aio_pika.Message(
		body=json.dumps(payload).encode("utf-8"),
		priority=priority_level,
		delivery_mode=aio_pika.DeliveryMode.PERSISTENT
		)

	# Pass it to redis for storage, tracks the notification status
	try:
		logger.info(f" [⚓] Sending notification id {tracking_id} to Redis.... ")
		await set_notification(tracking_id, notification_status)
		logger.info(f" [✅] notification id {tracking_id} was sent to Redis. ")
	except Exception as e:
		logger.debug(f" [⛔] notification id {tracking_id} was not sent to Redis; ", e)
	

	try:
		logger.info(f" [⚓] Sending notification id {tracking_id} to RabbitMQ.... ")
		# Route the payload or message to the exchange
		await channel.default_exchange.publish(message=message, routing_key="email")
		logger.info(f" [✅] notification id {tracking_id} was sent to RabbitMQ. ")
	except Exception as e:
		logger.debug(f" [⛔] notification id {tracking_id} was not sent to RabbitMQ;. ", e)
		raise e

async def publish_push_message(channel: aio_pika.Channel, json_string_payload: str, priority_level: int, tracking_id: str = None):
	"""Publish a message to the push priority queue"""
	# Use provided tracking_id or generate new one
	if tracking_id is None:
		tracking_id = str(uuid.uuid4())
	
	# Prepare the message to be sent to the email priority queue

	# Deserialize the incoming payload from JSON to Dict Object
	# By doing so i can tag onto it, the notification payload
	payload = json.loads(json_string_payload)

	# Generate a UUID for uniquely identifying each incoming notification
	# This is to enable tracking notification status
	current_timestamp = datetime.now(timezone.utc).isoformat()

	# Notification Status payload construction
	notification_status = {
		"notification_id": tracking_id,
		"status": "pending",
		"timestamp": current_timestamp,
		"error": ""
	}


	# The key 'tracking_metadata' is assigned to the notification_status
	# upon the usage of json.loads(), the key can be used to extrack and update
	# the notification status and sent back to the API Gateway Servive.

	payload["tracking_metadata"] = notification_status
	message = aio_pika.Message(
		body=json.dumps(payload).encode("utf-8"),
		priority=priority_level,
		delivery_mode=aio_pika.DeliveryMode.PERSISTENT
	)

	# Pass it to redis for storage, tracks the notification status
	try:
		logger.info(f" [⚓] Sending notification id {tracking_id} to Redis.... ")
		await set_notification(tracking_id, notification_status)
		logger.info(f" [✅] notification id {tracking_id} was sent to Redis. ")
	except Exception as e:
		logger.debug(f" [⛔] notification id {tracking_id} was not sent to Redis; ", e)

	try:
		logger.info(f" [⚓] Sending notification id {tracking_id} to RabbitMQ.... ")
		# Route the payload or message to the exchange, from exchange to the binded queue
		await channel.default_exchange.publish(message=message, routing_key="push")
		logger.info(f" [✅] notification id {tracking_id} was sent to RabbitMQ. ")

	except Exception as e:
		logger.debug(f" [⛔] notification id {tracking_id} was not sent to RabbitMQ;. ", e)
		raise e
	