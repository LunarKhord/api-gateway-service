import redis.asyncio as redis 
from redis.asyncio import Redis
import json
from fastapi_limiter import FastAPILimiter
import aio_pika
import logging

from models.notification import NotificationStatus

logger = logging.getLogger(__name__)

async def initialize_redis_client():
	global REDIS_CLIENT
	try:
		logger.info("Attempting to connect to the Redis Server.")
		REDIS_CLIENT = redis.from_url("redis://localhost:6379", decode_responses=True)
		await REDIS_CLIENT.ping()
		await FastAPILimiter.init(REDIS_CLIENT)
		logger.info("Redis Server connected successfully.")
		return REDIS_CLIENT
	except Exception as e:
		logger.debug(f"[⛔] Redis client was not initialized successfully:", e)



async def set_notification(notification_id, notification_payload):
    try:
        # Use HSET with the mapping argument (best practice for Redis hashes)
        # Keys and values in the dict are automatically handled by redis-py
        logger.info(f"Attempting to store a notification {notification_id} status to Redis using the HSET")
        if REDIS_CLIENT:
	        await REDIS_CLIENT.hset(
	            str(notification_id), 
	            mapping=notification_payload
	        )
	        logger.info(f"[✅] Saved the fields of {notification_id} to Redis Hash")
    except Exception as e:
        logger.debug(f"[⛔] Could not save {notification_id} to Redis: ", e)




async def process_notification_message(message: aio_pika.IncomingMessage):
	"""Consumes the status report from the queue and updates Redis"""
	logger.info(f"Attempting to process notification message")
	async with message.process():
		try:
			# Deserialize the message body into a Python object for Pydantic Field Validation
			message_body = message.body.decode()
			status_update = NotificationStatus().model_validate_json(message_body)

			# JSON dump of the validated Pydantic
			status_update_json = status_update.model_dump(mode='json')

			# Excecute the Redis notification update
			await set_notification(status_update.notification_id, status_update_json)
		except Exception as e:
			logger.debug(f"[⛔] Could not process notification message", e)
			raise e

async def get_notification_status():
	pass