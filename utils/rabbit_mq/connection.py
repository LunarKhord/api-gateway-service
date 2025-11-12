from aio_pika import connect_robust, RobustConnection, Channel
from contextlib import asynccontextmanager
from typing import Optional
import logging
from fastapi import FastAPI
import asyncio
from aio_pika import Channel

from utils.redis.redis_utils import initialize_redis_client

"""
Component,Example Value,Role
Exchange,notifications.direct,Receives all messages.
Queue,email.service.queue,The receiver.
Binding Key,email,"Tells the queue: I only want messages routed with the key email."
Routing Key,email,"Tells the Exchange: Deliver this message to the queue bound with the key email."
"""

# Setup up Logger
logger = logging.getLogger("uvicorn.error")
RABBITMQ_CONNECTION: Optional[RobustConnection] = None
RABBITMQ_CHANNEL: Optional[Channel] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
	"""
	FastAPI lifespan function intended to secure a connection the rabbitmq server before allowing any request to come in
	"""
	global RABBITMQ_CONNECTION, RABBITMQ_CHANNEL
	logger.info("[x] Attempting a connecting to the RabbitMQ server.")
	try:
		# using connect_robust handles, connection re-attempts.
		RABBITMQ_CONNECTION = await connect_robust("amqp://guest:guest@localhost/", client_properties={"connection_name": "API_Gateway_Publisher"})

		# Create a channel for publishing
		RABBITMQ_CHANNEL = await RABBITMQ_CONNECTION.channel()

		# Declare an Exchange, it acts as the centeral hub for all incoming messages
		await RABBITMQ_CHANNEL.declare_exchange(name="notifications.direct",type="direct",durable=True)
		
		logger.info(" [x] Successfully connected to RabbitMQ and declared an exchange.")
	except Exception as e:
		logger.error(f"[x] A fatail error occured, during RabbitMQ connection stage;Have a look see: ", e)
	
	# A Connection attempt for Redis on startup
	try:
		logger.info("[x] Attempting a connecting to the Redis server.")
		await initialize_redis_client()
	except Exception as e:
		logger.error(f"[x] A fatail error occured, during Redis connection stage;Have a look see: ", e)
	

	yield
	
	# A shutdown logic for RabbitMQ
	logger.info(" [x] We're DONE here, time to gracefully shutdown RabbitMQ, Attempting..........")
	if RABBITMQ_CONNECTION:
		await RABBITMQ_CONNECTION.close()
		logger.info("[x] RabbitMQ was closed Successfully.Thank You....")





# Where N --> Number
MAX_N_RETRIES = 7
MAX_TIMEOUT_BEFORE_RETRY = 0.5
"""A function that servers the purpose of returning the RABBIT_MQ channel, and keeps checking if its been created for an N number of times"""
async def get_channel_with_retries() -> Optional[Channel]:
	for current_try in range(MAX_N_RETRIES):
		if RABBITMQ_CHANNEL:
			return RABBITMQ_CHANNEL
		await asyncio.sleep(MAX_TIMEOUT_BEFORE_RETRY)


"""A function that serves the purpose of returing a RabbitMQ connection"""
async def get_rabbitmq_connection():
	for current_try in range(MAX_N_RETRIES):
		if RABBITMQ_CONNECTION:
			return RABBITMQ_CONNECTION
		await asyncio.sleep(MAX_TIMEOUT_BEFORE_RETRY)