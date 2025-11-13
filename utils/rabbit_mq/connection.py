from aio_pika import connect_robust, RobustConnection, Channel
from contextlib import asynccontextmanager
from typing import Optional
import logging
from fastapi import FastAPI
import asyncio
from aio_pika import Channel
import os

from utils.redis.redis_utils import initialize_redis_client, process_notification_message

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

#  Meant for the Exchange Worker
STATUS_EXCHANGE_NAME = "status.update.exchange"
STATUS_QUEUE = "status.updates"


# Where N --> Number
MAX_N_RETRIES = 7
MAX_TIMEOUT_BEFORE_RETRY = 0.5
"""A function that servers the purpose of returning the RABBIT_MQ channel, and keeps checking if its been created for an N number of times"""
async def get_channel_with_retries() -> Optional[Channel]:
    """Get or create RabbitMQ channel with retries"""
    global RABBITMQ_CHANNEL  # Add this line
    
    for current_try in range(MAX_N_RETRIES):
        if RABBITMQ_CHANNEL and not RABBITMQ_CHANNEL.is_closed:
            return RABBITMQ_CHANNEL
        
        # Try to create channel if it doesn't exist
        try:
            if RABBITMQ_CONNECTION and not RABBITMQ_CONNECTION.is_closed:
                RABBITMQ_CHANNEL = await RABBITMQ_CONNECTION.channel()
                if RABBITMQ_CHANNEL:
                    # Re-declare exchange if needed
                    await RABBITMQ_CHANNEL.declare_exchange(name="notifications.direct", type="direct", durable=True)
                    return RABBITMQ_CHANNEL
        except Exception as e:
            logger.warning(f"Failed to create channel (attempt {current_try + 1}): {e}")
        
        if current_try < MAX_N_RETRIES - 1:
            await asyncio.sleep(MAX_TIMEOUT_BEFORE_RETRY)
    
    return None


"""A function that serves the purpose of returing a RabbitMQ connection"""
async def get_rabbitmq_connection():
    global RABBITMQ_CONNECTION
    for current_try in range(MAX_N_RETRIES):
	    if RABBITMQ_CONNECTION:
		    return RABBITMQ_CONNECTION
	    await asyncio.sleep(MAX_TIMEOUT_BEFORE_RETRY)