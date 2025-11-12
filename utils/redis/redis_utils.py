import redis.asyncio as redis 
from redis.asyncio import Redis
import json
from fastapi_limiter import FastAPILimiter


async def initialize_redis_client():
	global REDIS_CLIENT
	try:
		REDIS_CLIENT = redis.from_url("redis://localhost:6379", decode_responses=True)
		await REDIS_CLIENT.ping()
		await FastAPILimiter.init(REDIS_CLIENT)
		return REDIS_CLIENT
		print("[✅] Redis client was initialized successfully")
	except Exception as e:
		print(f"[⛔] Redis client was not initialized successfully: ", e)



async def add_to_map(notification_id, notification_payload):
    try:
        # Use HSET with the mapping argument (best practice for Redis hashes)
        # Keys and values in the dict are automatically handled by redis-py
        if REDIS_CLIENT:
	        await REDIS_CLIENT.hset(
	            notification_id, 
	            mapping=notification_payload
	        )
	        print(f"[✅] Saved the fields of {notification_id} to Redis Hash")
    except Exception as e:
        print(f"[⛔] Could not save {notification_id} to Redis: ", e)


async def update_map_with_key():
	pass

async def delete_from_map_with_key():
	pass