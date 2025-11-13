from fastapi import FastAPI, HTTPException, Request, Depends, status
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter
from dotenv import load_dotenv
import os
from uuid import uuid4
from prometheus_client import generate_latest
from starlette.responses import Response



from models.notification import NotificationTypeEnum
from utils.load_request import load_request_payload
from utils.validate_user_token import get_current_jwt
from utils.rabbit_mq.connection import lifespan, get_channel_with_retries, get_rabbitmq_connection
from utils.rabbit_mq.producer import publish_email_message, publish_push_message
from utils.redis.redis_utils import initialize_redis_client
from middleware.metrics_middleware import MetricsMiddleWare


app = FastAPI(title="Notification API Gateway", lifespan=lifespan)
app.add_middleware(MetricsMiddleWare)
"""
# TODO:
# I was thinking since the endpoint /notifications
# relies so much on rabbit mq running
# i make sure a startup is made to make sure rabbitmq and the postgres db are both running before anything else
"""


@app.get("/health", status_code=status.HTTP_200_OK)
async def server_health():
    """Endpoint for retrieving the servers status including the following status of:
        - RabbitMQ
        - Redis
    """
    health_status = {"status": "Healthy", "dependencies": {}}

    # A health check for RabbitMQ
    try:
        RABBITMQ_CONNECTION = await get_rabbitmq_connection()
        if RABBITMQ_CONNECTION and not RABBITMQ_CONNECTION.is_closed:
            health_status["dependencies"]["rabbit_mq"] = "OK"
        else:
            health_status["dependencies"]["rabbit_mq"] = "DEGRADED"
    except Exception as e:
        raise ConnectionError("RabbitMQ, connection issue: ", e)

    # A health check for Redis
    try:
        REDIS_CLIENT = await initialize_redis_client()
        if REDIS_CLIENT and await REDIS_CLIENT.ping():
            health_status["dependencies"]["redis"] = "OK"
        else:
            health_status["dependencies"]["redis"] = "DEGRADED"
    except Exception as e:
        raise ConnectionError("Redis Connection issue: ", e)
    return JSONResponse(health_status)


@app.get("/metrics", include_in_schema=False, status_code=status.HTTP_200_OK)
async def metrics():
    """Endpoint for Prometheus to scrape metrics."""
    return Response(content=generate_latest(), media_type="text/plain; version=0.0.4")



@app.post("/api/v1/notifications/", dependencies=[Depends(RateLimiter(times=1000, seconds=1))], status_code=status.HTTP_202_ACCEPTED)
# , jwt_token: HTTPAuthorizationCredentials=Depends(get_current_jwt)
async def notification(request: Request):
    """
    The entry point for all incoming notifications, POST requests
    """
    
    # Programmer's NOTE: The intention behind using Request, as dependency injection NotificationRequest
    # was so i could have control over the JSONResponse in case of a validation error,
    # without it, FastAPI, automatically handles the messages and response codes.
   
    # Validation: A vaidation is performed on the users JWT token.
    try:
        pass
    except Exception as e:
        raise e

    # Validation: A validation is performed after the JWT checks out on the Request.body()
    try:
        response = await load_request_payload(request)
        # Routing Logic: Using the Enum as a means of determining what Queue to place the payload
        if response.notification_type == NotificationTypeEnum.EMAIL:
            # Place Valid Payload into the Email Queue
            RABBITMQ_CHANNEL = await get_channel_with_retries()
            await publish_email_message(RABBITMQ_CHANNEL, response.model_dump_json(), response.priority)
        elif response.notification_type == NotificationTypeEnum.PUSH:
            # Place Valid Payload into the Push Queue
            await publish_push_message(RABBITMQ_CHANNEL, response.model_dump_json(), response.priority)

            
    except Exception as e:
        print(e)
        return JSONResponse(status_code=422, content={ "error": {"message": "Invalid payload"}})