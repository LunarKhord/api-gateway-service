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
from utils.service_client import user_service_client, template_service_client
from utils.redis.redis_utils import get_notification_status
from utils.redis.redis_utils import is_request_processed, mark_request_processed
from utils.etcd_service import etcd_service

app = FastAPI(title="Notification API Gateway", lifespan=lifespan)
app.add_middleware(MetricsMiddleWare)
"""
# TODO:
# I was thinking since the endpoint /notifications
# relies so much on rabbit mq running
# i make sure a startup is made to make sure rabbitmq and the postgres db are both running before anything else
"""

@app.on_event("startup")
async def startup_event():
    """Register service with etcd on startup"""
    await etcd_service.register_service("api-gateway", "api-gateway-001", "0.0.0.0", 8000)

@app.on_event("shutdown")
async def shutdown_event():
    """Deregister service from etcd on shutdown"""
    await etcd_service.deregister_service("api-gateway", "api-gateway-001")

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
async def notification(request: Request):
    """
    The entry point for all incoming notifications
    """
    
    # Validation: A validation is performed after the JWT checks out on the Request.body()
    try:
        response = await load_request_payload(request)
        
        # Idempotency check
        if await is_request_processed(response.request_id):
            logger.info(f"Duplicate request detected: {response.request_id}")
            return JSONResponse(
                status_code=200,  # Return 200 for idempotent requests
                content={
                    "success": True,
                    "message": "Request already processed",
                    "request_id": response.request_id
                }
            )
        
        # Mark request as being processed
        await mark_request_processed(response.request_id)
        
        # Use circuit breaker to get user preferences
        try:
            user_data = await user_service_client.get(f"/users/{response.user_id}")
            if not user_data.get("data", {}).get("preferences", {}).get(response.notification_type, True):
                return JSONResponse(
                    status_code=400, 
                    content={"error": f"User has disabled {response.notification_type} notifications"}
                )
        except Exception as e:
            logger.warning(f"Could not verify user preferences: {e}. Proceeding with notification.")
        
        # Use circuit breaker to get template
        try:
            template_data = await template_service_client.get(f"/templates/name/{response.template_code}")
            template = template_data.get("data", {})
        except Exception as e:
            logger.error(f"Could not fetch template {response.template_code}: {e}")
            return JSONResponse(status_code=500, content={"error": "Template service unavailable"})
        
        # Routing Logic: Using the Enum as a means of determining what Queue to place the payload
        if response.notification_type == NotificationTypeEnum.EMAIL:
            RABBITMQ_CHANNEL = await get_channel_with_retries()
            await publish_email_message(RABBITMQ_CHANNEL, response.model_dump_json(), response.priority)
        elif response.notification_type == NotificationTypeEnum.PUSH:
            RABBITMQ_CHANNEL = await get_channel_with_retries()
            await publish_push_message(RABBITMQ_CHANNEL, response.model_dump_json(), response.priority)

    except Exception as e:
        print(e)
        return JSONResponse(status_code=422, content={ "error": {"message": "Invalid payload"}})

@app.get("/api/v1/notifications/{notification_id}")
async def get_notification(notification_id: str):
    """
    Retrieve the current status of a notification
    """
    try:
        status = await get_notification_status(notification_id)
        if status:
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "data": status,
                    "message": "Notification status retrieved successfully"
                }
            )
        else:
            return JSONResponse(
                status_code=404,
                content={
                    "success": False,
                    "error": "Notification not found",
                    "message": "No notification found with the provided ID"
                }
            )
    except Exception as e:
        logger.error(f"Error retrieving notification status: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "Internal server error",
                "message": "Failed to retrieve notification status"
            }
        )

