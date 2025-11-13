from fastapi import FastAPI, HTTPException, Request, Depends, status
from fastapi.security import HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
from fastapi_limiter import FastAPILimiter
from fastapi_limiter.depends import RateLimiter
from dotenv import load_dotenv
from contextlib import asynccontextmanager
import os
from uuid import uuid4
from prometheus_client import generate_latest
from starlette.responses import Response
import json
import logging

from models.notification import NotificationTypeEnum, CreateTemplateRequest
from utils.load_request import load_request_payload
from utils.validate_user_token import get_current_jwt
from utils.rabbit_mq.connection import get_channel_with_retries, get_rabbitmq_connection
from utils.rabbit_mq.producer import publish_email_message, publish_push_message
from utils.redis.redis_utils import initialize_redis_client
from middleware.metrics_middleware import MetricsMiddleWare
from utils.service_client import user_service_client, template_service_client
from utils.redis.redis_utils import get_notification_status
from utils.redis.redis_utils import is_request_processed, mark_request_processed
from utils.etcd_service import etcd_service

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events
    """
    # Startup
    logger.info("🚀 Starting API Gateway...")
    
    # Initialize Redis first (required for FastAPILimiter)
    redis_client = await initialize_redis_client()
    await FastAPILimiter.init(redis_client)
    logger.info("✅ Redis initialized and rate limiter configured")
    
    # Register with etcd
    await etcd_service.register_service(
        service_name="api-gateway",
        service_id="api-gateway-001",
        host="api-gateway",
        port=8000
    )
    logger.info("✅ Service registered with etcd")
    
     # Establish RabbitMQ connection (channel will be managed by get_channel_with_retries)
    global RABBITMQ_CONNECTION
    try:
        rabbitmq_url = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
        RABBITMQ_CONNECTION = await connect_robust(rabbitmq_url, client_properties={"connection_name": "API_Gateway_Publisher"})
        logger.info("✅ RabbitMQ connection established in lifespan")
    except Exception as e:
        logger.error(f"❌ Failed to establish RabbitMQ connection in lifespan: {e}")
        raise
    
    
    yield  # Application runs here
    
    # Shutdown
    logger.info("🛑 Shutting down API Gateway...")
    await etcd_service.deregister_service(
        service_name="api-gateway",
        service_id="api-gateway-001"
    )
    logger.info("✅ Service deregistered from etcd")

app = FastAPI(title="Notification API Gateway", lifespan=lifespan)
app.add_middleware(MetricsMiddleWare)

@app.get("/health", status_code=status.HTTP_200_OK)
async def server_health():
    """Endpoint for retrieving the server's status including:
        - RabbitMQ
        - Redis
        - etcd
    """
    health_status = {"status": "Healthy", "dependencies": {}}

    # Health check for RabbitMQ
    try:
        RABBITMQ_CONNECTION = await get_rabbitmq_connection()
        if RABBITMQ_CONNECTION and not RABBITMQ_CONNECTION.is_closed:
            health_status["dependencies"]["rabbit_mq"] = "OK"
        else:
            health_status["dependencies"]["rabbit_mq"] = "DEGRADED"
            health_status["status"] = "Degraded"
    except Exception as e:
        logger.error(f"RabbitMQ health check failed: {e}")
        health_status["dependencies"]["rabbit_mq"] = "DOWN"
        health_status["status"] = "Unhealthy"

    # Health check for Redis
    try:
        REDIS_CLIENT = await initialize_redis_client()
        if REDIS_CLIENT and await REDIS_CLIENT.ping():
            health_status["dependencies"]["redis"] = "OK"
        else:
            health_status["dependencies"]["redis"] = "DEGRADED"
            health_status["status"] = "Degraded"
    except Exception as e:
        logger.error(f"Redis health check failed: {e}")
        health_status["dependencies"]["redis"] = "DOWN"
        health_status["status"] = "Unhealthy"
    
    # Health check for etcd (optional, since it's for service discovery)
    try:
        # Try to discover our own service as a health check
        service_info = etcd_service.discover_service("api-gateway")
        if service_info:
            health_status["dependencies"]["etcd"] = "OK"
        else:
            health_status["dependencies"]["etcd"] = "DEGRADED"
    except Exception as e:
        logger.error(f"etcd health check failed: {e}")
        health_status["dependencies"]["etcd"] = "DOWN"

    return JSONResponse(health_status)


@app.get("/metrics", include_in_schema=False)
async def metrics():
    """Endpoint for Prometheus to scrape metrics."""
    return Response(content=generate_latest(), media_type="text/plain; version=0.0.4")


@app.post(
    "/api/v1/notifications/",
    dependencies=[Depends(RateLimiter(times=1000, seconds=60))],  # 1000 requests per minute
    status_code=status.HTTP_202_ACCEPTED
)
async def notification(request: Request):
    """
    The entry point for all incoming notifications
    """
    
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
            user_info = user_data.get("data", {})
            
            # Check user preferences
            if not user_info.get("preferences", {}).get(response.notification_type, True):
                return JSONResponse(
                    status_code=400, 
                    content={
                        "success": False,
                        "error": f"User has disabled {response.notification_type} notifications",
                        "message": "Notification blocked by user preference"
                    }
                )
            
            # Extract user email for notifications
            user_email = user_info.get("email")
            
        except Exception as e:
            logger.warning(f"Could not verify user preferences: {e}. Proceeding with notification.")
            user_email = None  # Fallback
        
        # Use circuit breaker to get template
        try:
            template_data = await template_service_client.get(f"/templates/name/{response.template_code}")
            template = template_data.get("data", {})
        except Exception as e:
            logger.error(f"Could not fetch template {response.template_code}: {e}")
            return JSONResponse(
                status_code=500,
                content={
                    "success": False,
                    "error": "Template service unavailable",
                    "message": "Failed to retrieve notification template"
                }
            )
        
        # Generate tracking ID
        tracking_id = str(uuid4())
        
        # Routing Logic: Using the Enum to determine which Queue to use
        if response.notification_type == NotificationTypeEnum.EMAIL:
            RABBITMQ_CHANNEL = await get_channel_with_retries()
            
            # Create enhanced payload with user email and tracking ID
            enhanced_payload = response.model_dump(mode='json')
            enhanced_payload["to_email"] = user_email
            enhanced_payload["tracking_id"] = tracking_id
            enhanced_payload["template"] = template  # Include template data
            
            await publish_email_message(
                RABBITMQ_CHANNEL,
                json.dumps(enhanced_payload),
                response.priority,
                tracking_id
            )
            
            return JSONResponse(
                status_code=202,
                content={
                    "success": True,
                    "message": "Email notification accepted for processing",
                    "notification_id": tracking_id,
                    "request_id": response.request_id
                }
            )
            
        elif response.notification_type == NotificationTypeEnum.PUSH:
            RABBITMQ_CHANNEL = await get_channel_with_retries()
            if not RABBITMQ_CHANNEL:
                return JSONResponse(status_code=503, content={"error": "Message queue unavailable"})
            
            tracking_id = str(uuid4())
            await publish_push_message(RABBITMQ_CHANNEL, response.model_dump_json(), response.priority, tracking_id)
            
            return JSONResponse(
                status_code=202,
                content={
                    "success": True,
                    "message": "Push notification accepted for processing",
                    "notification_id": tracking_id,
                    "request_id": response.request_id
                }
            )
        else:
            # Handle unsupported notification types
            return JSONResponse(
                status_code=400,
                content={
                    "success": False,
                    "error": f"Unsupported notification type: {response.notification_type}",
                    "message": "Invalid notification type provided"
                }
            )

    except Exception as e:
        logger.error(f"Error processing notification: {e}", exc_info=True)
        return JSONResponse(
            status_code=422,
            content={
                "success": False,
                "error": "Invalid payload",
                "message": str(e)
            }
        )


@app.get("/api/v1/notifications/{notification_id}")
async def get_notification(notification_id: str):
    """
    Retrieve the current status of a notification
    """
    try:
        status_data = await get_notification_status(notification_id)
        if status_data:
            return JSONResponse(
                status_code=200,
                content={
                    "success": True,
                    "data": status_data,
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


# User Management Endpoints
@app.post("/api/v1/users/")
async def register_user(request: Request):
    """Proxy user registration to user service"""
    return await proxy_to_service(user_service_client, request, "POST", "/auth/register")


@app.post("/api/v1/auth/login")
async def login(request: Request):
    """Proxy login to user service"""
    return await proxy_to_service(user_service_client, request, "POST", "/auth/login")


@app.get("/api/v1/users/")
async def get_users():
    """Get all users from user service"""
    return await user_service_client.get("/users/")


# Template Management Endpoints  
@app.post("/api/v1/templates/")
async def create_template(template_data: CreateTemplateRequest):
    """Create a new notification template"""
    return await template_service_client.post("/templates/", json=template_data.dict())


@app.get("/api/v1/templates/")
async def get_templates():
    """Get all notification templates"""
    return await template_service_client.get("/templates/")


@app.get("/api/v1/templates/{template_id}")
async def get_template(template_id: str):
    """Get a specific template by ID"""
    return await template_service_client.get(f"/templates/{template_id}")


# Helper function
async def proxy_to_service(service_client, request, method, endpoint):
    """Proxy request to individual service"""
    try:
        data = await request.json()
        if method == "POST":
            return await service_client.post(endpoint, json=data)
        elif method == "GET":
            return await service_client.get(endpoint)
        elif method == "PUT":
            return await service_client.put(endpoint, json=data)
        elif method == "DELETE":
            return await service_client.delete(endpoint)
    except Exception as e:
        logger.error(f"Error proxying request to {endpoint}: {e}")
        return JSONResponse(
            status_code=500,
            content={
                "success": False,
                "error": "Service unavailable",
                "message": f"Failed to communicate with downstream service"
            }
        )