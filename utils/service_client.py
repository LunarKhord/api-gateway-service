import aiohttp
import logging
from typing import Optional, Dict, Any
from .circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from utils.etcd_service import etcd_service


logger = logging.getLogger(__name__)

class ServiceClient:
    """HTTP client with circuit breaker and service discovery"""
    
    def __init__(self, service_name: str):
        self.service_name = service_name
        self.circuit_breaker = CircuitBreaker(
            failure_threshold=3,
            recovery_timeout=30,
            expected_exception=aiohttp.ClientError
        )
    
    async def _get_service_url(self) -> Optional[str]:
        """Get service URL from etcd"""
        return etcd_service.get_service_url(self.service_name)
    
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Make HTTP request with circuit breaker protection"""
        service_url = await self._get_service_url()
        if not service_url:
            raise Exception(f"Service {self.service_name} not found in etcd")
        
        url = f"{service_url}{endpoint}"
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.request(method, url, **kwargs) as response:
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientError as e:
                logger.error(f"Request to {self.service_name} failed: {e}")
                raise e
    
    async def get(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """GET request with circuit breaker"""
        try:
            return await self.circuit_breaker.call(self._make_request, "GET", endpoint, **kwargs)
        except CircuitBreakerOpenException:
            logger.error(f"Circuit breaker open for {self.service_name}")
            raise Exception(f"Service {self.service_name} is currently unavailable")
    
    async def post(self, endpoint: str, **kwargs) -> Dict[str, Any]:
        """POST request with circuit breaker"""
        try:
            return await self.circuit_breaker.call(self._make_request, "POST", endpoint, **kwargs)
        except CircuitBreakerOpenException:
            logger.error(f"Circuit breaker open for {self.service_name}")
            raise Exception(f"Service {self.service_name} is currently unavailable")

# Service clients
user_service_client = ServiceClient("user-service")
template_service_client = ServiceClient("template-service")

async def get_user_service_client():
    return user_service_client

async def get_template_service_client():
    return template_service_client