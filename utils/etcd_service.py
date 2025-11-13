import asyncio
import json
import logging
from typing import Optional, Dict, Any
import etcd3

logger = logging.getLogger(__name__)

class EtcdService:
    def __init__(self, host: str = "localhost", port: int = 2379):
        self.client = etcd3.client(host=host, port=port)
        self.service_prefix = "/services/"
        self.lease_ttl = 30  # 30 seconds

    async def register_service(self, service_name: str, service_id: str, host: str, port: int):
        """Register a service with etcd"""
        try:
            # Create a lease for automatic expiration
            lease = self.client.lease(ttl=self.lease_ttl)
            
            service_key = f"{self.service_prefix}{service_name}/{service_id}"
            service_data = {
                "name": service_name,
                "id": service_id,
                "address": host,
                "port": port,
                "registered_at": str(asyncio.get_event_loop().time())
            }
            
            # Store service info
            self.client.put(service_key, json.dumps(service_data), lease=lease)
            
            # Keep lease alive in background
            asyncio.create_task(self._keep_lease_alive(lease))
            
            logger.info(f"✅ Registered {service_name} with etcd")
        except Exception as e:
            logger.error(f"❌ Failed to register with etcd: {e}")

    async def _keep_lease_alive(self, lease):
        """Keep the lease alive to prevent service expiration"""
        while True:
            try:
                lease.refresh()
                await asyncio.sleep(self.lease_ttl // 2)  # Refresh halfway through TTL
            except Exception as e:
                logger.error(f"Failed to refresh lease: {e}")
                break

    async def deregister_service(self, service_name: str, service_id: str):
        """Deregister a service from etcd"""
        try:
            service_key = f"{self.service_prefix}{service_name}/{service_id}"
            self.client.delete(service_key)
            logger.info(f"✅ Deregistered {service_name} from etcd")
        except Exception as e:
            logger.error(f"❌ Failed to deregister from etcd: {e}")

    def discover_service(self, service_name: str) -> Optional[Dict[str, Any]]:
        """Discover a service by name"""
        try:
            service_prefix = f"{self.service_prefix}{service_name}/"
            
            # Get all instances of this service
            services = []
            for value, metadata in self.client.get_prefix(service_prefix):
                if value:
                    service_data = json.loads(value.decode('utf-8'))
                    services.append(service_data)
            
            if services:
                # Return the first healthy service (simple load balancing)
                return services[0]
            return None
        except Exception as e:
            logger.error(f"❌ Failed to discover service {service_name}: {e}")
            return None

    def get_service_url(self, service_name: str) -> Optional[str]:
        """Get the full URL for a service"""
        service = self.discover_service(service_name)
        if service:
            return f"http://{service['address']}:{service['port']}"
        return None

# Global instance
etcd_service = EtcdService()

async def get_etcd_service():
    return etcd_service
