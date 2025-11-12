from utils.metrics import REQUEST_COUNT, REQUEST_LATENCY
import time
from fastapi import FastAPI, Request
from starlette.middleware.base import BaseHTTPMiddleware
from prometheus_client import generate_latest # Import for exposing metrics


class MetricsMiddleWare(BaseHTTPMiddleware):
	async def dispatch(self, request: Request, call_next):
		start_time = time.time()


		try:
			response = await call_next(request)
		except Exception as e:
			REQUEST_COUNT.labels(
				method=request.method,
				endpoint=request.url.path,
				status="500").inc()
			raise e

		# Record the metrics
		process_time = time.time() - start_time

		labels = {
			"method": request.method,
			"endpoint":request.url.path
		}

		REQUEST_COUNT.labels(
			status=str(response.status_code),
			**labels
			).inc()

		REQUEST_LATENCY.labels(**labels).observe(process_time)

		return response