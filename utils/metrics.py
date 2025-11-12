from prometheus_client import Counter, Histogram

REQUEST_COUNT = Counter("http_requests_total","Total HTTP requests processed",["method", "endpoint", "status"])

REQUEST_LATENCY = Histogram('http_request_duration_seconds', 'HTTP request latency in seconds', ['method', 'endpoint'])