import json
from models.notification import NotificationRequest


"""
The intention behind the load_request_payload is to have
a centeral place where the request can be processed.
And that is what the function accomplishes.
"""
async def load_request_payload(request_object) -> NotificationRequest:
	request_body = await request_object.body()
	json_body = json.loads(request_body.decode("utf-8"))
	pydantic_response = NotificationRequest(**json_body)
	return pydantic_response