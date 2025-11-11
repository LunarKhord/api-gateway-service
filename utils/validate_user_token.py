from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
import jwt
from typing import Dict


# PUBLIC KEY FROM THE USER SERVICE
PUBLIC_KEY="..."

security = HTTPBearer()

def get_current_jwt(credentials: HTTPAuthorizationCredentials = Depends(security)) -> Dict:
	"""
	Validate the JWT and extract the UserID et al.
	"""
	token = credentials.credentials
	print(token)
	try:
		# Decode the token
		payload = jwt.decode(token, PUBLIC_KEY, algorithms=["RS256"])

		# Extract the needed fields for access validation:
		#  - sub
		#  - roles
		user_id = payload.get("sub")
		permissions = payload.get("roles") # Skeptical

		if not user_id:
			raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token payload missing user identification i.e user_id")

		# A Dict is returned containg the payload user_id and permission, then can futher be used to validate
		# Against the DB table.
		return {"user_id": user_id, "permissions": permissions}


	except jwt.ExpiredSignatureError:
		raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired")
	except jwt.InvalidSignatureError:
		raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token signature")
	except Exception:
		raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Could not validate credentials")