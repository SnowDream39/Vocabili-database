from fastapi_users import schemas
from pydantic import BaseModel

class UserRead(schemas.BaseUser[int]):
    username: str
    
class UserCreate(schemas.BaseUserCreate):
    password: str
    username: str

class UserId(BaseModel):
    id: int

