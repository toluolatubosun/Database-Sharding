import uuid
from uuid import UUID
from sqlmodel import Field
from models.base import TimestampedModel

class User(TimestampedModel, table=True):
    id: UUID = Field(primary_key=True, default_factory=uuid.uuid4)
    name: str = Field()
    email: str = Field(index=True, unique=True)
