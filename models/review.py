import uuid
from uuid import UUID
from sqlmodel import Field
from models.base import TimestampedModel

class Review(TimestampedModel, table=True):
    id: UUID = Field(primary_key=True, default_factory=uuid.uuid4)
    title: str = Field()
    content: str = Field()
    rating: int = Field()
    user_id: UUID = Field(foreign_key="user.id")
    product_id: UUID = Field()
