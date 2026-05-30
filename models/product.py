import uuid
from uuid import UUID
from decimal import Decimal
from sqlalchemy import Numeric
from sqlmodel import Field
from models.base import TimestampedModel

class Product(TimestampedModel, table=True):
    id: UUID = Field(primary_key=True, default_factory=uuid.uuid4)
    name: str = Field()
    price: Decimal = Field(sa_type=Numeric(10, 2))
    description: str = Field()
