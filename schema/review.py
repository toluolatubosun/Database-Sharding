from datetime import datetime
from uuid import UUID
from pydantic import BaseModel, Field

from schema.user import UserSummary


class ReviewCreate(BaseModel):
    user_id: UUID
    product_id: UUID
    title: str
    content: str
    rating: int = Field(ge=1, le=5)


class ReviewWithUser(BaseModel):
    id: UUID
    title: str
    content: str
    rating: int
    user_id: UUID
    user: UserSummary
    product_id: UUID
    created_at: datetime
    updated_at: datetime


class ReviewCursor(BaseModel):
    created_at: datetime
    id: UUID
