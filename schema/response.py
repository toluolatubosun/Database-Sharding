from pydantic import BaseModel
from typing import TypeVar, Generic, Optional

# Generic type for the data field
T = TypeVar('T')

class ApiResponse(BaseModel, Generic[T]):
    """API response structure for all endpoints."""
    data: Optional[T] = None
    message: str
    success: bool


class PaginatedResponse(BaseModel, Generic[T]):
    """API response for cursor-paginated endpoints."""
    data: list[T]
    next_cursor: Optional[str] = None
    message: str
    success: bool
