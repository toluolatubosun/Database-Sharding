from sqlmodel import Field, SQLModel
from sqlalchemy import event, DateTime
from datetime import datetime, timezone


class TimestampedModel(SQLModel):
    """Base model with auto-generated created_at and updated_at fields."""
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_type=DateTime(timezone=True),
        nullable=False,
    )
    updated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_type=DateTime(timezone=True),
        nullable=False,
    )


# Add event listener to automatically update updated_at field
@event.listens_for(TimestampedModel, 'before_update', propagate=True)
def update_timestamp(mapper, connection, target):
    """Automatically update the updated_at field before any update."""
    target.updated_at = datetime.now(timezone.utc)
