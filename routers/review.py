import base64
from uuid import UUID
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter, HTTPException, Query, status
from sqlmodel import select
from sqlalchemy import tuple_

from models.user import User
from models.product import Product
from models.review import Review
from schema.user import UserSummary
from schema.review import ReviewCreate, ReviewCursor, ReviewWithUser
from schema.response import ApiResponse, PaginatedResponse
from database import router as shard_router, get_session_shard


router = APIRouter(tags=["reviews"])


def _encode_cursor(cursor: ReviewCursor) -> str:
    return base64.urlsafe_b64encode(cursor.model_dump_json().encode()).decode()


def _decode_cursor(encoded: str) -> ReviewCursor:
    return ReviewCursor.model_validate_json(base64.urlsafe_b64decode(encoded.encode()))


@router.post("/reviews", response_model=ApiResponse[Review], status_code=status.HTTP_201_CREATED)
def create_review(payload: ReviewCreate) -> ApiResponse[Review]:
    # Reviews and Products live on different DBs, so there is no
    # FK constraint to lean on. Validate at the app layer instead.
    with get_session_shard("global") as session:
        if session.get(Product, payload.product_id) is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Product {payload.product_id} not found",
            )

    new_review = Review(**payload.model_dump())

    # During DUAL_WRITE this is two shards if the user's key is moving, otherwise one.
    shards = shard_router.shard_for(str(payload.user_id), "WRITE")

    for index, shard in enumerate(shards):
        # Fresh transient instance per shard
        instance = Review(**new_review.model_dump())
        with get_session_shard(shard) as session:
            # User and Review colocate by user_id, so the user MUST be on the current shard if they exist at all.
            # Only check on the first shard, that's the source of truth for reads.
            if index == 0 and session.get(User, payload.user_id) is None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"User {payload.user_id} not found",
                )
            session.add(instance)
            session.commit()
            # Refresh the instance after the last DB commit to get any DB defaults
            if index == len(shards) - 1:
                session.refresh(instance)
                new_review = instance

    return ApiResponse[Review](
        data=new_review,
        message=f"Review created on {', '.join(shards)} db",
        success=True,
    )


@router.get("/products/{product_id}/reviews", response_model=PaginatedResponse[ReviewWithUser])
def reviews_for_product(product_id: UUID, cursor: Optional[str] = Query(default=None), limit: int = Query(default=10, ge=1, le=100)) -> PaginatedResponse[ReviewWithUser]:
    try:
        decoded = _decode_cursor(cursor) if cursor else None
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid cursor",
        )

    shards = shard_router.all_shards()

    # Each shard answers "give me top N reviews for this product before the cursor" independently. We then merge and re-slice to N.
    # User and Review colocate on the same shard, so the JOIN is local, no cross-shard lookup needed.
    def query_shard(shard: str) -> list[ReviewWithUser]:
        with get_session_shard(shard) as session:
            stmt = (
                select(Review, User.name)
                .join(User, Review.user_id == User.id)
                .where(Review.product_id == product_id)
            )
            if decoded:
                stmt = stmt.where(
                    tuple_(Review.created_at, Review.id) <
                    tuple_(decoded.created_at, decoded.id)
                )
            stmt = stmt.order_by(Review.created_at.desc(), Review.id.desc()).limit(limit)
            return [
                ReviewWithUser(**review.model_dump(), user=UserSummary(id=review.user_id, name=user_name))
                for review, user_name in session.exec(stmt).all()
            ]

    # Query shards in parallel to speed up the fan-out.
    with ThreadPoolExecutor(max_workers=len(shards)) as pool:
        per_shard = list(pool.map(query_shard, shards))

    # unpack results from all shards, merge-sort by created_at desc, id desc, and take the top N.
    merged = [review for shard_results in per_shard for review in shard_results]
    merged.sort(key=lambda r: (r.created_at, r.id), reverse=True)
    page = merged[:limit]

    next_cursor: Optional[str] = None
    if len(page) == limit:
        last = page[-1]
        next_cursor = _encode_cursor(ReviewCursor(created_at=last.created_at, id=last.id))

    return PaginatedResponse[ReviewWithUser](
        data=page,
        next_cursor=next_cursor,
        message=f"Merged {len(shards)} shards, {len(page)} reviews",
        success=True,
    )
