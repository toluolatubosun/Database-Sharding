from uuid import UUID
from fastapi import APIRouter, HTTPException, status

from models.user import User
from schema.user import UserCreate
from schema.response import ApiResponse
from libraries.redis import redis_client
from database import router as shard_router, get_session_shard


router = APIRouter(prefix="/users", tags=["users"])


def _email_registry_key(email: str) -> str:
    return f"email:{email}"


@router.post("", response_model=ApiResponse[User], status_code=status.HTTP_201_CREATED)
def create_user(payload: UserCreate) -> ApiResponse[User]:
    email = payload.email.lower()
    new_user = User(name=payload.name, email=email)

    # Claim the email globally via Redis SETNX BEFORE touching any shard.
    # Postgres unique constraints only protect within a single shard; this is what keeps email unique across the whole cluster.
    claim_key = _email_registry_key(email)
    claimed = redis_client.set(claim_key, str(new_user.id), nx=True)
    if not claimed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Email {payload.email!r} already in use",
        )

    # During DUAL_WRITE this is two shards if the user's key is moving, otherwise one.
    # If any write fails, release the Redis claim so the email isn't burned.
    shards = shard_router.shard_for(str(new_user.id), "WRITE")
    try:
        for index, shard in enumerate(shards):
            # Fresh transient instance per shard
            instance = User(**new_user.model_dump())
            with get_session_shard(shard) as session:
                session.add(instance)
                session.commit()
                # Refresh the instance after the last DB commit to get any DB defaults
                if index == len(shards) - 1:
                    session.refresh(instance)
                    new_user = instance
    except Exception:
        redis_client.delete(claim_key)
        raise

    return ApiResponse[User](
        data=new_user,
        message=f"User created on {', '.join(shards)}",
        success=True,
    )


@router.get("/{user_id}", response_model=ApiResponse[User])
def get_user(user_id: UUID) -> ApiResponse[User]:
    shard = shard_router.shard_for(str(user_id))
    with get_session_shard(shard) as session:
        user = session.get(User, user_id)
        if user is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"User {user_id} not found",
            )

    return ApiResponse[User](
        data=user,
        message=f"User fetched from {shard}",
        success=True,
    )
