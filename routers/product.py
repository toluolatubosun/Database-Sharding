from uuid import UUID
from fastapi import APIRouter, HTTPException, status

from models.product import Product
from schema.product import ProductCreate
from schema.response import ApiResponse
from database import get_session_shard


router = APIRouter(prefix="/products", tags=["products"])


@router.post("/", response_model=ApiResponse[Product], status_code=status.HTTP_201_CREATED)
def create_product(payload: ProductCreate) -> ApiResponse[Product]:
    new_product = Product(**payload.model_dump())
    with get_session_shard("global") as session:
        session.add(new_product)
        session.commit()
        session.refresh(new_product)

    return ApiResponse[Product](
        data=new_product,
        message="Product created on global db",
        success=True,
    )


@router.get("/{product_id}", response_model=ApiResponse[Product])
def get_product(product_id: UUID) -> ApiResponse[Product]:
    with get_session_shard("global") as session:
        product = session.get(Product, product_id)
        if product is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Product {product_id} not found",
            )

    return ApiResponse[Product](
        data=product,
        message="Product fetched from global db",
        success=True,
    )
