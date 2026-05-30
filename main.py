from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

from configs.config import CONFIGS
from schema.response import ApiResponse
from routers import user as user_routes, product as product_routes, review as review_routes


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Add any startup code here
    yield


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

app.include_router(user_routes.router)
app.include_router(product_routes.router)
app.include_router(review_routes.router)

@app.get("/", response_model=ApiResponse[dict])
async def root():
    return ApiResponse[dict](
        data={"app_name": CONFIGS['APP_NAME']},
        message="Welcome to the API",
        success=True
    )