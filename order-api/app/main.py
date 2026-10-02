from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.routers import orders
from app.kafka_producer import kafka_producer


@asynccontextmanager
async def lifespan(app: FastAPI):
    kafka_producer.start()
    yield
    kafka_producer.stop()


app = FastAPI(
    title="Order API",
    description="Flash Sale Ticketing - Order API",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(orders.router)


@app.get("/health")
async def health_check():
    return {"status": "healthy"}