from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.core.rabbitmq import rabbitmq_client

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Connect to RabbitMQ
    await rabbitmq_client.connect()
    yield
    # Shutdown: Close connection
    await rabbitmq_client.close()

app = FastAPI(
    title="Enterprise Integration Orchestrator",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "rabbitmq": "connected" if rabbitmq_client.connection and not rabbitmq_client.connection.is_closed else "disconnected"
    }