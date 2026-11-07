import asyncio
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.core.config import settings
from app.core.rabbitmq import rabbitmq_client
from app.api.webhooks import router as webhooks_router
from app.services.orchestrator import orchestrator_service

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if settings.DEBUG else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Connect to RabbitMQ & setup topologies
    logger.info("Initializing Enterprise Integration Orchestrator...")
    consumer_task = None
    try:
        await rabbitmq_client.connect()
        # Start the background Saga consumer task
        consumer_task = asyncio.create_task(orchestrator_service.start_consumer())
        logger.info("Saga background consumer task initiated.")
    except Exception as e:
        logger.error(f"Initial RabbitMQ connection failed: {e}. Orchestrator will run in degraded mode until broker is available.")
    
    yield
    
    # Shutdown: Stop consumer and close RabbitMQ connection
    logger.info("Shutting down Enterprise Integration Orchestrator...")
    orchestrator_service.stop_consumer()
    if consumer_task and not consumer_task.done():
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
    await rabbitmq_client.close()

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description="Event-Driven Microservices Saga Orchestrator with RabbitMQ, AI Schema Mapping, and Fault Tolerance.",
    lifespan=lifespan
)

# Include Routers
app.include_router(webhooks_router)

@app.get("/health", tags=["System"])
async def health_check():
    """System health check endpoint verifying broker connectivity."""
    is_connected = (
        rabbitmq_client.connection is not None
        and not rabbitmq_client.connection.is_closed
        and rabbitmq_client.channel is not None
        and not rabbitmq_client.channel.is_closed
    )
    return {
        "status": "healthy" if is_connected else "degraded",
        "service": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "rabbitmq": "connected" if is_connected else "disconnected"
    }
