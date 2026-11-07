import os
from pathlib import Path
from pydantic import BaseModel, Field
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent.parent
load_dotenv(BASE_DIR / ".env")

class Settings(BaseModel):
    PROJECT_NAME: str = Field(default=os.getenv("PROJECT_NAME", "Enterprise Integration Orchestrator"))
    VERSION: str = Field(default=os.getenv("VERSION", "1.0.0"))
    DEBUG: bool = Field(default=os.getenv("DEBUG", "False").lower() in ("true", "1", "yes"))
    
    # RabbitMQ Connection & Topologies
    RABBITMQ_URL: str = Field(default=os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"))
    WORKFLOW_EXCHANGE: str = Field(default=os.getenv("WORKFLOW_EXCHANGE", "saga.orchestrator.exchange"))
    ORDER_SAGA_QUEUE: str = Field(default=os.getenv("ORDER_SAGA_QUEUE", "saga.orders.queue"))
    ORDER_SAGA_ROUTING_KEY: str = Field(default=os.getenv("ORDER_SAGA_ROUTING_KEY", "saga.order.start"))
    
    DLQ_EXCHANGE: str = Field(default=os.getenv("DLQ_EXCHANGE", "saga.dlx.exchange"))
    DLQ_QUEUE: str = Field(default=os.getenv("DLQ_QUEUE", "saga.dlx.queue"))
    DLQ_ROUTING_KEY: str = Field(default=os.getenv("DLQ_ROUTING_KEY", "saga.dlx"))
    
    RETRY_COUNT: int = Field(default=int(os.getenv("RETRY_COUNT", "5")))
    RETRY_DELAY: int = Field(default=int(os.getenv("RETRY_DELAY", "3")))
    
    # Llama 3 API Settings (for Phase 3)
    LLAMA_API_BASE_URL: str = Field(default=os.getenv("LLAMA_API_BASE_URL", "https://api.groq.com/openai/v1"))
    LLAMA_API_KEY: str = Field(default=os.getenv("LLAMA_API_KEY", ""))
    LLAMA_MODEL: str = Field(default=os.getenv("LLAMA_MODEL", "llama3-70b-8192"))

settings = Settings()
