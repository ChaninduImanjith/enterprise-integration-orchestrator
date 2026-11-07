import asyncio
import json
import logging
from typing import Any, Dict, Optional
import aio_pika
from aio_pika import ExchangeType, Message, DeliveryMode
from app.core.config import settings

logger = logging.getLogger(__name__)

class RabbitMQManager:
    def __init__(self):
        self.connection: Optional[aio_pika.RobustConnection] = None
        self.channel: Optional[aio_pika.RobustChannel] = None
        self.workflow_exchange: Optional[aio_pika.RobustExchange] = None
        self.dlq_exchange: Optional[aio_pika.RobustExchange] = None

    async def connect(self):
        retries = settings.RETRY_COUNT
        while retries > 0:
            try:
                self.connection = await aio_pika.connect_robust(settings.RABBITMQ_URL)
                self.channel = await self.connection.channel()
                await self.channel.set_qos(prefetch_count=10)
                
                # Setup Topologies (Exchange, Queues, DLQ)
                await self._setup_topologies()
                
                logger.info("Successfully connected to RabbitMQ broker and initialized topologies.")
                return
            except Exception as e:
                retries -= 1
                logger.warning(
                    f"Waiting for RabbitMQ... ({settings.RETRY_COUNT - retries}/{settings.RETRY_COUNT}). Error: {e}"
                )
                await asyncio.sleep(settings.RETRY_DELAY)
        
        logger.error("Could not connect to RabbitMQ broker after max retries.")
        raise ConnectionError("Failed to connect to RabbitMQ broker.")

    async def _setup_topologies(self):
        """Declare main exchange, dead letter exchange, and binding queues."""
        if not self.channel:
            raise RuntimeError("Cannot setup topologies without an active channel.")

        # 1. Setup DLQ Exchange & Queue
        self.dlq_exchange = await self.channel.declare_exchange(
            settings.DLQ_EXCHANGE,
            ExchangeType.DIRECT,
            durable=True
        )
        dlq_queue = await self.channel.declare_queue(
            settings.DLQ_QUEUE,
            durable=True
        )
        await dlq_queue.bind(self.dlq_exchange, routing_key=settings.DLQ_ROUTING_KEY)

        # 2. Setup Main Workflow Exchange
        self.workflow_exchange = await self.channel.declare_exchange(
            settings.WORKFLOW_EXCHANGE,
            ExchangeType.TOPIC,
            durable=True
        )

        # 3. Setup Order Saga Queue with Dead-Lettering configured
        order_queue = await self.channel.declare_queue(
            settings.ORDER_SAGA_QUEUE,
            durable=True,
            arguments={
                "x-dead-letter-exchange": settings.DLQ_EXCHANGE,
                "x-dead-letter-routing-key": settings.DLQ_ROUTING_KEY,
            }
        )
        await order_queue.bind(
            self.workflow_exchange,
            routing_key=settings.ORDER_SAGA_ROUTING_KEY
        )

    async def publish(
        self,
        routing_key: str,
        message: Dict[str, Any],
        correlation_id: Optional[str] = None,
        exchange_name: Optional[str] = None
    ):
        """
        Publishes a message asynchronously to RabbitMQ with persistent delivery.
        """
        if not self.channel or self.channel.is_closed:
            raise RuntimeError("RabbitMQ channel is not open. Ensure connect() was called.")

        exchange = (
            self.workflow_exchange
            if not exchange_name or exchange_name == settings.WORKFLOW_EXCHANGE
            else await self.channel.get_exchange(exchange_name)
        )

        body = json.dumps(message, default=str).encode("utf-8")
        
        pika_message = Message(
            body=body,
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            correlation_id=correlation_id,
            headers={"producer": settings.PROJECT_NAME}
        )

        await exchange.publish(pika_message, routing_key=routing_key)
        logger.info(f"Published message to exchange '{exchange.name}' with routing key '{routing_key}'")

    async def close(self):
        """Gracefully close channel and connection."""
        if self.channel and not self.channel.is_closed:
            await self.channel.close()
        if self.connection and not self.connection.is_closed:
            await self.connection.close()
        logger.info("RabbitMQ connection closed.")

rabbitmq_client = RabbitMQManager()
