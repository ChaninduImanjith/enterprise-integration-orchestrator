import aio_pika
import os
import asyncio

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

class RabbitMQManager:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def connect(self):
        retries = 5
        while retries > 0:
            try:
                self.connection = await aio_pika.connect_robust(RABBITMQ_URL)
                self.channel = await self.connection.channel()
                print(" Successfully connected to RabbitMQ Broker")
                return
            except Exception as e:
                retries -= 1
                print(f"Waiting for RabbitMQ... ({5 - retries}/5). Error: {e}")
                await asyncio.sleep(3)
        raise Exception("Could not connect to RabbitMQ")

    async def close(self):
        if self.connection:
            await self.connection.close()

rabbitmq_client = RabbitMQManager()