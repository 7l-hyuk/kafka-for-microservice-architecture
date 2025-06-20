from contextlib import asynccontextmanager

from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio

KAFKA_BROKER = "kafka:9092"

async def get_consumer():
    consumer = AIOKafkaConsumer(
        "test-topic",
        bootstrap_servers=KAFKA_BROKER,
        group_id="order-service-group"
    )
    await consumer.start()
    return consumer


async def consume_orders():
    consumer = await get_consumer()
    try:
        async for msg in consumer:
            print(f"Comsumer Receive: {msg}")
    finally:
        await consumer.stop()


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(consume_orders())
    yield
    
app = FastAPI(lifespan=lifespan)
