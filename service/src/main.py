from contextlib import asynccontextmanager

from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import asyncio
import json
import time


KAFKA_BROKER = "localhost:29092"

async def get_consumer():
    consumer = AIOKafkaConsumer(
        "test-topic",
        bootstrap_servers=KAFKA_BROKER,
        group_id="order-service-group"
    )
    await consumer.start()
    return consumer


async def get_producer():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    return producer


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
