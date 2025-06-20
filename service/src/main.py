from contextlib import asynccontextmanager

from fastapi import FastAPI
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError
import asyncio

KAFKA_BROKER = "kafka:9092"

async def get_consumer():
    consumer = AIOKafkaConsumer(
        "test-topic",
        bootstrap_servers=KAFKA_BROKER,
        group_id="order-service-group"
    )
    for _ in range(10):
        try:
            await consumer.start()
            return consumer
        except:
            await asyncio.sleep(5)
    else:
        raise RuntimeError("Kafka에 연결할 수 없습니다.")


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