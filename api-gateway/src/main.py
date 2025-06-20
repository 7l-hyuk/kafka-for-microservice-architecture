from fastapi import FastAPI
from aiokafka import AIOKafkaProducer

app = FastAPI()

KAFKA_BROKER = "kafka:9092"

async def get_producer():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    return producer


@app.post("/create-order/")
async def create_order(iter: int):
    producer = await get_producer()

    try:
        for i in range(iter):
            message = f"message{i}".encode("utf-8")
            await producer.send_and_wait("test-topic", message)
            print(f"Sent: {message}")
        return {"status": "Order request sent to Kafka"}
    finally:
        await producer.stop()
