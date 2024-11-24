import asyncio
import pytest
from AioKafkaEngine import ProducerEngine
from aiokafka import AIOKafkaProducer
import json


@pytest.mark.asyncio
async def test_consume_single_batch():
    """
    Test that the consumer fetches and processes a single batch of messages.
    """
    test_queue = asyncio.Queue()
    await test_queue.put(item={"key": "k", "key2": 2})

    # Using the mock setup, getmany should return two messages
    engine = ProducerEngine(
        producer=AIOKafkaProducer(
            bootstrap_servers="localhost:9094",
            value_serializer=lambda v: json.dumps(v).encode(),
            acks="all",
        ),
        queue=test_queue,
        topic="produce_topic",
    )
    await engine.start()

    produce_task = asyncio.create_task(engine.produce())

    # Allow the consumer loop to run once
    await asyncio.sleep(2)

    # Check if the messages were added to the queue
    assert test_queue.empty()

    # Cancel the task to stop the test
    produce_task.cancel()
    await engine.stop()
