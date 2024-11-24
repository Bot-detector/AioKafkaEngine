import asyncio
import pytest
from AioKafkaEngine.ConsumerEngine import (
    ConsumerEngine,
)
from aiokafka import AIOKafkaConsumer
import json


@pytest.mark.asyncio
async def test_consume_single_batch():
    """
    Test that the consumer fetches and processes a single batch of messages.
    """
    test_queue = asyncio.Queue()

    # Using the mock setup, getmany should return two messages
    engine = ConsumerEngine(
        consumer=AIOKafkaConsumer(
            *["test_topic"],
            bootstrap_servers="localhost:9094",
            group_id="my_group",
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
            auto_offset_reset="earliest",
        ),
        queue=test_queue,
        batch_size=10,
        timeout=1,
    )
    await engine.start()
    consume_task = asyncio.create_task(engine.consume())

    # Allow the consumer loop to run once
    await asyncio.sleep(2)

    # Check if the messages were added to the queue
    assert not test_queue.empty()
    batch = await test_queue.get()
    assert len(batch) == 10

    # Cancel the task to stop the test
    consume_task.cancel()
    await engine.stop()
