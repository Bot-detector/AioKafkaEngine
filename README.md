
# AsyncKafkaEngine
AsyncKafkaEngine is a wrapper around the aiokafka package.
 
its build with the idea of decoupling the reading & batching task from your main application task, this way the queue of batches is always full and reading from kafka is not being blocked by the work of your application logic.

# Installation
Installing the package using uv
```bash
uv add AioKafkaEngine
```
Install the package using pip:
```bash
pip install AioKafkaEngine
```

# Usage
## ConsumerEngine

The ConsumerEngine class manages the consumption of messages from Kafka topics asynchronously and places them into an queue. Your application can consume the queue.
Example

```python
import asyncio
from AioKafkaEngine import ConsumerEngine
from aiokafka import AIOKafkaConsumer
import json

async def work(queue):
    message = await queue.get()
    print(message)

async def main():
    """
    Test that the consumer fetches and processes a single batch of messages.
    """
    test_queue = asyncio.Queue()

    # Using the mock setup, getmany should return two messages
    engine = ConsumerEngine(
        consumer=AIOKafkaConsumer(
            *["test_topic"],
            bootstrap_servers="localhost:9092",
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
    
    # create workers
    workers = [asyncio.create_task(work(queue)) for _ in range(10)]

    # will never exit
    await asyncio.gather(consume_task, *workers)

asyncio.run(main())
```
## ProducerEngine

The ProducerEngine class manages the production of messages to a Kafka topic asynchronously by retrieving messages from an internal queue. It also logs production statistics periodically.
Example

```python
import asyncio
from AioKafkaEngine import ProducerEngine
from aiokafka import AIOKafkaProducer
import json

async def work(queue):
    await queue.put(item={"key": 1})

async def main():
    """
    Test that the consumer fetches and processes a single batch of messages.
    """
    queue = asyncio.Queue()
    await queue.put(item={"key": "k", "key2": 2})

    # Using the mock setup, getmany should return two messages
    engine = ProducerEngine(
        producer=AIOKafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode(),
            acks="all",
        ),
        queue=queue,
        topic="produce_topic",
    )
    await engine.start()

    produce_task = asyncio.create_task(engine.produce())
    
    # create workers
    workers = [asyncio.create_task(work(queue)) for _ in range(10)]

    # will never exit
    await asyncio.gather(produce_task, *workers)
asyncio.run(main())
```

# Contributing
Contributions are welcome! Please submit a pull request or open an issue on GitHub.

# License

This project is licensed under the BSD 2-Clause License.