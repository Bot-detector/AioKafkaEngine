import asyncio
from aiokafka import AIOKafkaConsumer
import logging

logger = logging.getLogger(__name__)


class ConsumerEngine:
    def __init__(
        self, consumer: AIOKafkaConsumer, queue: asyncio.Queue, batch_size=10, timeout=5
    ):
        """
        Initialize the Kafka ConsumerEngine.
        :param consumer: An instance of AIOKafkaConsumer.
        :param queue: asyncio.Queue to push consumed messages.
        :param batch_size: Max number of messages to fetch at a time.
        :param timeout: Max time (in seconds) to wait before pushing messages to the queue.
        """
        self.consumer = consumer
        self.queue = queue
        self.batch_size = batch_size
        self.timeout = timeout

    async def start(self):
        """
        Start the Kafka consumer.
        """
        await self.consumer.start()
        logger.info("Consumer started.")

    async def stop(self):
        """
        Stop the Kafka consumer.
        """
        await self.consumer.stop()
        logger.info("Consumer stopped.")

    async def consume(self):
        """
        Consume messages from Kafka and fill the asyncio.Queue.
        """

        while True:
            # Fetch records in batches using getmany
            records = await self.consumer.getmany(
                timeout_ms=self.timeout * 1000,
                max_records=self.batch_size,
            )
            batch = []
            for tp, messages in records.items():
                if not messages:
                    continue

                for message in messages:
                    batch.append(message)

                # Commit progress only for this partition
                await self.consumer.commit({tp: messages[-1].offset + 1})
                # ~aiokafka.errors.CommitFailedError
                # If membership already changed on broker.

                # ~aiokafka.errors.IllegalOperation
                # If used with group_id == None.

                # ~aiokafka.errors.IllegalStateError
                # If partitions not assigned.

                # ~aiokafka.errors.KafkaError
                # If commit failed on broker side. This could be due to invalid offset, too long metadata, authorization failure, etc.

                # ValueError
                # If offsets is of wrong format.

            # Push batch to the queue if not empty
            if batch:
                logger.info(f"Pushing {len(batch)} messages to queue.")
                await self.queue.put(batch)
