import asyncio
from aiokafka import AIOKafkaProducer
import logging

logger = logging.getLogger(__name__)


class ProducerEngine:
    def __init__(
        self,
        producer: AIOKafkaProducer,
        queue: asyncio.Queue,
        topic: str,
    ):
        """
        Initialize the Kafka ProducerEngine.
        :param producer: An instance of AIOKafkaProducer.
        :param queue: asyncio.Queue to fetch messages from.
        :param topic: Kafka topic to produce messages to.
        :param batch_size: Number of messages to send in one batch.
        :param timeout: Maximum time (in seconds) to wait for a batch to fill before sending.
        """
        self.producer = producer
        self.queue = queue
        self.topic = topic

    async def start(self):
        """
        Start the Kafka producer.
        """
        await self.producer.start()
        logger.info("Producer started.")

    async def stop(self):
        """
        Stop the Kafka producer.
        """
        await self.producer.stop()
        logger.info("Producer stopped.")

    async def produce(self):
        """
        Fetch messages from the asyncio.Queue and send them to Kafka in batches.
        """

        while True:
            message = await self.queue.get()
            await self.producer.send_and_wait(self.topic, message)
