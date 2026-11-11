"""Kafka side of the producer: publishing messages, and finding where it stopped.

The producer keeps no state of its own: the last message of the topic tells
which quarter-hour was published last (see last_published_time).
"""

import logging
from datetime import datetime
from typing import Any, Protocol

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition

from wattlake_streaming.messages import key_time

logger = logging.getLogger(__name__)


class KafkaPublishError(Exception):
    """Messages could not be delivered to Kafka."""


class ProducerLike(Protocol):
    """The part of confluent_kafka.Producer used here (a fake in the tests)."""

    def produce(self, topic: str, value: bytes, key: bytes, on_delivery: Any) -> None: ...
    def poll(self, timeout: float) -> int: ...
    def flush(self, timeout: float) -> int: ...


class ConsumerLike(Protocol):
    """The part of confluent_kafka.Consumer used here (a fake in the tests)."""

    def list_topics(self, topic: str, timeout: float) -> Any: ...
    def get_watermark_offsets(self, partition: TopicPartition, timeout: float) -> tuple[int, int]: ...
    def assign(self, partitions: list[TopicPartition]) -> None: ...
    def poll(self, timeout: float) -> Message | None: ...
    def close(self) -> None: ...


def producer_config(bootstrap_servers: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        "client.id": "wattlake-producer",
        # No duplicates or reordering on retries; implies acks=all, so a
        # message counts as delivered once written by every in-sync replica.
        "enable.idempotence": True,
    }


def consumer_config(bootstrap_servers: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap_servers,
        # Required by the client, but no offset is ever committed: the consumer
        # only reads the last message of each partition, at startup.
        "group.id": "wattlake-producer-cursor",
        "enable.auto.commit": False,
    }


class Publisher:
    """Publishes messages to one topic; publish_all() returns once Kafka has them all."""

    def __init__(self, producer: ProducerLike, topic: str, *, flush_timeout: float = 60.0) -> None:
        self._producer = producer
        self._topic = topic
        self._flush_timeout = flush_timeout

    @classmethod
    def connect(cls, bootstrap_servers: str, topic: str) -> "Publisher":
        return cls(Producer(producer_config(bootstrap_servers)), topic)

    def publish_all(self, messages: list[tuple[bytes, bytes]]) -> None:
        """Send messages in order and wait for every delivery report.

        Raises KafkaPublishError if one message was not delivered: the caller
        must not move its cursor past it.
        """
        errors: list[str] = []

        def on_delivery(err: KafkaError | None, msg: Message) -> None:
            if err is not None:
                errors.append(f"{msg.key()!r}: {err}")

        for key, value in messages:
            while True:
                try:
                    self._producer.produce(self._topic, value=value, key=key, on_delivery=on_delivery)
                    break
                except BufferError:
                    # Local queue full (first run, thousands of rows): let
                    # deliveries free some room, then retry this message.
                    self._producer.poll(1.0)
            self._producer.poll(0)

        remaining = self._producer.flush(self._flush_timeout)
        if remaining:
            raise KafkaPublishError(f"{remaining} messages still undelivered after {self._flush_timeout} s")
        if errors:
            raise KafkaPublishError(f"{len(errors)} messages not delivered, first: {errors[0]}")


def last_published_time(consumer: ConsumerLike, topic: str, *, timeout: float = 30.0) -> datetime | None:
    """Return the quarter-hour of the latest message in topic, None if it is empty.

    Each cycle publishes rows oldest first, so the last message of a partition
    is its latest quarter-hour. Compaction never removes it: it is the newest
    record for its key.
    """
    metadata = consumer.list_topics(topic, timeout=timeout)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        error = topic_metadata.error if topic_metadata is not None else "unknown topic"
        raise KafkaException(f"cannot read topic {topic}: {error}")

    latest = None
    for partition_id in sorted(topic_metadata.partitions):
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition_id), timeout=timeout)
        if high <= low:
            continue
        consumer.assign([TopicPartition(topic, partition_id, high - 1)])
        message = _poll_one(consumer, timeout)
        published = key_time(message.key())
        latest = published if latest is None else max(latest, published)
        logger.info("Partition %s of %s ends at offset %s, quarter-hour %s", partition_id, topic, high - 1, published)
    return latest


def _poll_one(consumer: ConsumerLike, timeout: float) -> Message:
    message = consumer.poll(timeout)
    if message is None:
        raise KafkaException(f"no message received within {timeout} s")
    if message.error() is not None:
        raise KafkaException(message.error())
    return message


def read_cursor(bootstrap_servers: str, topic: str) -> datetime | None:
    """Connect a throwaway consumer and return last_published_time(topic)."""
    consumer = Consumer(consumer_config(bootstrap_servers))
    try:
        return last_published_time(consumer, topic)
    finally:
        consumer.close()
