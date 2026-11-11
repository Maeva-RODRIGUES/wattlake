from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from confluent_kafka import KafkaException, TopicPartition

from wattlake_streaming.kafka import KafkaPublishError, Publisher, last_published_time

TOPIC = "eco2mix-realtime"


@dataclass
class FakeProducer:
    """Delivers every message on poll/flush, failing the keys listed in fail_keys."""

    fail_keys: set[bytes] = field(default_factory=set)
    # Number of produce() calls that raise BufferError before accepting messages.
    full_queue: int = 0
    undelivered_on_flush: int = 0
    pending: list = field(default_factory=list)
    delivered: list[tuple[bytes, bytes]] = field(default_factory=list)

    def produce(self, topic, value, key, on_delivery):
        assert topic == TOPIC
        if self.full_queue:
            self.full_queue -= 1
            raise BufferError("queue full")
        self.pending.append((key, value, on_delivery))

    def poll(self, timeout):
        for key, value, on_delivery in self.pending:
            message = SimpleNamespace(key=lambda key=key: key)
            if key in self.fail_keys:
                on_delivery("broker unavailable", message)
            else:
                self.delivered.append((key, value))
                on_delivery(None, message)
        self.pending.clear()
        return 0

    def flush(self, timeout):
        self.poll(0)
        return self.undelivered_on_flush


MESSAGES = [(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")]


def test_publish_all_delivers_in_order():
    producer = FakeProducer()
    Publisher(producer, TOPIC).publish_all(MESSAGES)
    assert producer.delivered == MESSAGES


def test_publish_all_waits_when_local_queue_is_full():
    producer = FakeProducer(full_queue=2)
    Publisher(producer, TOPIC).publish_all(MESSAGES)
    assert producer.delivered == MESSAGES


def test_publish_all_raises_on_failed_delivery():
    producer = FakeProducer(fail_keys={b"k2"})
    with pytest.raises(KafkaPublishError, match="1 messages not delivered"):
        Publisher(producer, TOPIC).publish_all(MESSAGES)


def test_publish_all_raises_when_flush_times_out():
    producer = FakeProducer(undelivered_on_flush=2)
    with pytest.raises(KafkaPublishError, match="2 messages still undelivered"):
        Publisher(producer, TOPIC).publish_all(MESSAGES)


@dataclass
class FakeConsumer:
    """A topic whose partitions hold the given keys, in offset order."""

    partitions: dict[int, list[bytes]]
    topic_error: str | None = None
    assigned: list[TopicPartition] = field(default_factory=list)

    def list_topics(self, topic, timeout):
        topics = (
            {}
            if self.partitions is None
            else {TOPIC: SimpleNamespace(error=self.topic_error, partitions={p: None for p in self.partitions})}
        )
        return SimpleNamespace(topics=topics)

    def get_watermark_offsets(self, partition, timeout):
        return 0, len(self.partitions[partition.partition])

    def assign(self, partitions):
        self.assigned = partitions

    def poll(self, timeout):
        tp = self.assigned[0]
        key = self.partitions[tp.partition][tp.offset]
        return SimpleNamespace(key=lambda: key, error=lambda: None)

    def close(self):
        pass


def test_last_published_time_is_none_on_empty_topic():
    assert last_published_time(FakeConsumer({0: []}), TOPIC) is None


def test_last_published_time_reads_the_last_message():
    consumer = FakeConsumer({0: [b"2026-11-11T15:45:00+00:00", b"2026-11-11T16:00:00+00:00"]})
    assert last_published_time(consumer, TOPIC) == datetime(2026, 11, 11, 16, 0, tzinfo=UTC)
    assert consumer.assigned[0].offset == 1


def test_last_published_time_takes_the_latest_partition():
    consumer = FakeConsumer({0: [b"2026-11-11T15:00:00+00:00"], 1: [b"2026-11-11T16:00:00+00:00"], 2: []})
    assert last_published_time(consumer, TOPIC) == datetime(2026, 11, 11, 16, 0, tzinfo=UTC)


def test_last_published_time_fails_on_missing_topic():
    with pytest.raises(KafkaException, match="unknown topic"):
        last_published_time(FakeConsumer(None), TOPIC)


def test_last_published_time_fails_on_topic_error():
    with pytest.raises(KafkaException, match="not authorized"):
        last_published_time(FakeConsumer({0: []}, topic_error="not authorized"), TOPIC)
