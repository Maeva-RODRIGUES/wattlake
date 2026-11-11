from datetime import UTC, datetime

import pytest
from conftest import api_row, message

from wattlake_streaming.bronze import (
    BRONZE_SCHEMA,
    OFFSETS_PROPERTY,
    TABLE,
    TOPIC_PROPERTY,
    BronzeSchemaError,
    KafkaRecord,
    append,
    ensure_table,
    next_offsets,
    to_batch,
    to_row,
)

TOPIC = "eco2mix-realtime"
NOW = datetime(2026, 11, 11, 17, 20, tzinfo=UTC)


def test_to_row_keeps_values_and_adds_technical_columns():
    row = to_row(message(42), NOW)

    assert row["date_heure"] == datetime(2026, 11, 11, 16, 0, tzinfo=UTC)
    assert row["consommation"] == 47025
    # Kept as published: cleaning belongs to silver.
    assert row["hydraulique_fil_eau_eclusee"] == "3492"
    assert row["_source_dataset"] == "eco2mix-national-tr"
    assert row["_source_updated_at"] == datetime(2026, 11, 11, 16, 15, 7, tzinfo=UTC)
    assert (row["_kafka_partition"], row["_kafka_offset"], row["_ingested_at"]) == (0, 42, NOW)


def test_to_row_rejects_missing_or_unexpected_fields():
    row = api_row()
    del row["taux_co2"]
    row["new_field"] = 1
    with pytest.raises(BronzeSchemaError, match=r"missing fields: \['taux_co2'\].*unexpected fields: \['new_field'\]"):
        to_row(message(1, row), NOW)


def test_to_row_rejects_other_messages():
    with pytest.raises(BronzeSchemaError, match="not a producer message"):
        to_row(KafkaRecord(partition=0, offset=3, value=b"not json"), NOW)


def test_to_batch_has_the_bronze_schema():
    batch = to_batch([message(0), message(1)], NOW)
    assert batch.schema == BRONZE_SCHEMA
    assert batch.num_rows == 2


def test_ensure_table_is_idempotent(catalog):
    first = ensure_table(catalog)
    second = ensure_table(catalog)
    assert first.metadata.table_uuid == second.metadata.table_uuid
    assert [f.name for f in second.schema().fields] == BRONZE_SCHEMA.names


def test_empty_table_has_no_offsets(catalog):
    assert next_offsets(ensure_table(catalog), TOPIC) == {}


def test_append_stores_offsets_in_the_same_snapshot(catalog):
    table = ensure_table(catalog)
    append(table, to_batch([message(0), message(1)], NOW), TOPIC, {0: 2})

    summary = table.current_snapshot().summary
    assert summary[TOPIC_PROPERTY] == TOPIC
    assert summary[OFFSETS_PROPERTY] == '{"0": 2}'
    assert summary["added-records"] == "2"
    assert next_offsets(catalog.load_table(TABLE), TOPIC) == {0: 2}


def test_next_offsets_skips_snapshots_from_other_operations(catalog):
    table = ensure_table(catalog)
    append(table, to_batch([message(0)], NOW), TOPIC, {0: 1})
    # A later snapshot without the writer's properties (maintenance, for example).
    table.append(to_batch([message(1)], NOW))

    assert next_offsets(table, TOPIC) == {0: 1}


def test_next_offsets_ignores_another_topic(catalog):
    table = ensure_table(catalog)
    append(table, to_batch([message(0)], NOW), "other-topic", {0: 1})
    # Falls back to the data: offset 0 was written, so 1 is next.
    assert next_offsets(table, TOPIC) == {0: 1}


def test_next_offsets_falls_back_to_the_data(catalog):
    table = ensure_table(catalog)
    # Snapshots without properties, as after the writer's snapshots were expired.
    table.append(to_batch([message(4, partition=0), message(9, partition=0), message(2, partition=1)], NOW))

    assert next_offsets(table, TOPIC) == {0: 10, 1: 3}
