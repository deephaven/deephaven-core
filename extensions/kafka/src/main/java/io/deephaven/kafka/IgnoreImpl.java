//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.KafkaTools.Produce;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.kafka.publish.KeyOrValueSerializer;
import io.deephaven.util.mutable.MutableInt;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.Map;
import java.util.Optional;

class IgnoreImpl {
    static final class IgnoreConsume extends Consume.KeyOrValueSpec {

        @Override
        public Optional<SchemaProvider> getSchemaProvider() {
            return Optional.empty();
        }

        @Override
        protected Deserializer<?> getDeserializer(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs) {
            return new ByteArrayDeserializer();
        }

        @Override
        protected KeyOrValueIngestData getIngestData(KeyOrValue keyOrValue,
                SchemaRegistryClient schemaRegistryClient, Map<String, ?> configs, MutableInt nextColumnIndexMut,
                List<ColumnDefinition<?>> columnDefinitionsOut) {
            return null;
        }

        @Override
        protected KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
            return null;
        }
    }

    static final class IgnoreProduce extends Produce.KeyOrValueSpec {

        @Override
        public Optional<SchemaProvider> getSchemaProvider() {
            return Optional.empty();
        }

        @Override
        Serializer<?> getSerializer(SchemaRegistryClient schemaRegistryClient, TableDefinition definition) {
            return new ByteArraySerializer();
        }

        @Override
        String[] getColumnNames(@NotNull Table t, SchemaRegistryClient schemaRegistryClient) {
            return null;
        }

        @Override
        KeyOrValueSerializer<?> getKeyOrValueSerializer(@NotNull Table t, @NotNull String[] columnNames) {
            return null;
        }
    }
}
