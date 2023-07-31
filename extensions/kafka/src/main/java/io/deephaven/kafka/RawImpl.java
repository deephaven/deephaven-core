/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
import io.deephaven.kafka.publish.SimpleKeyOrValueSerializer;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

class RawImpl {
    static final class RawConsume extends Consume.KeyOrValueSpec {
        private final ColumnDefinition<?> cd;
        private final Supplier<Deserializer<?>> supplier;

        public RawConsume(ColumnDefinition<?> cd, Class<? extends Deserializer<?>> deserializerClass) {
            this(cd, () -> {
                try {
                    return deserializerClass.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                        | NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        public RawConsume(ColumnDefinition<?> cd, Supplier<Deserializer<?>> supplier) {
            this.cd = Objects.requireNonNull(cd);
            this.supplier = Objects.requireNonNull(supplier);
        }

        @Override
        boolean isIgnore() {
            return false;
        }

        @Override
        public Optional<SchemaProvider> schemaProvider() {
            return Optional.empty();
        }

        @Override
        Deserializer<?> deserializer(KeyOrValue keyOrValue, SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs) {
            return supplier.get();
        }

        @Override
        KeyOrValueIngestData ingestData(KeyOrValue keyOrValue,
                List<ColumnDefinition<?>> columnDefinitionsOut, MutableInt nextColumnIndexMut,
                SchemaRegistryClient schemaRegistryClient, Map<String, ?> configs) {
            final KeyOrValueIngestData data = new KeyOrValueIngestData();
            data.simpleColumnIndex = nextColumnIndexMut.getAndAdd(1);
            columnDefinitionsOut.add(cd);
            return data;
        }

        @Override
        KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
            return null;
        }
    }

    static final class RawProduce extends Produce.KeyOrValueSpec {
        private final String columnName;
        private final Supplier<Serializer<?>> supplier;

        public RawProduce(String columnName, Class<? extends Serializer<?>> serializer) {
            this(columnName, () -> {
                try {
                    return serializer.getDeclaredConstructor().newInstance();
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                        | NoSuchMethodException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        public RawProduce(String columnName, Supplier<Serializer<?>> supplier) {
            this.columnName = Objects.requireNonNull(columnName);
            this.supplier = Objects.requireNonNull(supplier);
        }

        @Override
        boolean isIgnore() {
            return false;
        }

        @Override
        public Optional<SchemaProvider> schemaProvider() {
            return Optional.empty();
        }

        @Override
        Serializer<?> serializer(SchemaRegistryClient schemaRegistryClient, TableDefinition definition) {
            return supplier.get();
        }

        @Override
        String[] getColumnNames(@NotNull Table t, SchemaRegistryClient schemaRegistryClient) {
            return new String[] {columnName};
        }

        @Override
        KeyOrValueSerializer<?> keyOrValueSerializer(@NotNull Table t, @NotNull String[] columnNames) {
            return new SimpleKeyOrValueSerializer<>(t, columnName);
        }
    }
}
