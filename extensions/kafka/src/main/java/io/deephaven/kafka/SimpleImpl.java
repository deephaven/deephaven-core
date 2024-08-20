//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.deephaven.UncheckedDeephavenException;
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
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.ByteType;
import io.deephaven.qst.type.CharType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.DoubleType;
import io.deephaven.qst.type.FloatType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.IntType;
import io.deephaven.qst.type.LongType;
import io.deephaven.qst.type.PrimitiveType;
import io.deephaven.qst.type.ShortType;
import io.deephaven.qst.type.StringType;
import io.deephaven.qst.type.Type;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.mutable.MutableInt;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.BytesDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.FloatDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.ShortDeserializer;
import org.apache.kafka.common.serialization.UUIDDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.deephaven.kafka.KafkaTools.KEY_COLUMN_NAME_DEFAULT;
import static io.deephaven.kafka.KafkaTools.KEY_COLUMN_NAME_PROPERTY;
import static io.deephaven.kafka.KafkaTools.KEY_COLUMN_TYPE_PROPERTY;
import static io.deephaven.kafka.KafkaTools.VALUE_COLUMN_NAME_DEFAULT;
import static io.deephaven.kafka.KafkaTools.VALUE_COLUMN_NAME_PROPERTY;
import static io.deephaven.kafka.KafkaTools.VALUE_COLUMN_TYPE_PROPERTY;

class SimpleImpl {

    /**
     * Single spec for unidimensional (basic Kafka encoded for one type) fields.
     */
    static final class SimpleConsume extends Consume.KeyOrValueSpec {
        private final String columnName;
        private final Class<?> dataType;

        SimpleConsume(final String columnName, final Class<?> dataType) {
            this.columnName = columnName;
            this.dataType = dataType;
        }

        @Override
        public Optional<SchemaProvider> getSchemaProvider() {
            return Optional.empty();
        }

        @Override
        protected Deserializer<?> getDeserializer(
                KeyOrValue keyOrValue,
                SchemaRegistryClient schemaRegistryClient,
                Map<String, ?> configs) {
            final Type<?> type = getType(keyOrValue, configs);
            final Deserializer<?> deserializer = deserializer(type).orElse(null);
            if (deserializer != null) {
                return deserializer;
            }
            throw new UncheckedDeephavenException(String.format(
                    "Deserializer for %s not set in kafka consumer properties and can't automatically set it for type %s",
                    this, dataType));
        }

        @Override
        protected KeyOrValueIngestData getIngestData(
                KeyOrValue keyOrValue,
                SchemaRegistryClient schemaRegistryClient, Map<String, ?> configs, MutableInt nextColumnIndexMut,
                List<ColumnDefinition<?>> columnDefinitionsOut) {
            final KeyOrValueIngestData data = new KeyOrValueIngestData();
            data.simpleColumnIndex = nextColumnIndexMut.getAndIncrement();
            final ColumnDefinition<?> colDef = ColumnDefinition.of(
                    getColumnName(keyOrValue, configs),
                    getType(keyOrValue, configs));
            columnDefinitionsOut.add(colDef);
            return data;
        }

        @Override
        protected KeyOrValueProcessor getProcessor(TableDefinition tableDef, KeyOrValueIngestData data) {
            return null;
        }

        String getColumnName(KeyOrValue keyOrValue, Map<String, ?> configs) {
            if (columnName != null) {
                return columnName;
            }
            {
                final String nameProperty = keyOrValue == KeyOrValue.KEY
                        ? KEY_COLUMN_NAME_PROPERTY
                        : VALUE_COLUMN_NAME_PROPERTY;
                if (configs.containsKey(nameProperty)) {
                    return (String) configs.get(nameProperty);
                }
            }
            return keyOrValue == KeyOrValue.KEY
                    ? KEY_COLUMN_NAME_DEFAULT
                    : VALUE_COLUMN_NAME_DEFAULT;
        }


        @VisibleForTesting
        Type<?> getType(KeyOrValue keyOrValue, Map<String, ?> configs) {
            if (dataType != null) {
                return Type.find(dataType);
            }
            {
                final Type<?> typeFromProperty = getTypeFromDhProperty(keyOrValue, configs);
                if (typeFromProperty != null) {
                    return typeFromProperty;
                }
            }
            {
                final Type<?> typeFromDeserializer = getTypeFromDeserializerProperty(keyOrValue, configs);
                if (typeFromDeserializer != null) {
                    return typeFromDeserializer;
                }
            }
            final String columnName = getColumnName(keyOrValue, configs);
            final String specName = keyOrValue == KeyOrValue.KEY ? "key_spec" : "value_spec";
            final String dhProperty = dhProperty(keyOrValue);
            final String kafkaDeserializerProperty = kafkaDeserializerProperty(keyOrValue);
            throw new UncheckedDeephavenException(String.format(
                    "Unable to find the type for column '%s' (%s). Please explicitly set the data type in the constructor, or through the kafka configuration '%s' or '%s'.",
                    columnName, specName, dhProperty, kafkaDeserializerProperty));
        }

        private Type<?> getTypeFromDhProperty(KeyOrValue keyOrValue, Map<String, ?> configs) {
            final String typeProperty = dhProperty(keyOrValue);
            if (!configs.containsKey(typeProperty)) {
                return null;
            }
            final String typeAsString = (String) configs.get(typeProperty);
            switch (typeAsString) {
                case "short":
                    return Type.shortType();
                case "int":
                    return Type.intType();
                case "long":
                    return Type.longType();
                case "float":
                    return Type.floatType();
                case "double":
                    return Type.doubleType();
                case "byte[]":
                    return Type.byteType().arrayType();
                case "String":
                case "string":
                    return Type.stringType();
                default:
                    throw new IllegalArgumentException(String.format(
                            "Property %s value %s not supported", typeProperty, typeAsString));
            }
        }

        private Type<?> getTypeFromDeserializerProperty(KeyOrValue keyOrValue, Map<String, ?> configs) {
            final String deserializerProperty = kafkaDeserializerProperty(keyOrValue);
            final String deserializer = (String) configs.get(deserializerProperty);
            if (deserializer == null) {
                return null;
            }
            final Type<?> type = DESER_NAME_TO_TYPE.get(deserializer);
            if (type != null) {
                return type;
            }
            throw new IllegalArgumentException(String.format(
                    "Deserializer type %s for %s not supported.", deserializer, deserializerProperty));
        }

        private static String dhProperty(KeyOrValue keyOrValue) {
            return keyOrValue == KeyOrValue.KEY
                    ? KEY_COLUMN_TYPE_PROPERTY
                    : VALUE_COLUMN_TYPE_PROPERTY;
        }

        private static String kafkaDeserializerProperty(KeyOrValue keyOrValue) {
            return keyOrValue == KeyOrValue.KEY
                    ? ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG
                    : ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
        }
    }

    /**
     * Single spec for unidimensional (basic Kafka encoded for one type) fields.
     */
    static final class SimpleProduce extends Produce.KeyOrValueSpec {
        private final String columnName;

        SimpleProduce(final String columnName) {
            this.columnName = columnName;
        }

        @Override
        public Optional<SchemaProvider> getSchemaProvider() {
            return Optional.empty();
        }

        @Override
        Serializer<?> getSerializer(SchemaRegistryClient schemaRegistryClient, TableDefinition definition) {
            final Class<?> dataType = definition.getColumn(columnName).getDataType();
            final Serializer<?> serializer = serializer(Type.find(dataType)).orElse(null);
            if (serializer != null) {
                return serializer;
            }
            throw new UncheckedDeephavenException(
                    String.format("Serializer not found for column %s, type %s", columnName, dataType.getName()));
        }

        @Override
        String[] getColumnNames(@NotNull Table t, SchemaRegistryClient schemaRegistryClient) {
            return new String[] {columnName};
        }

        @Override
        KeyOrValueSerializer<?> getKeyOrValueSerializer(@NotNull Table t, @NotNull String[] columnNames) {
            return new SimpleKeyOrValueSerializer<>(t, columnName);
        }
    }

    @VisibleForTesting
    static final Map<String, Type<?>> DESER_NAME_TO_TYPE = Map.of(
            ShortDeserializer.class.getName(), Type.shortType(),
            IntegerDeserializer.class.getName(), Type.intType(),
            LongDeserializer.class.getName(), Type.longType(),
            FloatDeserializer.class.getName(), Type.floatType(),
            DoubleDeserializer.class.getName(), Type.doubleType(),
            ByteArrayDeserializer.class.getName(), Type.byteType().arrayType(),
            UUIDDeserializer.class.getName(), Type.ofCustom(UUID.class),
            ByteBufferDeserializer.class.getName(), Type.ofCustom(ByteBuffer.class),
            BytesDeserializer.class.getName(), Type.ofCustom(Bytes.class));

    @VisibleForTesting
    static Optional<Serializer<?>> serializer(Type<?> type) {
        return Optional.ofNullable(type.walk(SerDeserVisitor.INSTANCE)).map(Serde::serializer);
    }

    @VisibleForTesting
    static Optional<Deserializer<?>> deserializer(Type<?> type) {
        return Optional.ofNullable(type.walk(SerDeserVisitor.INSTANCE)).map(Serde::deserializer);
    }

    /**
     * The visitor pattern with SerDeser ensures that whenever a new type is added, it is added both for serialization
     * and deserialization at the same time.
     */
    private enum SerDeserVisitor implements
            Type.Visitor<Serde<?>>,
            PrimitiveType.Visitor<Serde<?>>,
            GenericType.Visitor<Serde<?>> {
        INSTANCE;

        @Override
        public Serde<?> visit(PrimitiveType<?> primitiveType) {
            return primitiveType.walk((PrimitiveType.Visitor<Serde<?>>) this);
        }

        @Override
        public Serde<?> visit(GenericType<?> genericType) {
            return genericType.walk((GenericType.Visitor<Serde<?>>) this);
        }

        @Override
        public Serde<?> visit(BooleanType booleanType) {
            return null;
        }

        @Override
        public Serde<?> visit(ByteType byteType) {
            return null;
        }

        @Override
        public Serde<?> visit(CharType charType) {
            return null;
        }

        @Override
        public Serde<?> visit(ShortType shortType) {
            return Serdes.Short();
        }

        @Override
        public Serde<?> visit(IntType intType) {
            return Serdes.Integer();
        }

        @Override
        public Serde<?> visit(LongType longType) {
            return Serdes.Long();
        }

        @Override
        public Serde<?> visit(FloatType floatType) {
            return Serdes.Float();
        }

        @Override
        public Serde<?> visit(DoubleType doubleType) {
            return Serdes.Double();
        }

        @Override
        public Serde<?> visit(BoxedType<?> boxedType) {
            return boxedType.primitiveType().walk((PrimitiveType.Visitor<Serde<?>>) this);
        }

        @Override
        public Serde<?> visit(StringType stringType) {
            return Serdes.String();
        }

        @Override
        public Serde<?> visit(InstantType instantType) {
            return null;
        }

        @Override
        public Serde<?> visit(ArrayType<?, ?> arrayType) {
            // we could walk ArrayType, but byteType().arrayType() is the only array type deserializer we support
            if (Type.byteType().arrayType().equals(arrayType)) {
                return Serdes.ByteArray();
            }
            return null;
        }

        @Override
        public Serde<?> visit(CustomType<?> customType) {
            if (customType.clazz() == UUID.class) {
                return Serdes.UUID();
            }
            if (customType.clazz() == ByteBuffer.class) {
                return Serdes.ByteBuffer();
            }
            if (customType.clazz() == Bytes.class) {
                return Serdes.Bytes();
            }
            return null;
        }
    }
}
