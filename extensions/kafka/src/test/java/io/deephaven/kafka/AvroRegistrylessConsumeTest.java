//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.deephaven.engine.table.TableDefinition;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.RawMessageEncoder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AvroRegistrylessConsume} that need no Kafka broker: the deserializer is exercised directly against
 * bytes produced by Avro's own encoders, and the table definition is derived through the public
 * {@link KafkaTools#getTableDefinition} entry point.
 */
public class AvroRegistrylessConsumeTest {

    private static final String TOPIC = "test.topic";

    private static final String QUOTE_SCHEMA_JSON = "{"
            + "\"type\":\"record\",\"name\":\"Quote\",\"namespace\":\"test\",\"fields\":["
            + "{\"name\":\"sym\",\"type\":\"string\"},"
            + "{\"name\":\"size\",\"type\":\"int\"},"
            + "{\"name\":\"price\",\"type\":\"double\"},"
            + "{\"name\":\"venue\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"ts\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
            + "{\"name\":\"book\",\"type\":{\"type\":\"record\",\"name\":\"Book\",\"fields\":["
            + "{\"name\":\"bid\",\"type\":\"double\"},"
            + "{\"name\":\"ask\",\"type\":\"double\"}]}}"
            + "]}";

    /** Adds a field relative to {@link #QUOTE_SCHEMA_JSON}, so it has a different CRC-64-AVRO fingerprint. */
    private static final String QUOTE_V2_SCHEMA_JSON = QUOTE_SCHEMA_JSON.replace(
            "{\"name\":\"sym\",\"type\":\"string\"},",
            "{\"name\":\"sym\",\"type\":\"string\"},{\"name\":\"exchange\",\"type\":\"string\"},");

    private static final Schema QUOTE_SCHEMA = KafkaTools.getAvroSchema(QUOTE_SCHEMA_JSON);
    private static final Schema QUOTE_V2_SCHEMA = KafkaTools.getAvroSchema(QUOTE_V2_SCHEMA_JSON);

    @Test
    public void noSchemaProviderMeansNoRegistryClient() {
        // This is what keeps KafkaTools from constructing a CachedSchemaRegistryClient, and therefore what makes
        // schema.registry.url unnecessary.
        assertThat(AvroRegistrylessConsume.singleObject(QUOTE_SCHEMA_JSON).getSchemaProvider()).isEmpty();
        assertThat(AvroRegistrylessConsume.unframed(QUOTE_SCHEMA_JSON).getSchemaProvider()).isEmpty();
    }

    @Test
    public void tableDefinitionMatchesSchema() {
        final TableDefinition definition = KafkaTools.getTableDefinition(
                noCommonColumns(), Consume.IGNORE, AvroRegistrylessConsume.singleObject(QUOTE_SCHEMA_JSON));

        assertThat(definition.getColumnNames())
                .containsExactly("sym", "size", "price", "venue", "ts", "book__bid", "book__ask");
        assertThat(definition.getColumn("sym").getDataType()).isEqualTo(String.class);
        assertThat(definition.getColumn("size").getDataType()).isEqualTo(int.class);
        assertThat(definition.getColumn("price").getDataType()).isEqualTo(double.class);
        // A ["null", "string"] union is the unioned type in Deephaven.
        assertThat(definition.getColumn("venue").getDataType()).isEqualTo(String.class);
        // timestamp-micros is mapped to Instant, not long.
        assertThat(definition.getColumn("ts").getDataType()).isEqualTo(Instant.class);
        // Nested field paths are flattened, with "." rewritten to NESTED_FIELD_COLUMN_NAME_SEPARATOR.
        assertThat(definition.getColumn("book__bid").getDataType()).isEqualTo(double.class);
    }

    @Test
    public void fieldMappingRenamesAndExcludes() {
        final AvroRegistrylessConsume spec = AvroRegistrylessConsume.builder()
                .readerSchemaJson(QUOTE_SCHEMA_JSON)
                .fieldPathToColumnName(Map.of("sym", "Sym", "price", "Price"), true)
                .build();

        assertThat(KafkaTools.getTableDefinition(noCommonColumns(), Consume.IGNORE, spec).getColumnNames())
                .containsExactly("Sym", "Price");
    }

    @Test
    public void singleObjectRoundTrip() throws IOException {
        final Deserializer<?> deserializer = deserializerFor(AvroRegistrylessConsume.singleObject(QUOTE_SCHEMA_JSON));

        final GenericRecord decoded =
                (GenericRecord) deserializer.deserialize(TOPIC, singleObjectEncode(QUOTE_SCHEMA, quote()));

        assertThat(decoded).isNotNull();
        assertThat(decoded.get("sym")).hasToString("DHC");
        assertThat(decoded.get("size")).isEqualTo(100);
        assertThat(decoded.get("price")).isEqualTo(42.5d);
        assertThat(decoded.get("venue")).hasToString("XNAS");
        assertThat(decoded.get("ts")).isEqualTo(1_700_000_000_000_000L);
        assertThat(((GenericRecord) decoded.get("book")).get("ask")).isEqualTo(42.75d);
    }

    @Test
    public void singleObjectRejectsConfluentFraming() {
        final Deserializer<?> deserializer = deserializerFor(AvroRegistrylessConsume.singleObject(QUOTE_SCHEMA_JSON));

        // Confluent frames as 0x00 followed by a four-byte big-endian schema ID; single-object encoding expects
        // 0xC3 0x01. Avro reports the bad marker either as an IOException (which we wrap) or as an
        // AvroRuntimeException, depending on version.
        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, new byte[] {0x00, 0x00, 0x00, 0x00, 0x01, 0x02}))
                .satisfiesAnyOf(
                        t -> assertThat(t).isInstanceOf(SerializationException.class),
                        t -> assertThat(t).isInstanceOf(AvroRuntimeException.class));
    }

    @Test
    public void singleObjectResolvesRegisteredWriterSchema() throws IOException {
        // Records written with QUOTE_V2 carry V2's fingerprint. Registering it lets Avro project V2 onto the reader's
        // schema, dropping the field the reader does not declare.
        final Deserializer<?> deserializer = deserializerFor(AvroRegistrylessConsume.builder()
                .readerSchemaJson(QUOTE_SCHEMA_JSON)
                .addWriterSchemaJson(QUOTE_V2_SCHEMA_JSON)
                .build());

        final GenericRecord decoded =
                (GenericRecord) deserializer.deserialize(TOPIC, singleObjectEncode(QUOTE_V2_SCHEMA, quoteV2()));

        assertThat(decoded.getSchema().getField("exchange")).isNull();
        assertThat(decoded.get("sym")).hasToString("DHC");
        assertThat(decoded.get("price")).isEqualTo(42.5d);
    }

    @Test
    public void singleObjectRejectsUnregisteredWriterSchema() throws IOException {
        final Deserializer<?> deserializer = deserializerFor(AvroRegistrylessConsume.singleObject(QUOTE_SCHEMA_JSON));
        final byte[] encoded = singleObjectEncode(QUOTE_V2_SCHEMA, quoteV2());

        assertThatThrownBy(() -> deserializer.deserialize(TOPIC, encoded))
                .satisfiesAnyOf(
                        t -> assertThat(t).isInstanceOf(SerializationException.class),
                        t -> assertThat(t).isInstanceOf(AvroRuntimeException.class));
    }

    @Test
    public void ignoreFingerprintDecodesUnknownFingerprint() throws IOException {
        // The whole point: records are written with a schema whose fingerprint the spec has never been told about, so
        // SINGLE_OBJECT would fail them (see singleObjectRejectsUnregisteredWriterSchema). Ignoring the fingerprint
        // decodes the body with the reader's schema instead.
        final Deserializer<?> deserializer =
                deserializerFor(AvroRegistrylessConsume.singleObjectIgnoringFingerprint(QUOTE_SCHEMA_JSON));

        // Encoded with the reader's own schema, but stamped with a header advertising a fingerprint that belongs to
        // nothing the spec knows.
        final byte[] encoded = singleObjectEncode(QUOTE_SCHEMA, quote());
        final byte[] tampered = encoded.clone();
        for (int i = 2; i < 10; i++) {
            tampered[i] = (byte) ~tampered[i];
        }

        final GenericRecord decoded = (GenericRecord) deserializer.deserialize(TOPIC, tampered);

        assertThat(decoded.get("sym")).hasToString("DHC");
        assertThat(decoded.get("size")).isEqualTo(100);
        assertThat(decoded.get("price")).isEqualTo(42.5d);
        assertThat(decoded.get("ts")).isEqualTo(1_700_000_000_000_000L);
        assertThat(((GenericRecord) decoded.get("book")).get("ask")).isEqualTo(42.75d);
    }

    @Test
    public void ignoreFingerprintStillRequiresTheMarker() {
        // Confluent framing is still rejected, so this does not silently paper over the wrong framing.
        final Deserializer<?> deserializer =
                deserializerFor(AvroRegistrylessConsume.singleObjectIgnoringFingerprint(QUOTE_SCHEMA_JSON));

        assertThatThrownBy(() -> deserializer.deserialize(
                TOPIC, new byte[] {0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07}))
                .satisfiesAnyOf(
                        t -> assertThat(t).isInstanceOf(SerializationException.class),
                        t -> assertThat(t).isInstanceOf(AvroRuntimeException.class))
                .hasMessageContaining("0xC3");
    }

    @Test
    public void ignoreFingerprintUsesRegisteredWriterSchema() throws IOException {
        // With an explicit writer's schema, a V2 payload projects onto the reader's schema even though its fingerprint
        // is never consulted.
        final Deserializer<?> deserializer = deserializerFor(AvroRegistrylessConsume.builder()
                .framing(AvroRegistrylessConsume.Framing.SINGLE_OBJECT_IGNORE_FINGERPRINT)
                .readerSchemaJson(QUOTE_SCHEMA_JSON)
                .addWriterSchemaJson(QUOTE_V2_SCHEMA_JSON)
                .build());

        final GenericRecord decoded =
                (GenericRecord) deserializer.deserialize(TOPIC, singleObjectEncode(QUOTE_V2_SCHEMA, quoteV2()));

        assertThat(decoded.getSchema().getField("exchange")).isNull();
        assertThat(decoded.get("sym")).hasToString("DHC");
        assertThat(decoded.get("price")).isEqualTo(42.5d);
    }

    @Test
    public void ignoreFingerprintRejectsMultipleWriterSchemas() {
        assertThatThrownBy(() -> AvroRegistrylessConsume.builder()
                .framing(AvroRegistrylessConsume.Framing.SINGLE_OBJECT_IGNORE_FINGERPRINT)
                .readerSchemaJson(QUOTE_SCHEMA_JSON)
                .addWriterSchemaJson(QUOTE_SCHEMA_JSON)
                .addWriterSchemaJson(QUOTE_V2_SCHEMA_JSON)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most one writer's schema");
    }

    @Test
    public void unframedRoundTrip() throws IOException {
        final Deserializer<?> deserializer = deserializerFor(AvroRegistrylessConsume.unframed(QUOTE_SCHEMA_JSON));
        final RawMessageEncoder<GenericRecord> encoder =
                new RawMessageEncoder<>(GenericData.get(), QUOTE_SCHEMA);

        final GenericRecord decoded =
                (GenericRecord) deserializer.deserialize(TOPIC, toByteArray(encoder.encode(quote())));

        assertThat(decoded.get("sym")).hasToString("DHC");
        assertThat(decoded.get("size")).isEqualTo(100);
    }

    @Test
    public void unframedRejectsMultipleWriterSchemas() {
        assertThatThrownBy(() -> AvroRegistrylessConsume.builder()
                .framing(AvroRegistrylessConsume.Framing.NONE)
                .readerSchemaJson(QUOTE_SCHEMA_JSON)
                .addWriterSchemaJson(QUOTE_SCHEMA_JSON)
                .addWriterSchemaJson(QUOTE_V2_SCHEMA_JSON)
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most one writer's schema");
    }

    @Test
    public void nullPayloadDecodesToNull() {
        assertThat(deserializerFor(AvroRegistrylessConsume.singleObject(QUOTE_SCHEMA_JSON))
                .deserialize(TOPIC, null)).isNull();
    }

    @Test
    public void readerSchemaIsRequired() {
        assertThatThrownBy(() -> AvroRegistrylessConsume.builder().build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("readerSchema");
    }

    private static Deserializer<?> deserializerFor(final AvroRegistrylessConsume spec) {
        // getDeserializer is protected on Consume.KeyOrValueSpec; this test shares the spec's package.
        return spec.getDeserializer(KeyOrValue.VALUE, null, Map.of());
    }

    private static GenericRecord quote() {
        return new GenericRecordBuilder(QUOTE_SCHEMA)
                .set("sym", "DHC")
                .set("size", 100)
                .set("price", 42.5d)
                .set("venue", "XNAS")
                .set("ts", 1_700_000_000_000_000L)
                .set("book", book(QUOTE_SCHEMA))
                .build();
    }

    private static GenericRecord quoteV2() {
        return new GenericRecordBuilder(QUOTE_V2_SCHEMA)
                .set("sym", "DHC")
                .set("exchange", "NASDAQ")
                .set("size", 100)
                .set("price", 42.5d)
                .set("venue", "XNAS")
                .set("ts", 1_700_000_000_000_000L)
                .set("book", book(QUOTE_V2_SCHEMA))
                .build();
    }

    private static GenericRecord book(final Schema quoteSchema) {
        return new GenericRecordBuilder(quoteSchema.getField("book").schema())
                .set("bid", 42.25d)
                .set("ask", 42.75d)
                .build();
    }

    private static byte[] singleObjectEncode(final Schema schema, final GenericRecord record) throws IOException {
        final BinaryMessageEncoder<GenericRecord> encoder = new BinaryMessageEncoder<>(GenericData.get(), schema);
        return toByteArray(encoder.encode(record));
    }

    private static byte[] toByteArray(final ByteBuffer buffer) {
        final byte[] bytes = new byte[buffer.remaining()];
        buffer.duplicate().get(bytes);
        return bytes;
    }

    /**
     * Disables the KafkaPartition / Offset / Timestamp columns that {@link KafkaTools} adds by default, so assertions
     * cover only the Avro-derived columns.
     */
    private static Properties noCommonColumns() {
        final Properties properties = new Properties();
        properties.setProperty(KafkaTools.KAFKA_PARTITION_COLUMN_NAME_PROPERTY, "");
        properties.setProperty(KafkaTools.OFFSET_COLUMN_NAME_PROPERTY, "");
        properties.setProperty(KafkaTools.TIMESTAMP_COLUMN_NAME_PROPERTY, "");
        return properties;
    }
}
