//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka;

import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.kafka.KafkaTools.Consume;
import io.deephaven.kafka.KafkaTools.KeyOrValue;
import io.deephaven.kafka.KafkaTools.KeyOrValueIngestData;
import io.deephaven.kafka.ingest.GenericRecordChunkAdapter;
import io.deephaven.kafka.ingest.KeyOrValueProcessor;
import io.deephaven.stream.StreamChunkUtils;
import io.deephaven.util.mutable.MutableInt;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.message.BadHeaderException;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.MessageDecoder;
import org.apache.avro.message.RawMessageDecoder;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * A Kafka consume spec for Avro payloads that carry no Confluent schema-registry framing.
 *
 * <p>
 * {@link Consume#avroSpec(Schema) Consume.avroSpec} always deserializes with Confluent's
 * {@link io.confluent.kafka.serializers.KafkaAvroDeserializer KafkaAvroDeserializer}, even when the {@link Schema} is
 * supplied explicitly: the explicit schema is used only as the <em>reader's</em> schema, while the <em>writer's</em>
 * schema is still resolved from the Confluent wire prefix ({@code 0x00} followed by a four-byte big-endian registry
 * ID). Payloads using Avro's own single-object encoding therefore fail with {@code Unknown magic byte!}, because SOE
 * frames a record as {@code 0xC3 0x01} followed by the eight-byte little-endian CRC-64-AVRO fingerprint of the writer's
 * schema.
 *
 * <p>
 * This spec substitutes a deserializer built from Avro's own {@code org.apache.avro.message} codecs and reports
 * {@link #getSchemaProvider()} as {@link Optional#empty() empty}, so {@link KafkaTools} never constructs a
 * {@link SchemaRegistryClient} and {@code schema.registry.url} is not required. Everything downstream of the
 * deserializer is the ordinary Avro path so the resulting table has the same shape it would have under
 * {@code avroSpec}: one typed column per Avro field, nested records flattened into
 * {@value KafkaTools#NESTED_FIELD_COLUMN_NAME_SEPARATOR}-joined columns,
 *
 * <p>
 * Usage from Python:
 *
 * <pre>
 * import jpy
 * from deephaven.stream.kafka.consumer import consume, KeyValueSpec, TableType
 *
 * _AvroSoe = jpy.get_type('io.deephaven.kafka.AvroRegistrylessConsume')
 * spec = KeyValueSpec(j_spec=_AvroSoe.singleObject(SCHEMA_JSON))
 *
 * t = consume({'bootstrap.servers': '...', 'group.id': '...'}, 'my.topic',
 *             key_spec=KeyValueSpec.IGNORE, value_spec=spec,
 *             table_type=TableType.append())
 * </pre>
 *
 * @see Framing
 */
public final class AvroRegistrylessConsume extends Consume.KeyOrValueSpec {

    /**
     * How each Kafka record's bytes are framed.
     */
    public enum Framing {
        /**
         * Avro single-object encoding: {@code 0xC3 0x01}, the eight-byte little-endian CRC-64-AVRO fingerprint of the
         * writer's schema, then the record body. The writer's schema is resolved by fingerprint, so every schema that
         * may appear on the wire must be registered via {@link Builder#addWriterSchema(Schema)}; the reader's schema is
         * registered automatically. An unregistered fingerprint fails the record with
         * {@code org.apache.avro.message.MissingSchemaException}.
         */
        SINGLE_OBJECT,

        /**
         * Avro single-object encoding, but the fingerprint is <em>not</em> used to select the writer's schema: the
         * ten-byte header is validated for the {@code 0xC3 0x01} marker and then discarded, and the body is decoded
         * with the single writer's schema supplied here — {@link Builder#addWriterSchema(Schema)}, or the reader's
         * schema if none was registered.
         *
         * <p>
         * Use this when the producer's schema is known but its fingerprint is not, which is the common case when the
         * exact bytes of the writer's schema cannot be reproduced. The fingerprint covers Avro's <em>canonical parsing
         * form</em>, which preserves field order, namespaces and the full field list, so a schema that is semantically
         * compatible with the writer's — even one that only adds a field with a default — still fingerprints
         * differently and fails {@link #SINGLE_OBJECT} with {@code org.apache.avro.message.MissingSchemaException}.
         * (Documentation strings are the notable exception; canonical form strips them, so they do not affect the
         * fingerprint.)
         *
         * <p>
         * The trade-off is that a genuine writer/reader mismatch is no longer detected up front. Avro still resolves
         * the body against the reader's schema, so an incompatible writer's schema fails per record while decoding
         * rather than once at the header.
         */
        SINGLE_OBJECT_IGNORE_FINGERPRINT,

        /**
         * No framing at all: the record's bytes are the Avro binary body and nothing else. There is no fingerprint to
         * resolve, so exactly one writer's schema applies to the whole topic — the one registered via
         * {@link Builder#addWriterSchema(Schema)}, or the reader's schema if none was registered.
         */
        NONE
    }

    private static final Pattern NESTED_FIELD_NAME_SEPARATOR_PATTERN =
            Pattern.compile(Pattern.quote(KafkaTools.NESTED_FIELD_NAME_SEPARATOR));

    private final Framing framing;
    private final Schema readerSchema;
    private final List<Schema> writerSchemas;
    private final Function<String, String> fieldPathToColumnName;
    private final boolean useUTF8Strings;

    /**
     * A spec for {@link Framing#SINGLE_OBJECT single-object encoded} records, naming columns with
     * {@link KafkaTools#DIRECT_MAPPING}.
     *
     * @param readerSchema the reader's schema, which is also registered as a writer's schema
     * @return the spec
     */
    public static AvroRegistrylessConsume singleObject(final Schema readerSchema) {
        return builder().readerSchema(readerSchema).framing(Framing.SINGLE_OBJECT).build();
    }

    /**
     * A spec for {@link Framing#SINGLE_OBJECT single-object encoded} records, naming columns with
     * {@link KafkaTools#DIRECT_MAPPING}.
     *
     * @param readerSchemaJson the reader's schema as a JSON string
     * @return the spec
     */
    public static AvroRegistrylessConsume singleObject(final String readerSchemaJson) {
        return singleObject(KafkaTools.getAvroSchema(readerSchemaJson));
    }

    /**
     * A spec for {@link Framing#SINGLE_OBJECT_IGNORE_FINGERPRINT single-object encoded records whose fingerprint is
     * ignored}, naming columns with {@link KafkaTools#DIRECT_MAPPING}.
     *
     * @param schema the schema, used as both the writer's and the reader's schema
     * @return the spec
     */
    public static AvroRegistrylessConsume singleObjectIgnoringFingerprint(final Schema schema) {
        return builder().readerSchema(schema).framing(Framing.SINGLE_OBJECT_IGNORE_FINGERPRINT).build();
    }

    /**
     * A spec for {@link Framing#SINGLE_OBJECT_IGNORE_FINGERPRINT single-object encoded records whose fingerprint is
     * ignored}, naming columns with {@link KafkaTools#DIRECT_MAPPING}.
     *
     * @param schemaJson the schema as a JSON string, used as both the writer's and the reader's schema
     * @return the spec
     */
    public static AvroRegistrylessConsume singleObjectIgnoringFingerprint(final String schemaJson) {
        return singleObjectIgnoringFingerprint(KafkaTools.getAvroSchema(schemaJson));
    }

    /**
     * A spec for {@link Framing#NONE unframed} records, naming columns with {@link KafkaTools#DIRECT_MAPPING}.
     *
     * @param schema the schema, used as both the writer's and the reader's schema
     * @return the spec
     */
    public static AvroRegistrylessConsume unframed(final Schema schema) {
        return builder().readerSchema(schema).framing(Framing.NONE).build();
    }

    /**
     * A spec for {@link Framing#NONE unframed} records, naming columns with {@link KafkaTools#DIRECT_MAPPING}.
     *
     * @param schemaJson the schema as a JSON string, used as both the writer's and the reader's schema
     * @return the spec
     */
    public static AvroRegistrylessConsume unframed(final String schemaJson) {
        return unframed(KafkaTools.getAvroSchema(schemaJson));
    }

    public static Builder builder() {
        return new Builder();
    }

    private AvroRegistrylessConsume(
            final Framing framing,
            final Schema readerSchema,
            final List<Schema> writerSchemas,
            final Function<String, String> fieldPathToColumnName,
            final boolean useUTF8Strings) {
        this.framing = Objects.requireNonNull(framing, "framing");
        this.readerSchema = Objects.requireNonNull(readerSchema, "readerSchema");
        this.writerSchemas = List.copyOf(writerSchemas);
        this.fieldPathToColumnName = Objects.requireNonNull(fieldPathToColumnName, "fieldPathToColumnName");
        this.useUTF8Strings = useUTF8Strings;
        if (framing != Framing.SINGLE_OBJECT && this.writerSchemas.size() > 1) {
            throw new IllegalArgumentException("Framing." + framing + " does not resolve a fingerprint, so at most one"
                    + " writer's schema may be registered; got " + this.writerSchemas.size());
        }
    }

    /**
     * Returns {@link Optional#empty()}, which is what keeps {@link KafkaTools} from building a
     * {@link SchemaRegistryClient} for this spec.
     */
    @Override
    public Optional<SchemaProvider> getSchemaProvider() {
        return Optional.empty();
    }

    @Override
    protected Deserializer<?> getDeserializer(
            final KeyOrValue keyOrValue,
            final SchemaRegistryClient schemaRegistryClient,
            final Map<String, ?> configs) {
        return new GenericRecordDeserializer(makeDecoder());
    }

    @Override
    protected KeyOrValueIngestData getIngestData(
            final KeyOrValue keyOrValue,
            final SchemaRegistryClient schemaRegistryClient,
            final Map<String, ?> configs,
            final MutableInt nextColumnIndexMut,
            final List<ColumnDefinition<?>> columnDefinitionsOut) {
        final KeyOrValueIngestData data = new KeyOrValueIngestData();
        data.fieldPathToColumnName = new HashMap<>();
        KafkaTools.avroSchemaToColumnDefinitions(columnDefinitionsOut, data.fieldPathToColumnName, readerSchema,
                fieldPathToColumnName, useUTF8Strings);
        data.extra = readerSchema;
        return data;
    }

    @Override
    protected KeyOrValueProcessor getProcessor(final TableDefinition tableDef, final KeyOrValueIngestData data) {
        return GenericRecordChunkAdapter.make(
                tableDef,
                ci -> StreamChunkUtils.chunkTypeForColumnIndex(tableDef, ci),
                data.fieldPathToColumnName,
                NESTED_FIELD_NAME_SEPARATOR_PATTERN,
                (Schema) data.extra,
                true);
    }

    private MessageDecoder<GenericRecord> makeDecoder() {
        switch (framing) {
            case SINGLE_OBJECT: {
                final BinaryMessageDecoder<GenericRecord> decoder =
                        new BinaryMessageDecoder<>(GenericData.get(), readerSchema);
                // The constructor registers the reader's schema; registering it again is a no-op, and doing so keeps
                // this independent of that detail. Additional writer's schemas must be registered so their
                // fingerprints resolve to a schema we can project onto the reader's schema.
                decoder.addSchema(readerSchema);
                for (final Schema writerSchema : writerSchemas) {
                    decoder.addSchema(writerSchema);
                }
                return decoder;
            }
            case SINGLE_OBJECT_IGNORE_FINGERPRINT:
                return new FingerprintIgnoringDecoder(rawDecoder());
            case NONE:
                return rawDecoder();
            default:
                throw new IllegalStateException("Unexpected framing " + framing);
        }
    }

    private RawMessageDecoder<GenericRecord> rawDecoder() {
        final Schema writerSchema = writerSchemas.isEmpty() ? readerSchema : writerSchemas.get(0);
        return new RawMessageDecoder<>(GenericData.get(), writerSchema, readerSchema);
    }

    /**
     * Discards a {@link Framing#SINGLE_OBJECT single-object} header instead of resolving the fingerprint in it, and
     * decodes the body with a fixed writer's schema.
     */
    private static final class FingerprintIgnoringDecoder extends MessageDecoder.BaseDecoder<GenericRecord> {
        /** {@code 0xC3 0x01} plus the eight-byte fingerprint. */
        private static final int HEADER_LENGTH = 10;
        private static final byte MARKER_0 = (byte) 0xC3;
        private static final byte MARKER_1 = (byte) 0x01;

        private final RawMessageDecoder<GenericRecord> body;

        private FingerprintIgnoringDecoder(final RawMessageDecoder<GenericRecord> body) {
            this.body = Objects.requireNonNull(body);
        }

        @Override
        public GenericRecord decode(final InputStream stream, final GenericRecord reuse) throws IOException {
            final byte[] header = new byte[HEADER_LENGTH];
            int read = 0;
            while (read < HEADER_LENGTH) {
                final int n = stream.read(header, read, HEADER_LENGTH - read);
                if (n < 0) {
                    throw new BadHeaderException(
                            "Expected a " + HEADER_LENGTH + "-byte single-object header, got only " + read + " bytes");
                }
                read += n;
            }
            if (header[0] != MARKER_0 || header[1] != MARKER_1) {
                // Worth being specific: 0x00 means the payload is Confluent-framed, and the built-in
                // Consume.avroSpec handles that case.
                throw new BadHeaderException(String.format(
                        "Expected the single-object marker 0xC3 0x01, got 0x%02X 0x%02X",
                        header[0], header[1]));
            }
            return body.decode(stream, reuse);
        }
    }

    /**
     * Adapts an Avro {@link MessageDecoder} to a Kafka {@link Deserializer}.
     *
     * <p>
     * A new instance is created per {@link #getDeserializer} call, and {@link KafkaTools} hands each one to a single
     * {@link io.deephaven.kafka.ingest.KafkaIngester KafkaIngester} consumer thread, so the decoder's internal reuse of
     * reader state is not shared across threads.
     */
    private static final class GenericRecordDeserializer implements Deserializer<GenericRecord> {
        private final MessageDecoder<GenericRecord> decoder;

        private GenericRecordDeserializer(final MessageDecoder<GenericRecord> decoder) {
            this.decoder = Objects.requireNonNull(decoder);
        }

        @Override
        public GenericRecord deserialize(final String topic, final byte[] data) {
            if (data == null) {
                return null;
            }
            try {
                return decoder.decode(data);
            } catch (IOException e) {
                throw new SerializationException(
                        "Failed to decode Avro payload for topic " + topic + " (" + data.length + " bytes)", e);
            }
        }
    }

    public static final class Builder {
        private Framing framing = Framing.SINGLE_OBJECT;
        private Schema readerSchema;
        private final List<Schema> writerSchemas = new ArrayList<>();
        private Function<String, String> fieldPathToColumnName = KafkaTools.DIRECT_MAPPING;
        private boolean useUTF8Strings;

        private Builder() {}

        /**
         * How each record's bytes are framed; {@link Framing#SINGLE_OBJECT} by default.
         */
        public Builder framing(final Framing framing) {
            this.framing = Objects.requireNonNull(framing, "framing");
            return this;
        }

        /**
         * The schema that determines the table's columns, and onto which each decoded record is projected. Required.
         */
        public Builder readerSchema(final Schema readerSchema) {
            this.readerSchema = Objects.requireNonNull(readerSchema, "readerSchema");
            return this;
        }

        /**
         * The reader's schema, as a JSON string.
         *
         * @see #readerSchema(Schema)
         */
        public Builder readerSchemaJson(final String readerSchemaJson) {
            return readerSchema(KafkaTools.getAvroSchema(readerSchemaJson));
        }

        /**
         * Registers an additional writer's schema. Needed only when producers write a schema that differs from the
         * reader's schema — Avro resolves the difference, but it can only do so for a schema it has been given. The
         * reader's schema is always registered.
         */
        public Builder addWriterSchema(final Schema writerSchema) {
            writerSchemas.add(Objects.requireNonNull(writerSchema, "writerSchema"));
            return this;
        }

        /**
         * Registers an additional writer's schema, as a JSON string.
         *
         * @see #addWriterSchema(Schema)
         */
        public Builder addWriterSchemaJson(final String writerSchemaJson) {
            return addWriterSchema(KafkaTools.getAvroSchema(writerSchemaJson));
        }

        /**
         * Maps an Avro field path to a column name, where nested paths are joined with
         * {@value KafkaTools#NESTED_FIELD_NAME_SEPARATOR}. Fields mapped to {@code null} are excluded from the table.
         * Defaults to {@link KafkaTools#DIRECT_MAPPING}, which keeps every field under its own name after rewriting the
         * nested separator to {@value KafkaTools#NESTED_FIELD_COLUMN_NAME_SEPARATOR} — a field path such as
         * {@code book.bid} becomes the column {@code book__bid}, since {@code .} is not usable in a column name.
         */
        public Builder fieldPathToColumnName(final Function<String, String> fieldPathToColumnName) {
            this.fieldPathToColumnName = Objects.requireNonNull(fieldPathToColumnName, "fieldPathToColumnName");
            return this;
        }

        /**
         * Maps Avro field paths to column names from {@code mapping}. Paths absent from {@code mapping} are excluded
         * from the table when {@code mappedOnly}, and otherwise fall back to {@link KafkaTools#DIRECT_MAPPING}. A path
         * present with a {@code null} value is excluded either way.
         *
         * @see #fieldPathToColumnName(Function)
         */
        public Builder fieldPathToColumnName(final Map<String, String> mapping, final boolean mappedOnly) {
            // Not Map.copyOf: a null value is how a caller excludes an explicitly named field.
            final Map<String, String> copy = new HashMap<>(mapping);
            return fieldPathToColumnName(mappedOnly
                    ? copy::get
                    : fieldPath -> copy.containsKey(fieldPath)
                            ? copy.get(fieldPath)
                            : KafkaTools.DIRECT_MAPPING.apply(fieldPath));
        }

        /**
         * When {@code true}, Avro string fields become {@code Utf8} columns rather than being converted to
         * {@code String}. {@code false} by default.
         */
        public Builder useUTF8Strings(final boolean useUTF8Strings) {
            this.useUTF8Strings = useUTF8Strings;
            return this;
        }

        public AvroRegistrylessConsume build() {
            if (readerSchema == null) {
                throw new IllegalStateException("readerSchema is required");
            }
            return new AvroRegistrylessConsume(framing, readerSchema, writerSchemas, fieldPathToColumnName,
                    useUTF8Strings);
        }
    }
}
