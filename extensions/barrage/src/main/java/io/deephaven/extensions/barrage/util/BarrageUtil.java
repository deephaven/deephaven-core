//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import com.google.flatbuffers.Constants;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.util.NameValidator;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.base.ArrayUtil;
import io.deephaven.base.ClassUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.GridAttributes;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.ComparatorRegistry;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageMessageWriterImpl;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.extensions.barrage.ColumnEncoding;
import io.deephaven.engine.util.ColumnFormatting;
import io.deephaven.engine.util.input.InputTableUpdater;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DefaultChunkWriterFactory;
import io.deephaven.extensions.barrage.chunk.ChunkReader;
import io.deephaven.extensions.barrage.chunk.vector.VectorExpansionKernel;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.DeephavenTableMetadata;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.InputTableMetadata;
import io.deephaven.proto.backplane.grpc.InputTableColumnInfo;
import io.deephaven.proto.flight.util.MessageHelper;
import io.deephaven.proto.flight.util.SchemaHelper;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.Vector;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.util.Collections2;
import org.apache.arrow.vector.PeriodDuration;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.ToIntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BarrageUtil {
    /**
     * Re-usable constant for the "true" value.
     */
    private static final String TRUE_STRING = Boolean.toString(true);

    public static final BarrageSubscriptionOptions DEFAULT_SUBSCRIPTION_OPTIONS =
            BarrageSubscriptionOptions.builder().build();
    public static final BarrageSnapshotOptions DEFAULT_SNAPSHOT_OPTIONS =
            BarrageSnapshotOptions.builder().build();

    public static final long FLATBUFFER_MAGIC = 0x6E687064;

    private static final Logger log = LoggerFactory.getLogger(BarrageUtil.class);

    public static final double TARGET_SNAPSHOT_PERCENTAGE =
            Configuration.getInstance().getDoubleForClassWithDefault(BarrageUtil.class,
                    "targetSnapshotPercentage", 0.25);

    public static final long MIN_SNAPSHOT_CELL_COUNT =
            Configuration.getInstance().getLongForClassWithDefault(BarrageUtil.class,
                    "minSnapshotCellCount", 1L << 13);
    public static final long MAX_SNAPSHOT_CELL_COUNT =
            Configuration.getInstance().getLongForClassWithDefault(BarrageUtil.class,
                    "maxSnapshotCellCount", 1L << 24);

    /** Whether to sample columns at schema-inference time to detect run patterns for REE auto-encoding. */
    private static final boolean REE_SAMPLING_ENABLED =
            Configuration.getInstance().getBooleanWithDefault("BarrageUtil.ree.samplingEnabled", true);

    /** Number of rows to sample per column when estimating run ratios. */
    private static final int REE_SAMPLE_SIZE =
            Configuration.getInstance().getIntegerWithDefault("BarrageUtil.ree.sampleSize", 1024);

    /**
     * Maximum ratio of (estimated runs / sampled rows) that triggers REE auto-encoding. At the default of 0.5, a column
     * must average at least 2 rows per run to be encoded.
     */
    private static final double REE_RUN_RATIO_THRESHOLD =
            Configuration.getInstance().getDoubleWithDefault("BarrageUtil.ree.runRatioThreshold", 0.5);

    /**
     * Minimum table row count before sampling is attempted; too few rows make the estimate noisy. Must be strictly less
     * than {@link #REE_SAMPLE_SIZE}.
     */
    static final int REE_MIN_SAMPLE_SIZE =
            Configuration.getInstance().getIntegerWithDefault("BarrageUtil.ree.minSampleSize", 16);

    /**
     * Note that arrow's wire format states that Timestamps without timezones are not UTC -- that they are no timezone
     * at all. It's very important that we mark these times as UTC.
     */
    public static final ArrowType.Timestamp NANO_SINCE_EPOCH_TYPE =
            new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC");

    /**
     * By default we'll use nanosecond resolution for Duration columns.
     */
    public static final ArrowType.Duration NANO_DURATION_TYPE =
            new ArrowType.Duration(TimeUnit.NANOSECOND);

    /**
     * The name of the attribute that indicates that a table is flat.
     */
    public static final String TABLE_ATTRIBUTE_IS_FLAT = "IsFlat";

    /**
     * The Apache Arrow metadata prefix for Deephaven attributes.
     */
    public static final String ATTR_DH_PREFIX = "deephaven:";

    /**
     * The deephaven metadata tag to indicate an attribute.
     */
    private static final String ATTR_ATTR_TAG = "attribute";

    /**
     * The deephaven metadata tag to indicate an attribute's type.
     */
    private static final String ATTR_ATTR_TYPE_TAG = "attribute_type";

    /**
     * The deephaven metadata tag to indicate the deephaven column type.
     */
    public static final String ATTR_TYPE_TAG = "type";

    /**
     * The deephaven metadata tag to indicate the deephaven column component type.
     */
    public static final String ATTR_COMPONENT_TYPE_TAG = "componentType";

    /**
     * The deephaven metadata tag to indicate the input table information.
     */
    public static final String ATTR_PROTO_METADATA_TAG = "tableMetadata";

    private static final boolean ENFORCE_FLATBUFFER_VERSION_CHECK =
            Configuration.getInstance().getBooleanWithDefault("barrage.version.check", true);

    static {
        verifyFlatbufferCompatibility(Message.class);
        verifyFlatbufferCompatibility(BarrageMessageWrapper.class);
    }

    private static void verifyFlatbufferCompatibility(Class<?> clazz) {
        try {
            clazz.getMethod("ValidateVersion").invoke(null);
        } catch (InvocationTargetException e) {
            Throwable targetException = e.getTargetException();
            if (targetException instanceof NoSuchMethodError) {
                // Caused when the reflective method is found and cannot be used because the flatbuffer version doesn't
                // match
                String requiredVersion = extractFlatBufferVersion(targetException.getMessage())
                        .orElseThrow(() -> new UncheckedDeephavenException(
                                "FlatBuffers version mismatch, can't read expected version", targetException));
                Optional<String> foundVersion = Arrays.stream(Constants.class.getDeclaredMethods())
                        .map(Method::getName)
                        .map(BarrageUtil::extractFlatBufferVersion)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .findFirst();
                String dependentLibrary = clazz.getPackage().getSpecificationTitle();
                final String message;
                if (foundVersion.isEmpty()) {
                    message = "Library '" + dependentLibrary + "' requires FlatBuffer " + requiredVersion
                            + ", cannot detect present version";
                } else {
                    message = "Library '" + dependentLibrary + "' requires FlatBuffer " + requiredVersion + ", found "
                            + foundVersion.get();
                }
                if (ENFORCE_FLATBUFFER_VERSION_CHECK) {
                    throw new UncheckedDeephavenException(message);
                } else {
                    log.warn().append(message).endl();
                }
            } else {
                throw new UncheckedDeephavenException("Cannot validate flatbuffer compatibility, unexpected exception",
                        targetException);
            }
        } catch (IllegalAccessException e) {
            throw new UncheckedDeephavenException(
                    "Cannot validate flatbuffer compatibility, " + clazz + "'s ValidateVersion() isn't accessible!", e);
        } catch (NoSuchMethodException e) {
            // Caused when the type isn't actually a flatbuffer Table (or the codegen format has changed)
            throw new UncheckedDeephavenException(
                    "Cannot validate flatbuffer compatibility, " + clazz + " is not a flatbuffer table!", e);
        }
    }

    private static Optional<String> extractFlatBufferVersion(String method) {
        Matcher matcher = Pattern.compile("FLATBUFFERS_([0-9]+)_([0-9]+)_([0-9]+)").matcher(method);

        if (matcher.find()) {
            if (Integer.valueOf(matcher.group(1)) <= 2) {
                // semver, third decimal doesn't matter
                return Optional.of(matcher.group(1) + "." + matcher.group(2) + ".x");
            }
            // "date" version, all three components should be shown
            return Optional.of(matcher.group(1) + "." + matcher.group(2) + "." + matcher.group(3));
        }
        return Optional.empty();
    }

    /**
     * These are the types that get special encoding but are otherwise not primitives. TODO (core#58): add custom
     * barrage serialization/deserialization support
     */
    @SuppressWarnings("unchecked")
    private static final Set<Class<?>> supportedTypes = new HashSet<>(Collections2.asImmutableList(
            BigDecimal.class,
            BigInteger.class,
            Boolean.class,
            Duration.class,
            Instant.class,
            LocalDate.class,
            LocalTime.class,
            Period.class,
            PeriodDuration.class,
            Schema.class,
            String.class,
            ZonedDateTime.class));

    /**
     * Create a subscription request payload to be sent via DoExchange.
     *
     * @param ticketId the ticket id of the table to subscribe to
     * @param options the barrage options
     * @return the subscription request payload
     */
    public static byte[] createSubscriptionRequestMetadataBytes(
            @NotNull final byte[] ticketId,
            @Nullable final BarrageSubscriptionOptions options) {
        return createSubscriptionRequestMetadataBytes(ticketId, options, null, null, false);
    }

    /**
     * Create a subscription request payload to be sent via DoExchange.
     *
     * @param ticketId the ticket id of the table to subscribe to
     * @param options the barrage options
     * @param viewport the viewport to subscribe to
     * @param columns the columns to subscribe to
     * @param reverseViewport whether to reverse the viewport
     * @return the subscription request payload
     */
    public static byte[] createSubscriptionRequestMetadataBytes(
            @NotNull final byte[] ticketId,
            @Nullable final BarrageSubscriptionOptions options,
            @Nullable final RowSet viewport,
            @Nullable final BitSet columns,
            final boolean reverseViewport) {
        return createSubscriptionRequestMetadataBytes(ticketId, options,
                viewport != null ? BarrageProtoUtil.toByteBuffer(viewport) : null,
                columns == null ? null : columns.toByteArray(), reverseViewport,
                BarrageMessageType.BarrageSubscriptionRequest);
    }

    /**
     * Create a subscription request payload to be sent via DoExchange.
     *
     * @param ticketId the ticket id of the table to subscribe to
     * @param options the barrage options
     * @param viewportBuffer the viewport to subscribe to, already converted to a ByteBuffer
     * @param columns the columns to subscribe to
     * @param reverseViewport whether to reverse the viewport
     * @param requestType the type of the request
     * @return the subscription request payload
     */
    public static byte[] createSubscriptionRequestMetadataBytes(
            @NotNull final byte[] ticketId,
            @Nullable final BarrageSubscriptionOptions options,
            final @Nullable ByteBuffer viewportBuffer,
            @Nullable final byte[] columns,
            final boolean reverseViewport,
            final byte requestType) {

        final FlatBufferBuilder metadata = new FlatBufferBuilder();

        int colOffset = 0;
        if (columns != null) {
            colOffset = BarrageSubscriptionRequest.createColumnsVector(metadata, columns);
        }
        int vpOffset = 0;
        if (viewportBuffer != null) {
            vpOffset = BarrageSubscriptionRequest.createViewportVector(
                    metadata, viewportBuffer);
        }
        int optOffset = 0;
        if (options != null) {
            optOffset = options.appendTo(metadata);
        }

        final int ticOffset = BarrageSubscriptionRequest.createTicketVector(metadata, ticketId);
        BarrageSubscriptionRequest.startBarrageSubscriptionRequest(metadata);
        BarrageSubscriptionRequest.addColumns(metadata, colOffset);
        BarrageSubscriptionRequest.addViewport(metadata, vpOffset);
        BarrageSubscriptionRequest.addSubscriptionOptions(metadata, optOffset);
        BarrageSubscriptionRequest.addTicket(metadata, ticOffset);
        BarrageSubscriptionRequest.addReverseViewport(metadata, reverseViewport);
        metadata.finish(BarrageSubscriptionRequest.endBarrageSubscriptionRequest(metadata));

        final FlatBufferBuilder wrapper = new FlatBufferBuilder();
        final int innerOffset = wrapper.createByteVector(metadata.dataBuffer());
        wrapper.finish(BarrageMessageWrapper.createBarrageMessageWrapper(
                wrapper,
                BarrageUtil.FLATBUFFER_MAGIC,
                requestType,
                innerOffset));
        return wrapper.sizedByteArray();
    }

    /**
     * Create a snapshot request payload to be sent via DoExchange.
     *
     * @param ticketId the ticket id of the table to subscribe to
     * @param options the barrage options
     * @return the subscription request payload
     */
    public static byte[] createSnapshotRequestMetadataBytes(
            @NotNull final byte[] ticketId,
            @Nullable final BarrageSnapshotOptions options) {
        return createSnapshotRequestMetadataBytes(ticketId, options, null, null, false);
    }

    /**
     * Create a subscription request payload to be sent via DoExchange.
     *
     * @param ticketId the ticket id of the table to subscribe to
     * @param options the barrage options
     * @param viewport the viewport to subscribe to
     * @param columns the columns to subscribe to
     * @param reverseViewport whether to reverse the viewport
     * @return the subscription request payload
     */
    static public byte[] createSnapshotRequestMetadataBytes(
            @NotNull final byte[] ticketId,
            @Nullable final BarrageSnapshotOptions options,
            @Nullable final RowSet viewport,
            @Nullable final BitSet columns,
            final boolean reverseViewport) {

        final FlatBufferBuilder metadata = new FlatBufferBuilder();

        int colOffset = 0;
        if (columns != null) {
            colOffset = BarrageSnapshotRequest.createColumnsVector(metadata, columns.toByteArray());
        }
        int vpOffset = 0;
        if (viewport != null) {
            vpOffset = BarrageSnapshotRequest.createViewportVector(
                    metadata, BarrageProtoUtil.toByteBuffer(viewport));
        }
        int optOffset = 0;
        if (options != null) {
            optOffset = options.appendTo(metadata);
        }

        final int ticOffset = BarrageSnapshotRequest.createTicketVector(metadata, ticketId);
        BarrageSnapshotRequest.startBarrageSnapshotRequest(metadata);
        BarrageSnapshotRequest.addColumns(metadata, colOffset);
        BarrageSnapshotRequest.addViewport(metadata, vpOffset);
        BarrageSnapshotRequest.addSnapshotOptions(metadata, optOffset);
        BarrageSnapshotRequest.addTicket(metadata, ticOffset);
        BarrageSnapshotRequest.addReverseViewport(metadata, reverseViewport);
        metadata.finish(BarrageSnapshotRequest.endBarrageSnapshotRequest(metadata));

        final FlatBufferBuilder wrapper = new FlatBufferBuilder();
        final int innerOffset = wrapper.createByteVector(metadata.dataBuffer());
        wrapper.finish(BarrageMessageWrapper.createBarrageMessageWrapper(
                wrapper,
                BarrageUtil.FLATBUFFER_MAGIC,
                BarrageMessageType.BarrageSnapshotRequest,
                innerOffset));
        return wrapper.sizedByteArray();
    }

    /**
     * Create a snapshot request payload to be sent via DoExchange.
     *
     * @param ticketId the ticket id of the table to subscribe to
     * @param options the barrage options
     * @return the subscription request payload
     */
    public static byte[] createSerializationOptionsMetadataBytes(
            @NotNull final byte[] ticketId,
            @Nullable final BarrageSubscriptionOptions options) {
        final FlatBufferBuilder metadata = new FlatBufferBuilder();

        int optOffset = 0;
        if (options != null) {
            optOffset = options.appendTo(metadata);
        }

        final int ticOffset = BarrageSubscriptionRequest.createTicketVector(metadata, ticketId);
        BarrageSubscriptionRequest.startBarrageSubscriptionRequest(metadata);
        BarrageSubscriptionRequest.addColumns(metadata, 0);
        BarrageSubscriptionRequest.addViewport(metadata, 0);
        BarrageSubscriptionRequest.addSubscriptionOptions(metadata, optOffset);
        BarrageSubscriptionRequest.addTicket(metadata, ticOffset);
        BarrageSubscriptionRequest.addReverseViewport(metadata, false);
        metadata.finish(BarrageSubscriptionRequest.endBarrageSubscriptionRequest(metadata));

        final FlatBufferBuilder wrapper = new FlatBufferBuilder();
        final int innerOffset = wrapper.createByteVector(metadata.dataBuffer());
        wrapper.finish(BarrageMessageWrapper.createBarrageMessageWrapper(
                wrapper,
                BarrageUtil.FLATBUFFER_MAGIC,
                BarrageMessageType.BarrageSerializationOptions,
                innerOffset));
        return wrapper.sizedByteArray();
    }

    public static ByteString schemaBytesFromTable(@NotNull final Table table) {
        return schemaBytes(fbb -> makeTableSchemaPayload(fbb, DEFAULT_SNAPSHOT_OPTIONS, table));
    }

    public static ByteString schemaBytesFromTableDefinition(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Map<String, Object> attributes,
            final boolean isFlat) {
        return schemaBytes(fbb -> makeSchema(DEFAULT_SNAPSHOT_OPTIONS, tableDefinition, attributes, isFlat)
                .getSchema(fbb));
    }

    public static Schema schemaFromTable(@NotNull final Table table) {
        return schemaForTable(DEFAULT_SNAPSHOT_OPTIONS, table);
    }

    private static Schema schemaForTable(
            @NotNull final BarrageOptions options,
            @NotNull final Table table) {
        if (table.hasAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE)) {
            return makeSchema(options, table.getDefinition(), table.getAttributes(), table.isFlat());
        }
        return makeSchema(options, table.getDefinition(), table.getAttributes(), table.isFlat(),
                inferEncodings(table));
    }

    public static ByteString schemaBytes(@NotNull final ToIntFunction<FlatBufferBuilder> schemaPayloadWriter) {

        // note that flight expects the Schema to be wrapped in a Message prefixed by a 4-byte identifier
        // (to detect end-of-stream in some cases) followed by the size of the flatbuffer message

        final FlatBufferBuilder builder = new FlatBufferBuilder();
        final int schemaOffset = schemaPayloadWriter.applyAsInt(builder);
        builder.finish(MessageHelper.wrapInMessage(builder, schemaOffset,
                org.apache.arrow.flatbuf.MessageHeader.Schema));

        return ByteStringAccess.wrap(MessageHelper.toIpcBytes(builder));
    }

    public static int makeTableSchemaPayload(
            @NotNull final FlatBufferBuilder builder,
            @NotNull final BarrageOptions options,
            @NotNull final Table table) {
        return schemaForTable(options, table).getSchema(builder);
    }

    public static Schema makeSchema(
            @NotNull final BarrageOptions options,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Map<String, Object> attributes,
            final boolean isFlat) {
        return makeSchema(options, tableDefinition, attributes, isFlat, Map.of());
    }

    private static Schema makeSchema(
            @NotNull final BarrageOptions options,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Map<String, Object> attributes,
            final boolean isFlat,
            @NotNull final Map<String, ColumnEncoding> encodings) {
        final Map<String, String> schemaMetadata = attributesToMetadata(attributes, isFlat);
        final Map<String, String> descriptions = GridAttributes.getColumnDescriptions(attributes);
        final InputTableUpdater inputTableUpdater = (InputTableUpdater) attributes.get(Table.INPUT_TABLE_ATTRIBUTE);
        maybeAddInputTableMetadata(tableDefinition, schemaMetadata, inputTableUpdater);
        final List<Field> fields = columnDefinitionsToFields(
                descriptions, inputTableUpdater, tableDefinition, tableDefinition.getColumns(),
                ignored -> new HashMap<>(),
                attributes, options.columnsAsList())
                .map(field -> encodings.get(field.getName()) == ColumnEncoding.RUN_END_ENCODED
                        ? toReeField(field)
                        : field)
                .collect(Collectors.toList());
        return new Schema(fields, schemaMetadata);
    }

    private static void maybeAddInputTableMetadata(@NotNull TableDefinition tableDefinition,
            Map<String, String> schemaMetadata, InputTableUpdater inputTableUpdater) {
        if (inputTableUpdater == null) {
            return;
        }

        final InputTableMetadata.Builder builder = InputTableMetadata.newBuilder();

        for (final ColumnDefinition<?> columnDefinition : tableDefinition.getColumns()) {
            final String name = columnDefinition.getName();

            final InputTableColumnInfo.Builder columnBuilder = InputTableColumnInfo.newBuilder();

            if (inputTableUpdater.getKeyNames().contains(name)) {
                columnBuilder.setKind(InputTableColumnInfo.Kind.KIND_KEY);
            } else if (inputTableUpdater.getValueNames().contains(name)) {
                columnBuilder.setKind(InputTableColumnInfo.Kind.KIND_VALUE);
            } else {
                continue;
            }

            final List<Any> columnRestrictions = inputTableUpdater.getColumnRestrictions(name);
            if (columnRestrictions != null) {
                columnBuilder.addAllRestrictions(columnRestrictions);
            }

            builder.putColumnInfo(name, columnBuilder.build());
        }

        final DeephavenTableMetadata metadata =
                DeephavenTableMetadata.newBuilder().setInputTableMetadata(builder).build();

        final byte[] bytes = metadata.toByteArray();
        final String base64 = Base64.getEncoder().encodeToString(bytes);
        putMetadata(schemaMetadata, ATTR_PROTO_METADATA_TAG, base64);
    }

    @NotNull
    public static Map<String, String> attributesToMetadata(@NotNull final Map<String, Object> attributes) {
        return attributesToMetadata(attributes, false);
    }

    @NotNull
    public static Map<String, String> attributesToMetadata(
            @NotNull final Map<String, Object> attributes,
            final boolean isFlat) {
        final Map<String, String> metadata = new HashMap<>();
        if (isFlat) {
            putMetadata(metadata, ATTR_ATTR_TAG + "." + TABLE_ATTRIBUTE_IS_FLAT, TRUE_STRING);
            putMetadata(metadata, ATTR_ATTR_TYPE_TAG + "." + TABLE_ATTRIBUTE_IS_FLAT,
                    Boolean.class.getCanonicalName());
        }
        for (final Map.Entry<String, Object> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            final Object val = entry.getValue();
            if (val instanceof Byte || val instanceof Short || val instanceof Integer ||
                    val instanceof Long || val instanceof Float || val instanceof Double ||
                    val instanceof Character || val instanceof Boolean || val instanceof String) {
                // Copy primitives as strings
                putMetadata(metadata, ATTR_ATTR_TAG + "." + key, val.toString());
                putMetadata(metadata, ATTR_ATTR_TYPE_TAG + "." + key, val.getClass().getCanonicalName());
            } else {
                // Mote which attributes have a value we couldn't send
                putMetadata(metadata, "unsent." + ATTR_ATTR_TAG + "." + key, "");
            }
        }
        return metadata;
    }

    public static Stream<Field> columnDefinitionsToFields(
            @NotNull final Map<String, String> columnDescriptions,
            @Nullable final InputTableUpdater inputTableUpdater,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Collection<ColumnDefinition<?>> columnDefinitions,
            @NotNull final Function<String, Map<String, String>> fieldMetadataFactory,
            @NotNull final Map<String, Object> attributes) {
        return columnDefinitionsToFields(columnDescriptions, inputTableUpdater, tableDefinition, columnDefinitions,
                fieldMetadataFactory, attributes, false);
    }

    private static boolean isDataTypeSortable(final Class<?> dataType) {
        return dataType.isPrimitive() || Comparable.class.isAssignableFrom(dataType)
                || ComparatorRegistry.INSTANCE.getComparator(dataType) != null;
    }

    public static Stream<Field> columnDefinitionsToFields(
            @NotNull final Map<String, String> columnDescriptions,
            @Nullable final InputTableUpdater inputTableUpdater,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Collection<ColumnDefinition<?>> columnDefinitions,
            @NotNull final Function<String, Map<String, String>> fieldMetadataFactory,
            @NotNull final Map<String, Object> attributes,
            final boolean columnsAsList) {
        boolean wireFormatSpecified = attributes.containsKey(Table.BARRAGE_SCHEMA_ATTRIBUTE);

        // Find columns that are sortable
        final Set<String> sortableColumns;
        if (attributes.containsKey(GridAttributes.SORTABLE_COLUMNS_ATTRIBUTE)) {
            final String[] restrictedSortColumns =
                    attributes.get(GridAttributes.SORTABLE_COLUMNS_ATTRIBUTE).toString().split(",");
            sortableColumns = Arrays.stream(restrictedSortColumns)
                    .filter(columnName -> isDataTypeSortable(tableDefinition.getColumn(columnName).getDataType()))
                    .collect(Collectors.toSet());
        } else {
            sortableColumns = columnDefinitions.stream()
                    .filter(column -> isDataTypeSortable(column.getDataType()))
                    .map(ColumnDefinition::getName)
                    .collect(Collectors.toSet());
        }

        final Set<String> formatColumns = new HashSet<>();
        final Map<String, Field> fieldMap = new LinkedHashMap<>();

        final Function<ColumnDefinition<?>, Field> fieldFor = (final ColumnDefinition<?> column) -> {
            Field field = fieldMap.get(column.getName());
            final String name = column.getName();
            Class<?> dataType = column.getDataType();
            Class<?> componentType = column.getComponentType();
            final Map<String, String> metadata = fieldMetadataFactory.apply(name);

            if (column.isPartitioning()) {
                putMetadata(metadata, "isPartitioning", TRUE_STRING);
            }
            if (sortableColumns.contains(name)) {
                putMetadata(metadata, "isSortable", TRUE_STRING);
            }

            // Wire up style and format column references
            final String styleFormatName = ColumnFormatting.getStyleFormatColumn(name);
            if (formatColumns.contains(styleFormatName)) {
                putMetadata(metadata, "styleColumn", styleFormatName);
            }
            final String numberFormatName = ColumnFormatting.getNumberFormatColumn(name);
            if (formatColumns.contains(numberFormatName)) {
                putMetadata(metadata, "numberFormatColumn", numberFormatName);
            }
            final String dateFormatName = ColumnFormatting.getDateFormatColumn(name);
            if (formatColumns.contains(dateFormatName)) {
                putMetadata(metadata, "dateFormatColumn", dateFormatName);
            }

            // Add type information
            if (wireFormatSpecified || isTypeNativelySupported(dataType)) {
                putMetadata(metadata, ATTR_TYPE_TAG, dataType.getCanonicalName());
                if (componentType != null) {
                    if (isTypeNativelySupported(componentType)) {
                        putMetadata(metadata, ATTR_COMPONENT_TYPE_TAG, componentType.getCanonicalName());
                    } else {
                        // Otherwise, component type will be converted to a string
                        putMetadata(metadata, ATTR_COMPONENT_TYPE_TAG, String.class.getCanonicalName());
                    }
                }
            } else {
                // Otherwise, send the data type as is, but we will serialize to a string
                putMetadata(metadata, ATTR_TYPE_TAG, dataType.getCanonicalName());
            }

            // Only one of these will be true, if any are true the column will not be visible
            if (ColumnFormatting.isRowStyleFormatColumn(name)) {
                putMetadata(metadata, "isRowStyle", TRUE_STRING);
            }
            if (ColumnFormatting.isStyleFormatColumn(name)) {
                putMetadata(metadata, "isStyle", TRUE_STRING);
            }
            if (ColumnFormatting.isNumberFormatColumn(name)) {
                putMetadata(metadata, "isNumberFormat", TRUE_STRING);
            }
            if (ColumnFormatting.isDateFormatColumn(name)) {
                putMetadata(metadata, "isDateFormat", TRUE_STRING);
            }

            final String columnDescription = columnDescriptions.get(name);
            if (columnDescription != null) {
                putMetadata(metadata, "description", columnDescription);
            }
            if (inputTableUpdater != null) {
                if (inputTableUpdater.getKeyNames().contains(name)) {
                    putMetadata(metadata, "inputtable.isKey", TRUE_STRING);
                }
                if (inputTableUpdater.getValueNames().contains(name)) {
                    putMetadata(metadata, "inputtable.isValue", TRUE_STRING);
                }
            }

            if (field != null) {
                final FieldType origType = field.getFieldType();
                // user defined metadata should override the default metadata
                metadata.putAll(field.getMetadata());
                final FieldType newType =
                        new FieldType(origType.isNullable(), origType.getType(), origType.getDictionary(), metadata);
                field = new Field(field.getName(), newType, field.getChildren());
            } else if (Vector.class.isAssignableFrom(dataType)) {
                field = arrowFieldForVectorType(name, dataType, componentType, metadata);
            } else {
                field = arrowFieldFor(name, dataType, componentType, metadata, columnsAsList);
            }

            if (columnsAsList) {
                final boolean nullable = false;
                final FieldType wrappedType =
                        new FieldType(nullable, Types.MinorType.LIST.getType(), null, field.getMetadata());
                field = new Field(field.getName(), wrappedType, Collections.singletonList(field));
            }
            return field;
        };

        if (wireFormatSpecified) {
            final Schema targetSchema = (Schema) attributes.get(Table.BARRAGE_SCHEMA_ATTRIBUTE);
            targetSchema.getFields().forEach(field -> fieldMap.put(field.getName(), field));

            fieldMap.keySet().stream()
                    .filter(ColumnFormatting::isFormattingColumn)
                    .forEach(formatColumns::add);

            final Map<String, ColumnDefinition<?>> columnDefinitionMap = new LinkedHashMap<>();
            columnDefinitions.stream().filter(column -> fieldMap.containsKey(column.getName()))
                    .forEach(column -> columnDefinitionMap.put(column.getName(), column));

            return fieldMap.keySet().stream().map(columnDefinitionMap::get).map(fieldFor);
        }

        // Find the format columns
        columnDefinitions.stream().map(ColumnDefinition::getName)
                .filter(ColumnFormatting::isFormattingColumn)
                .forEach(formatColumns::add);

        // Build metadata for columns and add the fields
        return columnDefinitions.stream().map(fieldFor);
    }

    public static void putMetadata(final Map<String, String> metadata, final String key, final String value) {
        metadata.put(ATTR_DH_PREFIX + key, value);
    }

    public static BarrageTypeInfo<Field> getDefaultType(@NotNull final Field field) {

        Class<?> explicitClass = null;
        final String explicitClassName = field.getMetadata().get(ATTR_DH_PREFIX + ATTR_TYPE_TAG);
        if (explicitClassName != null) {
            try {
                explicitClass = ClassUtil.lookupClass(explicitClassName);
            } catch (final ClassNotFoundException e) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        String.format("BarrageUtil could not find class: %s", explicitClassName));
            }
        }

        final String explicitComponentTypeName = field.getMetadata().get(ATTR_DH_PREFIX + ATTR_COMPONENT_TYPE_TAG);
        Class<?> columnComponentType = null;
        if (explicitComponentTypeName != null) {
            try {
                columnComponentType = ClassUtil.lookupClass(explicitComponentTypeName);
            } catch (final ClassNotFoundException e) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        String.format("BarrageUtil could not find class: %s", explicitComponentTypeName));
            }
        }

        if (field.getType().getTypeID() == ArrowType.ArrowTypeID.Map) {
            return new BarrageTypeInfo<>(Map.class, null, field);
        }

        if (field.getType().getTypeID() == ArrowType.ArrowTypeID.RunEndEncoded) {
            // The Deephaven column type is determined by the values child (child[1]); the run_ends
            // child (child[0]) is purely an index. Delegate to the values child so that its
            // deephaven:type / deephaven:componentType metadata are honoured.
            return getDefaultType(field.getChildren().get(1));
        }

        final Class<?> columnType = getDefaultType(field, explicitClass);
        if (columnComponentType == null && columnType.isArray()) {
            columnComponentType = columnType.getComponentType();
        }

        return new BarrageTypeInfo<>(columnType, columnComponentType, field);
    }

    private static Class<?> getDefaultType(
            final Field arrowField,
            final Class<?> explicitType) {
        if (explicitType != null) {
            return explicitType;
        }

        final String exMsg = "Schema did not include `" + ATTR_DH_PREFIX + ATTR_TYPE_TAG + "` metadata for field";
        switch (arrowField.getType().getTypeID()) {
            case Int:
                final ArrowType.Int intType = (ArrowType.Int) arrowField.getType();
                if (intType.getIsSigned()) {
                    // SIGNED
                    switch (intType.getBitWidth()) {
                        case 8:
                            return byte.class;
                        case 16:
                            return short.class;
                        case 32:
                            return int.class;
                        case 64:
                            return long.class;
                    }
                } else {
                    // UNSIGNED
                    switch (intType.getBitWidth()) {
                        case 8:
                            return short.class;
                        case 16:
                            return char.class;
                        case 32:
                            return long.class;
                        case 64:
                            return BigInteger.class;
                    }
                }
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                        " of intType(signed=" + intType.getIsSigned() + ", bitWidth=" + intType.getBitWidth() + ")");
            case Bool:
                if (arrowField.isNullable()) {
                    return Boolean.class;
                }
                return boolean.class;
            case Duration:
                return Duration.class;
            case Time:
                return LocalTime.class;
            case Date:
                return LocalDate.class;
            case Timestamp:
                final ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowField.getType();
                final String tz = timestampType.getTimezone();
                if ((tz == null || "UTC".equals(tz))) {
                    return Instant.class;
                } else {
                    return ZonedDateTime.class;
                }
            case FloatingPoint:
                final ArrowType.FloatingPoint floatingPointType = (ArrowType.FloatingPoint) arrowField.getType();
                switch (floatingPointType.getPrecision()) {
                    case HALF:
                    case SINGLE:
                        return float.class;
                    case DOUBLE:
                        return double.class;
                    default:
                        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                                " of floatingPointType(Precision=" + floatingPointType.getPrecision() + ")");
                }
            case Decimal:
                return BigDecimal.class;
            case Utf8:
                return java.lang.String.class;
            case Binary:
            case FixedSizeBinary:
                return byte[].class;
            case Interval:
                final ArrowType.Interval intervalType = (ArrowType.Interval) arrowField.getType();
                switch (intervalType.getUnit()) {
                    case DAY_TIME:
                        return Duration.class;
                    case YEAR_MONTH:
                        return Period.class;
                    case MONTH_DAY_NANO:
                        return PeriodDuration.class;
                    default:
                        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                                " of intervalType(IntervalUnit=" + intervalType.getUnit() + ")");
                }
            case Map:
                return Map.class;
            case RunEndEncoded:
                // Delegate to the values child (child[1]); run_ends (child[0]) is just an index.
                return getDefaultType(arrowField.getChildren().get(1), null);
            case Union:
            case Null:
                return Object.class;
            default:
                if (arrowField.getType().getTypeID() == ArrowType.ArrowTypeID.List
                        || arrowField.getType().getTypeID() == ArrowType.ArrowTypeID.ListView
                        || arrowField.getType().getTypeID() == ArrowType.ArrowTypeID.FixedSizeList) {
                    final Class<?> childType = getDefaultType(arrowField.getChildren().get(0), null);
                    return Array.newInstance(childType, 0).getClass();
                }
                if (arrowField.getType().getTypeID() == ArrowType.ArrowTypeID.Union) {
                    return Object.class;
                }
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                        " of type " + arrowField.getType().getTypeID().toString());
        }
    }

    public static class ConvertedArrowSchema {
        public final TableDefinition tableDef;
        public final Schema arrowSchema;
        public final Map<String, Object> attributes = new HashMap<>();

        private ConvertedArrowSchema(
                @NotNull final TableDefinition tableDef,
                @NotNull final Schema arrowSchema) {
            this.tableDef = tableDef;
            this.arrowSchema = arrowSchema;
        }

        public ChunkType[] computeWireChunkTypes() {
            return tableDef.getColumnStream()
                    .map(def -> {
                        final Field field = arrowSchema.findField(def.getName());
                        if (field != null && field.getType().getTypeID() == ArrowType.ArrowTypeID.Timestamp) {
                            // An Arrow timestamp is a long; so we should interpret it as such.
                            return ChunkType.Long;
                        }
                        return ReinterpretUtils.maybeConvertToWritablePrimitiveChunkType(def.getDataType());
                    })
                    .toArray(ChunkType[]::new);
        }

        public Class<?>[] computeWireTypes() {
            return tableDef.getColumnStream()
                    .map(ColumnDefinition::getDataType)
                    .map(ReinterpretUtils::maybeConvertToPrimitiveDataType)
                    .toArray(Class[]::new);
        }

        public Class<?>[] computeWireComponentTypes() {
            return tableDef.getColumnStream()
                    .map(ColumnDefinition::getComponentType).toArray(Class[]::new);
        }

        public ChunkReader<? extends Values>[] computeChunkReaders(
                @NotNull final ChunkReader.Factory chunkReaderFactory,
                @NotNull final org.apache.arrow.flatbuf.Schema schema,
                @NotNull final BarrageOptions barrageOptions) {
            // noinspection unchecked
            final ChunkReader<? extends Values>[] readers =
                    (ChunkReader<? extends Values>[]) new ChunkReader[tableDef.numColumns()];

            final List<ColumnDefinition<?>> columns = tableDef.getColumns();
            for (int ii = 0; ii < tableDef.numColumns(); ++ii) {
                final ColumnDefinition<?> columnDefinition = ReinterpretUtils.maybeConvertToPrimitive(columns.get(ii));
                final BarrageTypeInfo<org.apache.arrow.flatbuf.Field> typeInfo = BarrageTypeInfo.make(
                        columnDefinition.getDataType(), columnDefinition.getComponentType(), schema.fields(ii));
                readers[ii] = chunkReaderFactory.newReader(typeInfo, barrageOptions);
            }

            return readers;
        }
    }

    public static TableDefinition convertTableDefinition(@NotNull final ExportedTableCreationResponse response) {
        return convertArrowSchema(SchemaHelper.flatbufSchema(response)).tableDef;
    }

    public static ConvertedArrowSchema convertArrowSchema(@NotNull final ExportedTableCreationResponse response) {
        return convertArrowSchema(SchemaHelper.flatbufSchema(response));
    }

    public static ConvertedArrowSchema convertArrowSchema(@NotNull final org.apache.arrow.flatbuf.Schema schema) {
        return convertArrowSchema(schema, null);
    }

    public static ConvertedArrowSchema convertArrowSchema(
            @NotNull final org.apache.arrow.flatbuf.Schema schema,
            @Nullable final BarrageOptions options) {
        return convertArrowSchema(
                Schema.convertSchema(schema),
                options,
                schema.fieldsLength(),
                i -> Field.convertField(schema.fields(i)),
                i -> visitor -> {
                    final org.apache.arrow.flatbuf.Field field = schema.fields(i);
                    if (field.dictionary() != null) {
                        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                "Dictionary encoding is not supported: " + field.name());
                    }
                    for (int j = 0; j < field.customMetadataLength(); j++) {
                        final KeyValue keyValue = field.customMetadata(j);
                        visitor.accept(keyValue.key(), keyValue.value());
                    }
                },
                visitor -> {
                    for (int j = 0; j < schema.customMetadataLength(); j++) {
                        final KeyValue keyValue = schema.customMetadata(j);
                        visitor.accept(keyValue.key(), keyValue.value());
                    }
                });
    }

    public static ConvertedArrowSchema convertArrowSchema(final Schema schema) {
        return convertArrowSchema(schema, null);
    }

    public static ConvertedArrowSchema convertArrowSchema(
            final Schema schema,
            final BarrageOptions options) {
        return convertArrowSchema(
                schema,
                options,
                schema.getFields().size(),
                i -> schema.getFields().get(i),
                i -> visitor -> {
                    schema.getFields().get(i).getMetadata().forEach(visitor);
                },
                visitor -> schema.getCustomMetadata().forEach(visitor));
    }

    private static ConvertedArrowSchema convertArrowSchema(
            @NotNull final Schema schema,
            @Nullable final BarrageOptions options,
            final int numColumns,
            @NotNull final IntFunction<Field> getField,
            @NotNull final IntFunction<Consumer<BiConsumer<String, String>>> columnMetadataVisitor,
            @NotNull final Consumer<BiConsumer<String, String>> tableMetadataVisitor) {
        final ColumnDefinition<?>[] columns = new ColumnDefinition[numColumns];

        for (int i = 0; i < numColumns; ++i) {
            Field field = getField.apply(i);
            final String origName = field.getName();
            final String name = NameValidator.legalizeColumnName(origName);
            final MutableObject<Class<?>> type = new MutableObject<>();
            final MutableObject<Class<?>> componentType = new MutableObject<>();

            columnMetadataVisitor.apply(i).accept((key, value) -> {
                if (key.equals(ATTR_DH_PREFIX + ATTR_TYPE_TAG)) {
                    try {
                        type.setValue(ClassUtil.lookupClass(value));
                    } catch (final ClassNotFoundException e) {
                        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                String.format("BarrageUtil could not find class: %s", value));
                    }
                } else if (key.equals(ATTR_DH_PREFIX + ATTR_COMPONENT_TYPE_TAG)) {
                    try {
                        componentType.setValue(ClassUtil.lookupClass(value));
                    } catch (final ClassNotFoundException e) {
                        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                String.format("BarrageUtil could not find class: %s", value));
                    }
                }
            });

            // this has side effects such as type validation; must call even if dest type is well known
            if (options != null && options.columnsAsList()) {
                field = field.getChildren().get(0);
            }
            Class<?> defaultType = getDefaultType(field, type.getValue());

            if (type.getValue() == null) {
                type.setValue(defaultType);
            } else if (type.getValue() == boolean.class || type.getValue() == Boolean.class) {
                // force to boxed boolean to allow nullability in the column sources
                type.setValue(Boolean.class);
            }
            if (defaultType == ObjectVector.class && componentType.getValue() == null) {
                componentType.setValue(getDefaultType(field.getChildren().get(0), null));
            }
            columns[i] = ColumnDefinition.fromGenericType(name, type.getValue(), componentType.getValue());
        }

        final Schema resultSchema;
        if (options == null || !options.columnsAsList()) {
            resultSchema = schema;
        } else {
            // must unwrap each column's list wrapper
            final List<Field> unwrappedFields = new ArrayList<>(numColumns);
            for (final Field wrappedCol : schema.getFields()) {
                Assert.eq(wrappedCol.getType().getTypeID(), "wrappedCol.getType().getTypeID()",
                        ArrowType.ArrowTypeID.List, "ArrowType.ArrowTypeID.List");
                final Field realCol = wrappedCol.getChildren().get(0);
                unwrappedFields.add(new Field(wrappedCol.getName(), realCol.getFieldType(), realCol.getChildren()));
            }
            resultSchema = new Schema(unwrappedFields, schema.getCustomMetadata());
        }
        final ConvertedArrowSchema result = new ConvertedArrowSchema(TableDefinition.of(columns), resultSchema);

        final HashMap<String, String> attributeTypeMap = new HashMap<>();
        tableMetadataVisitor.accept((key, value) -> {
            final String isAttributePrefix = ATTR_DH_PREFIX + ATTR_ATTR_TAG + ".";
            final String isAttributeTypePrefix = ATTR_DH_PREFIX + ATTR_ATTR_TYPE_TAG + ".";
            if (key.startsWith(isAttributePrefix)) {
                result.attributes.put(key.substring(isAttributePrefix.length()), value);
            } else if (key.startsWith(isAttributeTypePrefix)) {
                attributeTypeMap.put(key.substring(isAttributeTypePrefix.length()), value);
            }
        });

        attributeTypeMap.forEach((attrKey, attrType) -> {
            if (!result.attributes.containsKey(attrKey)) {
                // ignore if we receive a type for an unsent key (server code won't do this)
                log.warn().append("Schema included ").append(ATTR_ATTR_TYPE_TAG).append(" tag but not a corresponding ")
                        .append(ATTR_ATTR_TAG).append(" tag for key ").append(attrKey).append(".").endl();
                return;
            }

            Object currValue = result.attributes.get(attrKey);
            if (!(currValue instanceof String)) {
                // we just inserted this as a string in the block above
                throw new IllegalStateException();
            }

            final String stringValue = (String) currValue;
            switch (attrType) {
                case "java.lang.Byte":
                    result.attributes.put(attrKey, Byte.valueOf(stringValue));
                    break;
                case "java.lang.Short":
                    result.attributes.put(attrKey, Short.valueOf(stringValue));
                    break;
                case "java.lang.Integer":
                    result.attributes.put(attrKey, Integer.valueOf(stringValue));
                    break;
                case "java.lang.Long":
                    result.attributes.put(attrKey, Long.valueOf(stringValue));
                    break;
                case "java.lang.Float":
                    result.attributes.put(attrKey, Float.valueOf(stringValue));
                    break;
                case "java.lang.Double":
                    result.attributes.put(attrKey, Double.valueOf(stringValue));
                    break;
                case "java.lang.Character":
                    result.attributes.put(attrKey, stringValue.isEmpty() ? (char) 0 : stringValue.charAt(0));
                    break;
                case "java.lang.Boolean":
                    result.attributes.put(attrKey, Boolean.valueOf(stringValue));
                    break;
                case "java.lang.String":
                    // leave as is
                    break;
                default:
                    log.warn().append("Schema included unsupported ").append(ATTR_ATTR_TYPE_TAG).append(" tag of '")
                            .append(attrType).append("' for key ").append(attrKey).append(".").endl();
            }
        });

        return result;
    }

    private static boolean isTypeNativelySupported(final Class<?> typ) {
        if (typ.isPrimitive() || TypeUtils.isBoxedType(typ) || supportedTypes.contains(typ)
                || Vector.class.isAssignableFrom(typ) || Instant.class == typ || ZonedDateTime.class == typ) {
            return true;
        }
        if (typ.isArray()) {
            return isTypeNativelySupported(typ.getComponentType());
        }
        return false;
    }

    /**
     * Returns {@code true} when the given Arrow SDK field is Run-End Encoded with Int16 (16-bit signed) run_ends. Used
     * to detect whether any column's chunk writer is constrained to batches of at most {@link Short#MAX_VALUE} rows.
     */
    public static boolean isReeInt16Field(final Field field) {
        if (!(field.getType() instanceof ArrowType.RunEndEncoded)) {
            return false;
        }
        final List<Field> children = field.getChildren();
        if (children.isEmpty()) {
            return false;
        }
        final Field runEnds = children.get(0);
        return (runEnds.getType() instanceof ArrowType.Int)
                && ((ArrowType.Int) runEnds.getType()).getBitWidth() == 16;
    }

    private static Field toReeField(final Field field) {
        if (field.getType().getTypeID() == ArrowType.ArrowTypeID.RunEndEncoded) {
            return field;
        }
        final Field runEndsField = new Field(
                "run_ends",
                new FieldType(false, Types.MinorType.INT.getType(), null),
                Collections.emptyList());
        final Field valuesField = new Field(
                "values",
                new FieldType(field.isNullable(), field.getType(), field.getDictionary(), Collections.emptyMap()),
                field.getChildren());
        return new Field(
                field.getName(),
                new FieldType(false, new ArrowType.RunEndEncoded(), null, field.getMetadata()),
                List.of(runEndsField, valuesField));
    }

    /**
     * Samples {@code sampleSize} rows from {@code rowSet} and returns the ratio of estimated run count to rows sampled.
     * A ratio near 0 means the column is highly repetitive; 1.0 means every consecutive pair differs.
     */
    static Map<String, ColumnEncoding> inferEncodings(@NotNull final Table table) {
        final Map<String, ColumnEncoding> encodings = new HashMap<>();

        // Single-value sources are an obvious win, should always be encoded.
        table.getColumnSourceMap().forEach((name, source) -> {
            if (source instanceof NullValueColumnSource || source instanceof SingleValueColumnSource) {
                encodings.put(name, ColumnEncoding.RUN_END_ENCODED);
            }
        });

        // Partition tables that have been merged will have constant key columns per region.
        if (Boolean.TRUE.equals(table.getAttribute(Table.MERGED_TABLE_ATTRIBUTE))
                && table.hasAttribute(Table.KEY_COLUMNS_ATTRIBUTE)) {
            for (final String col : ((String) table.getAttribute(Table.KEY_COLUMNS_ATTRIBUTE)).split(",")) {
                encodings.put(col, ColumnEncoding.RUN_END_ENCODED);
            }
        }

        // Sample the table to detect columns with repetitive data patterns.
        if (REE_SAMPLING_ENABLED) {
            ConstructSnapshot.callDataSnapshotFunction("BarrageUtil.inferEncodings",
                    ConstructSnapshot.makeSnapshotControl(false, table.isRefreshing(),
                            (NotificationStepSource) table),
                    (usePrev, beforeClockValue) -> sampleColumnsForREE(table, encodings, usePrev));
        }

        return encodings;
    }

    private static boolean sampleColumnsForREE(
            @NotNull final Table table,
            @NotNull final Map<String, ColumnEncoding> encodings,
            final boolean usePrev) {
        final RowSet rowSetToUse = usePrev ? table.getRowSet().prev() : table.getRowSet();
        if (rowSetToUse.size() < REE_MIN_SAMPLE_SIZE) {
            return true;
        }
        // Build a single RowSet of REE_MIN_SAMPLE_SIZE chunks evenly distributed across the rowset.
        final int chunkSize = (int) Math.min(REE_MIN_SAMPLE_SIZE, rowSetToUse.size());
        final int sampleSize = (int) Math.min(REE_SAMPLE_SIZE, rowSetToUse.size());
        final int numChunks = sampleSize / chunkSize;
        final int stride = (int) rowSetToUse.size() / numChunks;
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        try (final RowSequence.Iterator it = rowSetToUse.getRowSequenceIterator()) {
            while (it.hasMore()) {
                final RowSequence rows = it.getNextRowSequenceWithLength(chunkSize);
                builder.appendRowSequence(rows);
                it.getNextRowSequenceWithLength(stride - chunkSize);
            }
        }
        // For every column, fill a chunk with the sampled values and count the runs.
        try (final WritableRowSet sampleRowSet = builder.build();
                final WritableBooleanChunk<Values> isEqualNext =
                        WritableBooleanChunk.makeWritableChunk(sampleSize - 1)) {
            table.getColumnSourceMap().forEach((name, source) -> {
                if (encodings.containsKey(name)) {
                    return;
                }
                try (final ColumnSource.GetContext context = source.makeGetContext(sampleSize)) {
                    final Chunk<? extends Values> chunk = usePrev
                            ? source.getPrevChunk(context, sampleRowSet)
                            : source.getChunk(context, sampleRowSet);
                    ChunkEquals.makeEqual(source.getChunkType()).equalNext(chunk, isEqualNext);
                    int numRuns = 1;
                    for (int i = 0; i < isEqualNext.size(); ++i) {
                        if (!isEqualNext.get(i)) {
                            ++numRuns;
                        }
                    }
                    if ((double) numRuns / chunk.size() < REE_RUN_RATIO_THRESHOLD) {
                        encodings.put(name, ColumnEncoding.RUN_END_ENCODED);
                    }
                }
            });
        }
        return true;
    }

    public static Field arrowFieldFor(
            final String name,
            final Class<?> type,
            final Class<?> componentType,
            final Map<String, String> metadata,
            final boolean columnAsList) {
        List<Field> children = Collections.emptyList();

        final FieldType fieldType = arrowFieldTypeFor(type, metadata, columnAsList);
        if (fieldType.getType().isComplex()) {
            if (type.isArray()) {
                Assert.eq(componentType, "componentType", type.getComponentType(), "type.getComponentType()");
                children = Collections.singletonList(arrowFieldFor(
                        "", componentType, componentType.getComponentType(),
                        Collections.emptyMap(),
                        false));
            } else if (Vector.class.isAssignableFrom(type)) {
                Class<?> vectorComponentType =
                        componentType == null ? VectorExpansionKernel.getComponentType(type, null) : componentType;
                children = Collections.singletonList(arrowFieldFor(
                        "", vectorComponentType,
                        vectorComponentType == null ? null : vectorComponentType.getComponentType(),
                        Collections.emptyMap(),
                        false));
            } else {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "No default mapping for Arrow complex type: " + fieldType.getType());
            }
        }

        return new Field(name, fieldType, children);
    }

    public static org.apache.arrow.flatbuf.Field flatbufFieldFor(
            final ColumnDefinition<?> columnDefinition,
            final Map<String, String> metadata) {
        return flatbufFieldFor(
                columnDefinition.getName(),
                columnDefinition.getDataType(),
                columnDefinition.getComponentType(),
                metadata);
    }

    public static org.apache.arrow.flatbuf.Field flatbufFieldFor(
            final String name,
            final Class<?> type,
            final Class<?> componentType,
            final Map<String, String> metadata) {
        final Field field = arrowFieldFor(name, type, componentType, metadata, false);
        final FlatBufferBuilder builder = new FlatBufferBuilder();
        builder.finish(field.getField(builder));
        return org.apache.arrow.flatbuf.Field.getRootAsField(builder.dataBuffer());
    }

    private static FieldType arrowFieldTypeFor(
            final Class<?> type,
            final Map<String, String> metadata,
            final boolean columnAsList) {
        return new FieldType(true, arrowTypeFor(type, columnAsList), null, metadata);
    }

    private static ArrowType arrowTypeFor(
            Class<?> type,
            final boolean columnAsList) {
        if (TypeUtils.isBoxedType(type)) {
            type = TypeUtils.getUnboxedType(type);
        }
        final ChunkType chunkType = ChunkType.fromElementType(type);
        switch (chunkType) {
            case Boolean:
                return Types.MinorType.BIT.getType();
            case Char:
                return Types.MinorType.UINT2.getType();
            case Byte:
                return Types.MinorType.TINYINT.getType();
            case Short:
                return Types.MinorType.SMALLINT.getType();
            case Int:
                return Types.MinorType.INT.getType();
            case Long:
                return Types.MinorType.BIGINT.getType();
            case Float:
                return Types.MinorType.FLOAT4.getType();
            case Double:
                return Types.MinorType.FLOAT8.getType();
            case Object:
                if (type != null) {
                    if (type.isArray()) {
                        if (type.getComponentType() == byte.class && !columnAsList) {
                            return Types.MinorType.VARBINARY.getType();
                        }
                        return Types.MinorType.LIST.getType();
                    }
                    if (Vector.class.isAssignableFrom(type)) {
                        return Types.MinorType.LIST.getType();
                    }
                    if (type == LocalDate.class) {
                        return Types.MinorType.DATEMILLI.getType();
                    }
                    if (type == LocalTime.class) {
                        return Types.MinorType.TIMENANO.getType();
                    }
                    if (type == BigDecimal.class
                            || type == BigInteger.class
                            || type == Schema.class) {
                        return Types.MinorType.VARBINARY.getType();
                    }
                    if (type == Instant.class || type == ZonedDateTime.class) {
                        // Note: We are choosing to discard the time zone for a ZonedDateTime here.
                        return NANO_SINCE_EPOCH_TYPE;
                    }
                    if (type == Duration.class) {
                        return NANO_DURATION_TYPE;
                    }
                    if (type == Period.class) {
                        // TODO: DH-22159: Period support with Flight/barrage. (MONTH_DAY_NANO is probably a better
                        // choice; captures full precision of Period.)
                        return new ArrowType.Interval(IntervalUnit.YEAR_MONTH);
                    }
                    if (type == PeriodDuration.class) {
                        return new ArrowType.Interval(IntervalUnit.MONTH_DAY_NANO);
                    }
                }

                // everything gets converted to a string
                return Types.MinorType.VARCHAR.getType(); // aka Utf8
        }
        throw new IllegalStateException("No ArrowType for type: " + type + " w/chunkType: " + chunkType);
    }

    private static Field arrowFieldForVectorType(
            final String name, final Class<?> type, final Class<?> knownComponentType,
            final Map<String, String> metadata) {

        // Vectors are always lists.
        final FieldType fieldType = new FieldType(true, Types.MinorType.LIST.getType(), null, metadata);
        final Class<?> componentType = VectorExpansionKernel.getComponentType(type, knownComponentType);
        final Class<?> innerComponentType = componentType == null ? null : componentType.getComponentType();

        final List<Field> children = Collections.singletonList(arrowFieldFor(
                "", componentType, innerComponentType, Collections.emptyMap(), false));

        return new Field(name, fieldType, children);
    }

    /**
     * Returns the effective Arrow SDK schema for {@code table}, honoring {@link Table#BARRAGE_SCHEMA_ATTRIBUTE} when
     * present, otherwise building from the table definition with inferred REE encodings applied. This schema is
     * authoritative for both the schema IPC message and chunk-writer initialization.
     */
    private static Schema computeEffectiveSchema(
            @NotNull final BarrageOptions options,
            @NotNull final BaseTable<?> table) {
        if (table.hasAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE)) {
            return (Schema) table.getAttribute(Table.BARRAGE_SCHEMA_ATTRIBUTE);
        }
        return makeSchema(options, table.getDefinition(), table.getAttributes(), table.isFlat(), inferEncodings(table));
    }

    /**
     * Returns the maximum batch size imposed by the schema: {@link Short#MAX_VALUE} when any field uses Int16 Run-End
     * Encoding, otherwise {@link BarrageMessageWriterImpl#DEFAULT_BATCH_SIZE}.
     */
    private static int maxBatchSizeForSchema(@NotNull final Schema schema) {
        return schema.getFields().stream().anyMatch(BarrageUtil::isReeInt16Field)
                ? Short.MAX_VALUE
                : BarrageMessageWriterImpl.DEFAULT_BATCH_SIZE;
    }

    /**
     * Converts an Arrow SDK schema to a column-name → flatbuf-Field map for use by chunk writers.
     */
    private static Map<String, org.apache.arrow.flatbuf.Field> buildFlatbufFieldMap(
            @NotNull final Schema schema) {
        final Map<String, org.apache.arrow.flatbuf.Field> fieldFor = new HashMap<>();
        // noinspection DataFlowIssue
        schema.getFields().forEach(f -> {
            final FlatBufferBuilder fbb = new FlatBufferBuilder();
            final int offset = f.getField(fbb);
            fbb.finish(offset);
            fieldFor.put(f.getName(), org.apache.arrow.flatbuf.Field.getRootAsField(fbb.dataBuffer()));
        });
        return fieldFor;
    }

    /**
     * Returns snapshot options whose effective batch size does not exceed {@code maxBatchSize}. When
     * {@code maxBatchSize} equals {@link BarrageMessageWriterImpl#DEFAULT_BATCH_SIZE} the original options are returned
     * unchanged.
     */
    private static BarrageSnapshotOptions effectiveSnapshotOptions(
            @NotNull final BarrageSnapshotOptions options,
            final int maxBatchSize) {
        if (maxBatchSize == BarrageMessageWriterImpl.DEFAULT_BATCH_SIZE) {
            return options;
        }
        final int requested = options.batchSize();
        final int effective = requested <= 0 ? maxBatchSize : Math.min(requested, maxBatchSize);
        if (effective == requested) {
            return options;
        }
        return options.withBatchSize(effective);
    }

    public static void createAndSendStaticSnapshot(
            BarrageMessageWriter.Factory bmwFactory,
            BaseTable<?> table,
            BitSet columns,
            RowSet viewport,
            boolean reverseViewport,
            BarrageSnapshotOptions snapshotRequestOptions,
            StreamObserver<BarrageMessageWriter.MessageView> listener,
            BarragePerformanceLog.SnapshotMetricsHelper metrics) {
        // start with small value and grow
        long snapshotTargetCellCount = MIN_SNAPSHOT_CELL_COUNT;
        double snapshotNanosPerCell = 0.0;

        final Schema effectiveSchema = computeEffectiveSchema(snapshotRequestOptions, table);
        final Map<String, org.apache.arrow.flatbuf.Field> fieldFor = buildFlatbufFieldMap(effectiveSchema);
        snapshotRequestOptions =
                effectiveSnapshotOptions(snapshotRequestOptions, maxBatchSizeForSchema(effectiveSchema));

        // noinspection unchecked
        final ChunkWriter<Chunk<Values>>[] chunkWriters = table.getDefinition().getColumns().stream()
                .map(cd -> DefaultChunkWriterFactory.INSTANCE.newWriter(BarrageTypeInfo.make(
                        ReinterpretUtils.maybeConvertToPrimitiveDataType(cd.getDataType()),
                        cd.getComponentType(),
                        fieldFor.getOrDefault(cd.getName(), flatbufFieldFor(cd, Map.of())))))
                .toArray(ChunkWriter[]::new);

        final long columnCount =
                Math.max(1, columns != null ? columns.cardinality() : table.getDefinition().getColumns().size());

        try (final WritableRowSet snapshotViewport = RowSetFactory.empty();
                final WritableRowSet targetViewport = RowSetFactory.empty()) {
            // compute the target viewport
            if (viewport == null) {
                targetViewport.insertRange(0, table.size() - 1);
            } else if (!reverseViewport) {
                targetViewport.insert(viewport);
            } else {
                // compute the forward version of the reverse viewport
                try (final RowSet rowKeys = table.getRowSet().subSetForReversePositions(viewport);
                        final RowSet inverted = table.getRowSet().invert(rowKeys)) {
                    targetViewport.insert(inverted);
                }
            }

            try (final RowSequence.Iterator rsIt = targetViewport.getRowSequenceIterator()) {
                while (rsIt.hasMore()) {
                    // compute the next range to snapshot
                    final long cellCount = Math.max(
                            MIN_SNAPSHOT_CELL_COUNT, Math.min(snapshotTargetCellCount, MAX_SNAPSHOT_CELL_COUNT));
                    final long numRows = Math.min(Math.max(1, cellCount / columnCount), ArrayUtil.MAX_ARRAY_SIZE);

                    final RowSequence snapshotPartialViewport = rsIt.getNextRowSequenceWithLength(numRows);
                    // add these ranges to the running total
                    snapshotPartialViewport.forAllRowKeyRanges(snapshotViewport::insertRange);

                    // grab the snapshot and measure elapsed time for next projections
                    long start = System.nanoTime();
                    final BarrageMessage msg = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(
                            log, table, columns, snapshotPartialViewport, null);
                    msg.modColumnData = BarrageMessage.ZERO_MOD_COLUMNS; // no mod column data for DoGet
                    long elapsed = System.nanoTime() - start;
                    // accumulate snapshot time in the metrics
                    metrics.snapshotNanos += elapsed;

                    // send out the data. Note that although a `BarrageUpdateMetaData` object will
                    // be provided with each unique snapshot, vanilla Flight clients will ignore
                    // these and see only an incoming stream of batches
                    try (final BarrageMessageWriter bmw = bmwFactory.newMessageWriter(msg, chunkWriters, metrics)) {
                        if (rsIt.hasMore()) {
                            listener.onNext(bmw.getSnapshotView(snapshotRequestOptions,
                                    snapshotViewport, false,
                                    msg.rowsIncluded, columns));
                        } else {
                            listener.onNext(bmw.getSnapshotView(snapshotRequestOptions,
                                    viewport, reverseViewport,
                                    msg.rowsIncluded, columns));
                        }
                    }

                    if (!msg.rowsIncluded.isEmpty()) {
                        final long targetNanos = targetSnapshotTime(table.getUpdateGraph());

                        long nanosPerCell = elapsed / (msg.rowsIncluded.size() * columnCount);

                        // apply an exponential moving average to filter the data
                        if (snapshotNanosPerCell == 0) {
                            snapshotNanosPerCell = nanosPerCell; // initialize to first value
                        } else {
                            // EMA smoothing factor is 0.1 (N = 10)
                            snapshotNanosPerCell =
                                    (snapshotNanosPerCell * 0.9) + (nanosPerCell * 0.1);
                        }

                        snapshotTargetCellCount =
                                (long) (targetNanos / Math.max(1, snapshotNanosPerCell));
                    }
                }
            }
        }
    }

    /**
     * Very simplistic logic to take the last snapshot and extrapolate max number of rows that will not exceed the
     * target update graph processing time percentage.
     * 
     * @param updateGraph the update graph for the table
     * @return the target snapshot time, in nanos
     */
    public static long targetSnapshotTime(final UpdateGraph updateGraph) {
        long targetCycleDurationMillis;
        if (updateGraph instanceof PeriodicUpdateGraph) {
            final PeriodicUpdateGraph periodicUpdateGraph = updateGraph.cast();
            targetCycleDurationMillis = periodicUpdateGraph.getTargetCycleDurationMillis();
        } else {
            targetCycleDurationMillis = PeriodicUpdateGraph.getDefaultTargetCycleDurationMillis();
        }
        return (long) (TARGET_SNAPSHOT_PERCENTAGE * targetCycleDurationMillis * 1000000);
    }

    public static void createAndSendSnapshot(
            BarrageMessageWriter.Factory bwmFactory,
            BaseTable<?> table,
            BitSet columns, RowSet viewport, boolean reverseViewport,
            BarrageSnapshotOptions options,
            StreamObserver<BarrageMessageWriter.MessageView> listener,
            BarragePerformanceLog.SnapshotMetricsHelper metrics) {

        // if the table is static and a full snapshot is requested, we can make and send multiple
        // snapshots to save memory and operate more efficiently
        if (!table.isRefreshing()) {
            createAndSendStaticSnapshot(bwmFactory, table, columns, viewport, reverseViewport,
                    options, listener, metrics);
            return;
        }

        final Schema effectiveSchema = computeEffectiveSchema(options, table);
        final Map<String, org.apache.arrow.flatbuf.Field> fieldFor = buildFlatbufFieldMap(effectiveSchema);
        options = effectiveSnapshotOptions(options, maxBatchSizeForSchema(effectiveSchema));

        // noinspection unchecked
        final ChunkWriter<Chunk<Values>>[] chunkWriters = table.getDefinition().getColumns().stream()
                .map(cd -> DefaultChunkWriterFactory.INSTANCE.newWriter(BarrageTypeInfo.make(
                        ReinterpretUtils.maybeConvertToPrimitiveDataType(cd.getDataType()),
                        cd.getComponentType(),
                        fieldFor.getOrDefault(cd.getName(), flatbufFieldFor(cd, Map.of())))))
                .toArray(ChunkWriter[]::new);

        // otherwise snapshot the entire request and send to the client
        final BarrageMessage msg;

        final long snapshotStartTm = System.nanoTime();
        if (reverseViewport) {
            msg = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(log, table,
                    columns, null, viewport);
        } else {
            msg = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(log, table,
                    columns, viewport, null);
        }
        metrics.snapshotNanos = System.nanoTime() - snapshotStartTm;

        msg.modColumnData = BarrageMessage.ZERO_MOD_COLUMNS; // no mod column data

        // translate the viewport to keyspace and make the call
        try (final BarrageMessageWriter bmw = bwmFactory.newMessageWriter(msg, chunkWriters, metrics);
                final RowSet keySpaceViewport = viewport != null
                        ? msg.rowsAdded.subSetForPositions(viewport, reverseViewport)
                        : null) {
            listener.onNext(bmw.getSnapshotView(options, viewport, reverseViewport, keySpaceViewport, columns));
        }
    }
}
