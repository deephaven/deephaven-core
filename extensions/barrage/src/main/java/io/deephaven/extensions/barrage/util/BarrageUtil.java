//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import com.google.flatbuffers.Constants;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.api.util.NameValidator;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.base.ArrayUtil;
import io.deephaven.base.ClassUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.GridAttributes;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.updategraph.impl.PeriodicUpdateGraph;
import io.deephaven.engine.util.ColumnFormatting;
import io.deephaven.engine.util.input.InputTableUpdater;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageStreamGenerator;
import io.deephaven.extensions.barrage.chunk.ChunkReader;
import io.deephaven.extensions.barrage.chunk.DefaultChunkReadingFactory;
import io.deephaven.extensions.barrage.chunk.vector.VectorExpansionKernel;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.flight.util.MessageHelper;
import io.deephaven.proto.flight.util.SchemaHelper;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.type.TypeUtils;
import io.deephaven.vector.Vector;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.util.Collections2;
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
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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

import static io.deephaven.extensions.barrage.chunk.ChunkReader.typeInfo;

public class BarrageUtil {
    public static final BarrageSnapshotOptions DEFAULT_SNAPSHOT_DESER_OPTIONS =
            BarrageSnapshotOptions.builder().build();

    public static final long FLATBUFFER_MAGIC = 0x6E687064;

    private static final Logger log = LoggerFactory.getLogger(BarrageUtil.class);

    public static final double TARGET_SNAPSHOT_PERCENTAGE =
            Configuration.getInstance().getDoubleForClassWithDefault(BarrageUtil.class,
                    "targetSnapshotPercentage", 0.25);

    // TODO (deephaven-core#188): drop this default to 50k once the jsapi can handle many batches
    public static final long MIN_SNAPSHOT_CELL_COUNT =
            Configuration.getInstance().getLongForClassWithDefault(BarrageUtil.class,
                    "minSnapshotCellCount", Long.MAX_VALUE);
    public static final long MAX_SNAPSHOT_CELL_COUNT =
            Configuration.getInstance().getLongForClassWithDefault(BarrageUtil.class,
                    "maxSnapshotCellCount", Long.MAX_VALUE);

    /**
     * Note that arrow's wire format states that Timestamps without timezones are not UTC -- that they are no timezone
     * at all. It's very important that we mark these times as UTC.
     */
    public static final ArrowType.Timestamp NANO_SINCE_EPOCH_TYPE =
            new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC");

    /** The name of the attribute that indicates that a table is flat. */
    public static final String TABLE_ATTRIBUTE_IS_FLAT = "IsFlat";

    private static final String ATTR_DH_PREFIX = "deephaven:";
    private static final String ATTR_ATTR_TAG = "attribute";
    private static final String ATTR_ATTR_TYPE_TAG = "attribute_type";
    private static final String ATTR_TYPE_TAG = "type";
    private static final String ATTR_COMPONENT_TYPE_TAG = "componentType";

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
    private static final Set<Class<?>> supportedTypes = new HashSet<>(Collections2.<Class<?>>asImmutableList(
            BigDecimal.class,
            BigInteger.class,
            String.class,
            Instant.class,
            Boolean.class,
            LocalDate.class,
            LocalTime.class,
            Schema.class));

    public static ByteString schemaBytesFromTable(@NotNull final Table table) {
        return schemaBytesFromTableDefinition(table.getDefinition(), table.getAttributes(), table.isFlat());
    }

    public static ByteString schemaBytesFromTableDefinition(
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Map<String, Object> attributes,
            final boolean isFlat) {
        return schemaBytes(fbb -> makeTableSchemaPayload(
                fbb, DEFAULT_SNAPSHOT_DESER_OPTIONS, tableDefinition, attributes, isFlat));
    }

    public static Schema schemaFromTable(@NotNull final Table table) {
        return makeSchema(DEFAULT_SNAPSHOT_DESER_OPTIONS, table.getDefinition(), table.getAttributes(), table.isFlat());
    }

    public static Schema toSchema(final TableDefinition definition, Map<String, Object> attributes, boolean isFlat) {
        return makeSchema(DEFAULT_SNAPSHOT_DESER_OPTIONS, definition, attributes, isFlat);
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
            @NotNull final StreamReaderOptions options,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Map<String, Object> attributes,
            final boolean isFlat) {
        return makeSchema(options, tableDefinition, attributes, isFlat).getSchema(builder);
    }

    public static Schema makeSchema(
            @NotNull final StreamReaderOptions options,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Map<String, Object> attributes,
            final boolean isFlat) {
        final Map<String, String> schemaMetadata = attributesToMetadata(attributes, isFlat);
        final Map<String, String> descriptions = GridAttributes.getColumnDescriptions(attributes);
        final InputTableUpdater inputTableUpdater = (InputTableUpdater) attributes.get(Table.INPUT_TABLE_ATTRIBUTE);
        final List<Field> fields = columnDefinitionsToFields(
                descriptions, inputTableUpdater, tableDefinition, tableDefinition.getColumns(),
                ignored -> new HashMap<>(),
                attributes, options.columnsAsList())
                .collect(Collectors.toList());
        return new Schema(fields, schemaMetadata);
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
            putMetadata(metadata, ATTR_ATTR_TAG + "." + TABLE_ATTRIBUTE_IS_FLAT, "true");
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
                fieldMetadataFactory,
                attributes,
                false);
    }

    private static boolean isDataTypeSortable(final Class<?> dataType) {
        return dataType.isPrimitive() || Comparable.class.isAssignableFrom(dataType);
    }

    public static Stream<Field> columnDefinitionsToFields(
            @NotNull final Map<String, String> columnDescriptions,
            @Nullable final InputTableUpdater inputTableUpdater,
            @NotNull final TableDefinition tableDefinition,
            @NotNull final Collection<ColumnDefinition<?>> columnDefinitions,
            @NotNull final Function<String, Map<String, String>> fieldMetadataFactory,
            @NotNull final Map<String, Object> attributes,
            final boolean columnsAsList) {
        // Find the format columns
        final Set<String> formatColumns = new HashSet<>();
        columnDefinitions.stream().map(ColumnDefinition::getName)
                .filter(ColumnFormatting::isFormattingColumn)
                .forEach(formatColumns::add);

        // Find columns that are sortable
        Set<String> sortableColumns;
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

        // Build metadata for columns and add the fields
        return columnDefinitions.stream().map((final ColumnDefinition<?> column) -> {
            final String name = column.getName();
            Class<?> dataType = column.getDataType();
            Class<?> componentType = column.getComponentType();
            final Map<String, String> metadata = fieldMetadataFactory.apply(name);

            putMetadata(metadata, "isPartitioning", column.isPartitioning() + "");
            putMetadata(metadata, "isSortable", String.valueOf(sortableColumns.contains(name)));

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
            if (isTypeNativelySupported(dataType)) {
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
                // Otherwise, data type will be converted to a String
                putMetadata(metadata, ATTR_TYPE_TAG, String.class.getCanonicalName());
            }

            // Only one of these will be true, if any are true the column will not be visible
            putMetadata(metadata, "isRowStyle", ColumnFormatting.isRowStyleFormatColumn(name) + "");
            putMetadata(metadata, "isStyle", ColumnFormatting.isStyleFormatColumn(name) + "");
            putMetadata(metadata, "isNumberFormat", ColumnFormatting.isNumberFormatColumn(name) + "");
            putMetadata(metadata, "isDateFormat", ColumnFormatting.isDateFormatColumn(name) + "");

            final String columnDescription = columnDescriptions.get(name);
            if (columnDescription != null) {
                putMetadata(metadata, "description", columnDescription);
            }
            if (inputTableUpdater != null) {
                putMetadata(metadata, "inputtable.isKey", inputTableUpdater.getKeyNames().contains(name) + "");
            }

            if (columnsAsList) {
                componentType = dataType;
                dataType = Array.newInstance(dataType, 0).getClass();
            }

            if (Vector.class.isAssignableFrom(dataType)) {
                return arrowFieldForVectorType(name, dataType, componentType, metadata);
            }
            return arrowFieldFor(name, dataType, componentType, metadata);
        });
    }

    public static void putMetadata(final Map<String, String> metadata, final String key, final String value) {
        metadata.put(ATTR_DH_PREFIX + key, value);
    }

    private static boolean maybeConvertForTimeUnit(
            final TimeUnit unit,
            final ConvertedArrowSchema result,
            final int columnOffset) {
        switch (unit) {
            case NANOSECOND:
                return true;
            case MICROSECOND:
                setConversionFactor(result, columnOffset, 1000);
                return true;
            case MILLISECOND:
                setConversionFactor(result, columnOffset, 1000 * 1000);
                return true;
            case SECOND:
                setConversionFactor(result, columnOffset, 1000 * 1000 * 1000);
                return true;
            default:
                return false;
        }
    }

    private static Class<?> getDefaultType(
            final ArrowType arrowType,
            final ConvertedArrowSchema result,
            final int columnOffset,
            final Class<?> explicitType) {
        final String exMsg = "Schema did not include `" + ATTR_DH_PREFIX + ATTR_TYPE_TAG + "` metadata for field ";
        switch (arrowType.getTypeID()) {
            case Int:
                final ArrowType.Int intType = (ArrowType.Int) arrowType;
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
                            return int.class;
                        case 32:
                            return long.class;
                    }
                }
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                        " of intType(signed=" + intType.getIsSigned() + ", bitWidth=" + intType.getBitWidth() + ")");
            case Bool:
                return Boolean.class;
            case Duration:
                final ArrowType.Duration durationType = (ArrowType.Duration) arrowType;
                final TimeUnit durationUnit = durationType.getUnit();
                if (maybeConvertForTimeUnit(durationUnit, result, columnOffset)) {
                    return long.class;
                }
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                        " of durationType(unit=" + durationUnit.toString() + ")");
            case Timestamp:
                final ArrowType.Timestamp timestampType = (ArrowType.Timestamp) arrowType;
                final String tz = timestampType.getTimezone();
                final TimeUnit timestampUnit = timestampType.getUnit();
                boolean conversionSuccess = maybeConvertForTimeUnit(timestampUnit, result, columnOffset);
                if ((tz == null || "UTC".equals(tz)) && conversionSuccess) {
                    return Instant.class;
                }
                if (explicitType != null) {
                    return explicitType;
                }
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                        " of timestampType(Timezone=" + tz +
                        ", Unit=" + timestampUnit.toString() + ")");
            case FloatingPoint:
                final ArrowType.FloatingPoint floatingPointType = (ArrowType.FloatingPoint) arrowType;
                switch (floatingPointType.getPrecision()) {
                    case SINGLE:
                        return float.class;
                    case DOUBLE:
                        return double.class;
                    case HALF:
                    default:
                        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                                " of floatingPointType(Precision=" + floatingPointType.getPrecision().toString() + ")");
                }
            case Utf8:
                return java.lang.String.class;
            default:
                if (explicitType != null) {
                    return explicitType;
                }
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, exMsg +
                        " of type " + arrowType.getTypeID().toString());
        }
    }

    public static class ConvertedArrowSchema {
        public final int nCols;
        public TableDefinition tableDef;
        // a multiplicative factor to apply when reading; useful for eg converting arrow timestamp time units
        // to the expected nanos value for Instant.
        public int[] conversionFactors;
        public Map<String, Object> attributes;

        public ConvertedArrowSchema(final int nCols) {
            this.nCols = nCols;
        }

        public ChunkType[] computeWireChunkTypes() {
            return tableDef.getColumnStream()
                    .map(ColumnDefinition::getDataType)
                    .map(ReinterpretUtils::maybeConvertToWritablePrimitiveChunkType)
                    .toArray(ChunkType[]::new);
        }

        public Class<?>[] computeWireTypes() {
            return tableDef.getColumnStream().map(ColumnDefinition::getDataType).toArray(Class[]::new);
        }

        public Class<?>[] computeWireComponentTypes() {
            return tableDef.getColumnStream()
                    .map(ColumnDefinition::getComponentType).toArray(Class[]::new);
        }

        public ChunkReader[] computeChunkReaders(
                @NotNull final ChunkReader.Factory chunkReaderFactory,
                @NotNull final org.apache.arrow.flatbuf.Schema schema,
                @NotNull final StreamReaderOptions barrageOptions) {
            final ChunkReader[] readers = new ChunkReader[tableDef.numColumns()];

            final List<ColumnDefinition<?>> columns = tableDef.getColumns();
            for (int ii = 0; ii < tableDef.numColumns(); ++ii) {
                final ColumnDefinition<?> columnDefinition = columns.get(ii);
                final int factor = (conversionFactors == null) ? 1 : conversionFactors[ii];
                final ChunkReader.TypeInfo typeInfo = typeInfo(
                        ReinterpretUtils.maybeConvertToWritablePrimitiveChunkType(columnDefinition.getDataType()),
                        columnDefinition.getDataType(),
                        columnDefinition.getComponentType(),
                        schema.fields(ii));
                readers[ii] = DefaultChunkReadingFactory.INSTANCE.getReader(barrageOptions, factor, typeInfo);
            }

            return readers;
        }
    }

    private static void setConversionFactor(
            final ConvertedArrowSchema result,
            final int columnOffset,
            final int factor) {
        if (result.conversionFactors == null) {
            result.conversionFactors = new int[result.nCols];
            Arrays.fill(result.conversionFactors, 1);
        }
        result.conversionFactors[columnOffset] = factor;
    }

    public static TableDefinition convertTableDefinition(final ExportedTableCreationResponse response) {
        return convertArrowSchema(SchemaHelper.flatbufSchema(response)).tableDef;
    }

    public static ConvertedArrowSchema convertArrowSchema(final ExportedTableCreationResponse response) {
        return convertArrowSchema(SchemaHelper.flatbufSchema(response));
    }

    public static ConvertedArrowSchema convertArrowSchema(
            final org.apache.arrow.flatbuf.Schema schema) {
        return convertArrowSchema(
                schema.fieldsLength(),
                i -> schema.fields(i).name(),
                i -> ArrowType.getTypeForField(schema.fields(i)),
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
        return convertArrowSchema(
                schema.getFields().size(),
                i -> schema.getFields().get(i).getName(),
                i -> schema.getFields().get(i).getType(),
                i -> visitor -> {
                    schema.getFields().get(i).getMetadata().forEach(visitor);
                },
                visitor -> schema.getCustomMetadata().forEach(visitor));
    }

    private static ConvertedArrowSchema convertArrowSchema(
            final int numColumns,
            final IntFunction<String> getName,
            final IntFunction<ArrowType> getArrowType,
            final IntFunction<Consumer<BiConsumer<String, String>>> columnMetadataVisitor,
            final Consumer<BiConsumer<String, String>> tableMetadataVisitor) {
        final ConvertedArrowSchema result = new ConvertedArrowSchema(numColumns);
        final ColumnDefinition<?>[] columns = new ColumnDefinition[numColumns];

        for (int i = 0; i < numColumns; ++i) {
            final String origName = getName.apply(i);
            final String name = NameValidator.legalizeColumnName(origName);
            final MutableObject<Class<?>> type = new MutableObject<>();
            final MutableObject<Class<?>> componentType = new MutableObject<>();

            columnMetadataVisitor.apply(i).accept((key, value) -> {
                if (key.equals(ATTR_DH_PREFIX + ATTR_TYPE_TAG)) {
                    try {
                        type.setValue(ClassUtil.lookupClass(value));
                    } catch (final ClassNotFoundException e) {
                        throw new UncheckedDeephavenException("Could not load class from schema", e);
                    }
                } else if (key.equals(ATTR_DH_PREFIX + ATTR_COMPONENT_TYPE_TAG)) {
                    try {
                        componentType.setValue(ClassUtil.lookupClass(value));
                    } catch (final ClassNotFoundException e) {
                        throw new UncheckedDeephavenException("Could not load class from schema", e);
                    }
                }
            });

            // this has side effects such as setting the conversion factor; must call even if dest type is well known
            Class<?> defaultType = getDefaultType(getArrowType.apply(i), result, i, type.getValue());

            if (type.getValue() == null) {
                type.setValue(defaultType);
            } else if (type.getValue() == boolean.class || type.getValue() == Boolean.class) {
                // check existing barrage clients that might be sending int8 instead of bool
                // TODO (deephaven-core#3403) widen this check for better assurances
                Assert.eq(Boolean.class, "deephaven column type", defaultType, "arrow inferred type");
                // force to boxed boolean to allow nullability in the column sources
                type.setValue(Boolean.class);
            }
            columns[i] = ColumnDefinition.fromGenericType(name, type.getValue(), componentType.getValue());
        }

        result.tableDef = TableDefinition.of(columns);

        result.attributes = new HashMap<>();

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

    private static Field arrowFieldFor(
            final String name, final Class<?> type, final Class<?> componentType, final Map<String, String> metadata) {
        List<Field> children = Collections.emptyList();

        final FieldType fieldType = arrowFieldTypeFor(type, metadata);
        if (fieldType.getType().isComplex()) {
            if (type.isArray()) {
                children = Collections.singletonList(arrowFieldFor(
                        "", componentType, componentType.getComponentType(), Collections.emptyMap()));
            } else {
                throw new UnsupportedOperationException("Arrow Complex Type Not Supported: " + fieldType.getType());
            }
        }

        return new Field(name, fieldType, children);
    }

    private static FieldType arrowFieldTypeFor(final Class<?> type, final Map<String, String> metadata) {
        return new FieldType(true, arrowTypeFor(type), null, metadata);
    }

    private static ArrowType arrowTypeFor(Class<?> type) {
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
                if (type.isArray()) {
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
                    return NANO_SINCE_EPOCH_TYPE;
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
        final List<Field> children = Collections.singletonList(arrowFieldFor(
                "", componentType, componentType.getComponentType(), Collections.emptyMap()));

        return new Field(name, fieldType, children);
    }

    public static void createAndSendStaticSnapshot(
            BarrageStreamGenerator.Factory streamGeneratorFactory,
            BaseTable<?> table,
            BitSet columns,
            RowSet viewport,
            boolean reverseViewport,
            BarrageSnapshotOptions snapshotRequestOptions,
            StreamObserver<BarrageStreamGenerator.MessageView> listener,
            BarragePerformanceLog.SnapshotMetricsHelper metrics) {
        // start with small value and grow
        long snapshotTargetCellCount = MIN_SNAPSHOT_CELL_COUNT;
        double snapshotNanosPerCell = 0.0;

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
                    try (final BarrageStreamGenerator bsg = streamGeneratorFactory.newGenerator(msg, metrics)) {
                        if (rsIt.hasMore()) {
                            listener.onNext(bsg.getSnapshotView(snapshotRequestOptions,
                                    snapshotViewport, false,
                                    msg.rowsIncluded, columns));
                        } else {
                            listener.onNext(bsg.getSnapshotView(snapshotRequestOptions,
                                    viewport, reverseViewport,
                                    msg.rowsIncluded, columns));
                        }
                    }

                    if (!msg.rowsIncluded.isEmpty()) {
                        // very simplistic logic to take the last snapshot and extrapolate max
                        // number of rows that will not exceed the target UGP processing time
                        // percentage
                        PeriodicUpdateGraph updateGraph = table.getUpdateGraph().cast();
                        long targetNanos = (long) (TARGET_SNAPSHOT_PERCENTAGE
                                * updateGraph.getTargetCycleDurationMillis()
                                * 1000000);

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

    public static void createAndSendSnapshot(
            BarrageStreamGenerator.Factory streamGeneratorFactory,
            BaseTable<?> table,
            BitSet columns, RowSet viewport, boolean reverseViewport,
            BarrageSnapshotOptions snapshotRequestOptions,
            StreamObserver<BarrageStreamGenerator.MessageView> listener,
            BarragePerformanceLog.SnapshotMetricsHelper metrics) {

        // if the table is static and a full snapshot is requested, we can make and send multiple
        // snapshots to save memory and operate more efficiently
        if (!table.isRefreshing()) {
            createAndSendStaticSnapshot(streamGeneratorFactory, table, columns, viewport, reverseViewport,
                    snapshotRequestOptions, listener, metrics);
            return;
        }

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
        try (final BarrageStreamGenerator bsg = streamGeneratorFactory.newGenerator(msg, metrics);
                final RowSet keySpaceViewport = viewport != null
                        ? msg.rowsAdded.subSetForPositions(viewport, reverseViewport)
                        : null) {
            listener.onNext(bsg.getSnapshotView(
                    snapshotRequestOptions, viewport, reverseViewport, keySpaceViewport, columns));
        }
    }
}
