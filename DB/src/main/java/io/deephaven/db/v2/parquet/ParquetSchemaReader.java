package io.deephaven.db.v2.parquet;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.parquet.metadata.CodecInfo;
import io.deephaven.db.v2.parquet.metadata.ColumnTypeInfo;
import io.deephaven.db.v2.parquet.metadata.TableInfo;
import io.deephaven.parquet.ParquetFileReader;
import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import io.deephaven.util.codec.SimpleByteArrayCodec;
import io.deephaven.util.codec.UTF8StringAsByteArrayCodec;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class ParquetSchemaReader {
    @FunctionalInterface
    public interface ColumnDefinitionConsumer {
        void accept(final ParquetMessageDefinition def);
    }

    public static final class ParquetMessageDefinition {
        /** Yes you guessed right.  This is the column name. */
        public String name;
        /** The parquet type. */
        public Class<?> baseType;
        /** Some types require special annotations to support regular parquet tools and efficient DH handling.
         * Examples are StringSet and DbArray; a parquet file with a DbIntArray special type metadata annotation,
         * but storing types as repeated int, can be loaded both by other parquet tools and efficiently by DH. */
        public ColumnTypeInfo.SpecialType dhSpecialType;
        /** Parquet 1.0 did not support logical types; if we encounter a type like this is true.
         * For example, in parquet 1.0 binary columns with no annotation are used to represent strings.
         * They are also used to represent other things that are not strings.  Good luck, may the force be with you. */
        public boolean noLogicalType;
        /** Your guess is good here */
        public boolean isArray;
        /** Your guess is good here. */
        public boolean isGrouping;
        /** When codec metadata is present (which will be returned as modified read instructions below for actual codec
         * name and args), we expect codec type and component type to be present.  When they are present,
         * codecType and codecComponentType take precedence over any other DH type deducing heuristics (and
         * thus baseType in this structure can be ignored). */
        public String codecType;
        public String codecComponentType;
        /** We reuse the guts of this poor object between calls to avoid allocating.
         * Like prometheus nailed to a mountain, this poor object has to suffer his guts being eaten forever.
         * Or not forever but at least for one stack frame activation of readParquetSchema and as many columns
         * that function finds in the file. */
        void reset() {
            name = null;
            baseType = null;
            dhSpecialType = null;
            noLogicalType = isArray = isGrouping = false;
            codecType = codecComponentType = null;
        }
    }

    /**
     * Obtain schema information from a parquet file
     *
     * @param filePath          Location for input parquet file
     * @param readInstructions Parquet read instructions specifying transformations like column mappings and codecs.
     *                         Note a new read instructions based on this one may be returned by this method to provide necessary
     *                         transformations, eg, replacing unsupported characters like ' ' (space) in column names.
     * @param consumer         A ColumnDefinitionConsumer whose accept method would be called for each column in the file
     * @return Parquet read instructions, either the ones supplied or a new object based on the supplied with necessary
     * transformations added.
     */
    public static ParquetInstructions readParquetSchema(
            @NotNull final String filePath,
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final ColumnDefinitionConsumer consumer,
            @NotNull final BiFunction<String, Set<String>, String> legalizeColumnNameFunc
    ) throws IOException {
        final ParquetFileReader parquetFileReader = ParquetTools.getParquetFileReader(new File(filePath));
        final ParquetMetadata parquetMetadata = new ParquetMetadataConverter().fromParquetMetadata(parquetFileReader.fileMetaData);
        return readParquetSchema(parquetFileReader.getSchema(), parquetMetadata.getFileMetaData().getKeyValueMetaData(), readInstructions, consumer, legalizeColumnNameFunc);
    }

    public static Optional<TableInfo> parseMetadata(@NotNull final Map<String, String> keyValueMetadata) {
        final String tableInfoRaw = keyValueMetadata.get(ParquetTableWriter.METADATA_KEY);
        if (tableInfoRaw == null) {
            return Optional.empty();
        }
        try {
            return Optional.of(TableInfo.deserializeFromJSON(tableInfoRaw));
        } catch (JsonProcessingException e) {
            throw new TableDataException("Failed to parse " + ParquetTableWriter.METADATA_KEY + " metadata", e);
        }
    }

    /**
     * Obtain schema information from a parquet file
     *
     * @param schema           Parquet schema.
     *                         DO NOT RELY ON {@link ParquetMetadataConverter} FOR THIS! USE {@link ParquetFileReader}!
     * @param keyValueMetadata Parquet key-value metadata map
     * @param readInstructions Parquet read instructions specifying transformations like column mappings and codecs.
     *                         Note a new read instructions based on this one may be returned by this method to provide necessary
     *                         transformations, eg, replacing unsupported characters like ' ' (space) in column names.
     * @param consumer         A ColumnDefinitionConsumer whose accept method would be called for each column in the file
     * @return Parquet read instructions, either the ones supplied or a new object based on the supplied with necessary
     * transformations added.
     */
    public static ParquetInstructions readParquetSchema(
            @NotNull final MessageType schema,
            @NotNull final Map<String, String> keyValueMetadata,
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final ColumnDefinitionConsumer consumer,
            @NotNull final BiFunction<String, Set<String>, String> legalizeColumnNameFunc
    ) {
        final MutableObject<String> errorString = new MutableObject<>();
        final MutableObject<ColumnDescriptor> currentColumn = new MutableObject<>();
        final Optional<TableInfo> tableInfo = parseMetadata(keyValueMetadata);
        final Set<String> groupingColumnNames = tableInfo.map(TableInfo::groupingColumnNames).orElse(Collections.emptySet());
        final Map<String, ColumnTypeInfo> nonDefaultTypeColumns = tableInfo.map(TableInfo::columnTypeMap).orElse(Collections.emptyMap());
        final LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Class<?>> visitor = getVisitor(nonDefaultTypeColumns, errorString, currentColumn);

        final MutableObject<ParquetInstructions.Builder> instructionsBuilder = new MutableObject<>();
        final Supplier<ParquetInstructions.Builder> builderSupplier = () -> {
            if (instructionsBuilder.getValue() == null) {
                instructionsBuilder.setValue(new ParquetInstructions.Builder(readInstructions));
            }
            return instructionsBuilder.getValue();
        };
        final ParquetMessageDefinition colDef = new ParquetMessageDefinition();
        final Map<String, String[]> parquetColumnNameToFirstPath = new HashMap<>();
        for (final ColumnDescriptor column : schema.getColumns()) {
            if (column.getMaxRepetitionLevel() > 1) {
                // TODO (https://github.com/deephaven/deephaven-core/issues/871): Support this
                throw new UnsupportedOperationException("Unsupported maximum repetition level "
                        + column.getMaxRepetitionLevel() + " in column " + String.join("/", column.getPath()));
            }

            colDef.reset();
            currentColumn.setValue(column);
            final PrimitiveType primitiveType = column.getPrimitiveType();
            final LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();

            final String parquetColumnName = column.getPath()[0];
            parquetColumnNameToFirstPath.compute(parquetColumnName, (final String pcn, final String[] oldPath) -> {
                if (oldPath != null) {
                    // TODO (https://github.com/deephaven/deephaven-core/issues/871): Support this
                    throw new UnsupportedOperationException("Encountered unsupported multi-column field "
                            + parquetColumnName + ": found columns " + String.join("/", oldPath) + " and " + String.join("/", column.getPath()));
                }
                return column.getPath();
            });
            final String colName;
            final String mappedName = readInstructions.getColumnNameFromParquetColumnName(parquetColumnName);
            if (mappedName != null) {
                colName = mappedName;
            } else {
                final String legalized = legalizeColumnNameFunc.apply(
                        parquetColumnName,
                        (instructionsBuilder.getValue() == null)
                                ? Collections.emptySet()
                                : instructionsBuilder.getValue().getTakenNames());
                if (!legalized.equals(parquetColumnName)) {
                    colName = legalized;
                    builderSupplier.get().addColumnNameMapping(parquetColumnName, colName);
                } else {
                    colName = parquetColumnName;
                }
            }
            final Optional<ColumnTypeInfo> columnTypeInfo = Optional.ofNullable(nonDefaultTypeColumns.get(colName));

            colDef.name = colName;
            colDef.dhSpecialType = columnTypeInfo.flatMap(ColumnTypeInfo::specialType).orElse(null);
            colDef.isGrouping = groupingColumnNames.contains(colName);
            final Optional<CodecInfo> codecInfo = columnTypeInfo.flatMap(ColumnTypeInfo::codec);
            String codecName = codecInfo.map(CodecInfo::codecName).orElse(null);
            String codecArgs = codecInfo.flatMap(CodecInfo::codecArg).orElse(null);
            colDef.codecType = codecInfo.map(CodecInfo::dataType).orElse(null);
            if (codecName != null && !codecName.isEmpty()) {
                builderSupplier.get().addColumnCodec(colName, codecName, codecArgs);
            }
            colDef.isArray = column.getMaxRepetitionLevel() > 0;
            if (colDef.codecType != null && !colDef.codecType.isEmpty()) {
                colDef.codecComponentType = codecInfo.flatMap(CodecInfo::componentType).orElse(null);
                consumer.accept(colDef);
                continue;
            }
            if (logicalTypeAnnotation == null) {
                colDef.noLogicalType = true;
                final PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();
                switch (typeName) {
                    case BOOLEAN:
                        colDef.baseType = boolean.class;
                        break;
                    case INT32:
                        colDef.baseType = int.class;
                        break;
                    case INT64:
                        colDef.baseType = long.class;
                        break;
                    case INT96:
                        colDef.baseType = DBDateTime.class;
                        break;
                    case DOUBLE:
                        colDef.baseType = double.class;
                        break;
                    case FLOAT:
                        colDef.baseType = float.class;
                        break;
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                        if (colDef.dhSpecialType != null) {
                            if (colDef.dhSpecialType == ColumnTypeInfo.SpecialType.StringSet) {
                                colDef.baseType = null;  // when dhSpecialType is set, it takes precedence.
                                colDef.isArray = true;
                            } else {
                                // We don't expect to see any other special types here.
                                throw new UncheckedDeephavenException(
                                        "BINARY or FIXED_LEN_BYTE_ARRAY type " + column.getPrimitiveType()
                                            + " for column " + Arrays.toString(column.getPath())
                                            + " with unknown special type " + colDef.dhSpecialType);
                            }
                        } else if (codecName == null || codecName.isEmpty()) {
                            codecArgs = (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                    ? Integer.toString(primitiveType.getTypeLength())
                                    : null;
                            if (readInstructions.isLegacyParquet()) {
                                colDef.baseType = String.class;
                                codecName = UTF8StringAsByteArrayCodec.class.getName();
                            } else {
                                colDef.baseType = byte.class;
                                colDef.isArray = true;
                                codecName = SimpleByteArrayCodec.class.getName();
                            }
                            builderSupplier.get().addColumnCodec(colName, codecName, codecArgs);
                        }
                        break;
                    default:
                        throw new UncheckedDeephavenException(
                                "Unhandled type " + column.getPrimitiveType()
                                        + " for column " + Arrays.toString(column.getPath()));
                }
            } else {
                colDef.baseType = logicalTypeAnnotation.accept(visitor).orElseThrow(() -> {
                    final String logicalTypeString = errorString.getValue();
                    String msg = "Unable to read column " + Arrays.toString(column.getPath()) + ": ";
                    msg += (logicalTypeString != null)
                            ? (logicalTypeString + " not supported")
                            : "no mappable logical type annotation found"
                        ;
                    return new UncheckedDeephavenException(msg);
                });
            }
            consumer.accept(colDef);
        }
        return (instructionsBuilder.getValue() == null)
                ? readInstructions
                : instructionsBuilder.getValue().build()
                ;
    }

    private static LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Class<?>> getVisitor(
            final Map<String, ColumnTypeInfo> nonDefaultTypeColumns,
            final MutableObject<String> errorString,
            final MutableObject<ColumnDescriptor> currentColumn) {
        return new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Class<?>>() {
            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
                final ColumnDescriptor column = currentColumn.getValue();
                final String columnName = column.getPath()[0];
                final ColumnTypeInfo columnTypeInfo = nonDefaultTypeColumns.get(columnName);
                final ColumnTypeInfo.SpecialType specialType = columnTypeInfo == null ? null : columnTypeInfo.specialType().orElse(null);
                if (specialType != null) {
                    if (specialType == ColumnTypeInfo.SpecialType.StringSet) {
                        return Optional.of(StringSet.class);
                    }
                    if (specialType != ColumnTypeInfo.SpecialType.Vector) {
                        throw new UncheckedDeephavenException("Type " + column.getPrimitiveType()
                                + " for column " + Arrays.toString(column.getPath())
                                + " with unknown or incompatible special type " + specialType);
                    }
                }
                return Optional.of(String.class);
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.MapLogicalTypeAnnotation mapLogicalType) {
                errorString.setValue("MapLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.ListLogicalTypeAnnotation listLogicalType) {
                errorString.setValue("ListLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
                errorString.setValue("EnumLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
                errorString.setValue("DecimalLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
                errorString.setValue("DateLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
                errorString.setValue("TimeLogicalType, isAdjustedToUTC=" + timeLogicalType.isAdjustedToUTC());
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
                if (timestampLogicalType.isAdjustedToUTC()) {
                    switch(timestampLogicalType.getUnit()) {
                        case MILLIS:
                        case MICROS:
                        case NANOS:
                            return Optional.of(io.deephaven.db.tables.utils.DBDateTime.class);
                    }
                }
                errorString.setValue("TimestampLogicalType, isAdjustedToUTC=" + timestampLogicalType.isAdjustedToUTC() + ", unit=" + timestampLogicalType.getUnit());
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
                // Ensure this stays in sync with ReadOnlyParquetTableLocation.LogicalTypeVisitor.
                if (intLogicalType.isSigned()) {
                    switch (intLogicalType.getBitWidth()) {
                        case 64:
                            return Optional.of(long.class);
                        case 32:
                            return Optional.of(int.class);
                        case 16:
                            return Optional.of(short.class);
                        case 8:
                            return Optional.of(byte.class);
                        default:
                            // fallthrough.
                    }
                } else {
                    switch (intLogicalType.getBitWidth()) {
                        // uint16 maps to java's char;
                        // other unsigned types are promoted.
                        case 8:
                        case 16:
                            return Optional.of(char.class);
                        case 32:
                            return Optional.of(long.class);
                        default:
                            // fallthrough.
                    }
                }
                errorString.setValue("IntLogicalType, isSigned=" + intLogicalType.isSigned() + ", bitWidth=" + intLogicalType.getBitWidth());
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
                errorString.setValue("JsonLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
                errorString.setValue("BsonLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.UUIDLogicalTypeAnnotation uuidLogicalType) {
                errorString.setValue("UUIDLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.IntervalLogicalTypeAnnotation intervalLogicalType) {
                errorString.setValue("IntervalLogicalType");
                return Optional.empty();
            }

            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.MapKeyValueTypeAnnotation mapKeyValueLogicalType) {
                errorString.setValue("MapKeyValueType");
                return Optional.empty();
            }
        };
    }
}
