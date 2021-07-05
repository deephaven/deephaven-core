package io.deephaven.db.v2.parquet;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.ClassUtil;
import io.deephaven.db.tables.libs.StringSet;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.parquet.*;
import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import io.deephaven.parquet.utils.CachedChannelProvider;
import io.deephaven.parquet.utils.LocalFSChannelProvider;
import io.deephaven.parquet.utils.SeekableChannelsProvider;
import io.deephaven.util.codec.SimpleByteArrayCodec;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.IOException;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class ParquetReaderUtil {
    @FunctionalInterface
    public interface ColumnDefinitionConsumer {
        // objectWidth == -1 means not present.
        void accept(
                String name, Class<?> baseType, final String dbSpecialType,
                boolean isLegacyType, boolean isArray, boolean isGroupingColumn,
                String codecName, String codecArgs, String codecType, String codecComponentType);
    }

    public static final class ColDefHelper {
        String name;
        Class<?> baseType;
        String dbSpecialType;
        boolean isLegacyType;
        boolean isArray;
        boolean isGroupingColumn;
        String codecName;
        String codecArgs;
        String codecType;
        String codecComponentType;
        final ColumnDefinitionConsumer consumer;
        ColDefHelper(final ColumnDefinitionConsumer consumer) {
            this.consumer = consumer;
        }
        void reset() {
            name = null;
            baseType = null;
            dbSpecialType = null;
            isLegacyType = isArray = isGroupingColumn = false;
            codecName = codecArgs = codecType = codecComponentType = null;
        }
        void accept() {
            consumer.accept(
                    name, baseType, dbSpecialType,
                    isLegacyType, isArray, isGroupingColumn,
                    codecName, codecArgs, codecType, codecComponentType);
        }
    }

    /**
     * Obtain schema information from a parquet file
     *
     * @param filePath  Location for input parquet file
     * @param readInstructions  Parquet read instructions specifying transformations like column mappings and codecs.
     *                          Note a new read instructions based on this one may be returned by this method to provide necessary
     *                          transformations, eg, replacing unsupported characters like ' ' (space) in column names.
     * @param consumer  A ColumnDefinitionConsumer whose accept method would be called for each column in the file
     * @return Parquet read instructions, either the ones supplied or a new object based on the supplied with necessary
     *         transformations added.
     * @throws IOException if the specified file cannot be read
     */
    public static ParquetInstructions readParquetSchema(
            final String filePath,
            final ParquetInstructions readInstructions,
            final ColumnDefinitionConsumer consumer,
            final BiFunction<String, Set<String>, String> legalizeColumnNameFunc
    ) throws IOException {
        final ParquetFileReader pf = new ParquetFileReader(
                filePath, getChannelsProvider(), 0);
        final MessageType schema = pf.getSchema();
        final ParquetMetadata pm = new ParquetMetadataConverter().fromParquetMetadata(pf.fileMetaData);
        final Map<String, String> keyValueMetaData = pm.getFileMetaData().getKeyValueMetaData();
        final MutableObject<String> errorString = new MutableObject<>();
        final MutableObject<ColumnDescriptor> currentColumn = new MutableObject<>();
        final LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Class<?>> visitor = getVisitor(keyValueMetaData, errorString, currentColumn);
        final String csvGroupingCols = keyValueMetaData.get(ParquetTableWriter.GROUPING);
        Set<String> groupingCols = Collections.emptySet();
        if (csvGroupingCols != null && !csvGroupingCols.isEmpty()) {
            groupingCols = new HashSet<>(Arrays.asList(csvGroupingCols.split(",")));
        }

        ParquetInstructions.Builder instructionsBuilder = null;
        final ColDefHelper colDef = new ColDefHelper(consumer);
        for (ColumnDescriptor column : schema.getColumns()) {
            colDef.reset();
            currentColumn.setValue(column);
            final PrimitiveType primitiveType = column.getPrimitiveType();
            final LogicalTypeAnnotation logicalTypeAnnotation = primitiveType.getLogicalTypeAnnotation();
            final String parquetColumnName = column.getPath()[0];
            final String colName;
            final String mappedName = readInstructions.getColumnNameFromParquetColumnName(parquetColumnName);
            if (mappedName != null) {
                colName = mappedName;
            } else {
                final String legalized = legalizeColumnNameFunc.apply(
                        parquetColumnName,
                        (instructionsBuilder == null) ? Collections.emptySet() : instructionsBuilder.getTakenNames());
                if (!legalized.equals(parquetColumnName)) {
                    colName = legalized;
                    if (instructionsBuilder == null) {
                        instructionsBuilder = new ParquetInstructions.Builder(readInstructions);
                    }
                    instructionsBuilder.addColumnNameMapping(parquetColumnName, colName);
                } else {
                    colName = parquetColumnName;
                }
            }
            colDef.name = colName;
            colDef.dbSpecialType = keyValueMetaData.get(ParquetTableWriter.SPECIAL_TYPE_NAME_PREFIX + colName);
            colDef.isGroupingColumn = groupingCols.contains(colName);
            colDef.codecName = keyValueMetaData.get(ParquetTableWriter.CODEC_NAME_PREFIX + colName);
            colDef.codecArgs = keyValueMetaData.get(ParquetTableWriter.CODEC_ARGS_PREFIX + colName);
            colDef.codecType = keyValueMetaData.get(ParquetTableWriter.CODEC_DATA_TYPE_PREFIX + colName);
            colDef.isArray = column.getMaxRepetitionLevel() > 0;
            if (colDef.codecType != null && !colDef.codecType.isEmpty()) {
                colDef.codecComponentType = keyValueMetaData.get(ParquetTableWriter.CODEC_COMPONENT_TYPE_PREFIX + colName);
                colDef.accept();
                continue;
            }
            if (logicalTypeAnnotation == null) {
                colDef.isLegacyType = true;
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
                        final Supplier<String> exceptionTextSupplier = ()
                                -> "BINARY or FIXED_LEN_BYTE_ARRAY type " + column.getPrimitiveType()
                                    + " for column " + Arrays.toString(column.getPath());
                        if (colDef.dbSpecialType != null) {
                            if (colDef.dbSpecialType.equals(ParquetTableWriter.STRING_SET_SPECIAL_TYPE)) {
                                colDef.dbSpecialType = ParquetTableWriter.STRING_SET_SPECIAL_TYPE;
                            } else {
                                throw new UncheckedDeephavenException(exceptionTextSupplier.get()
                                        + " with unknown special type " + colDef.dbSpecialType);
                            }
                        }
                        if (colDef.codecName == null || colDef.codecName.isEmpty()) {
                            colDef.codecName = SimpleByteArrayCodec.class.getName();
                            colDef.codecArgs =  (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                ? Integer.toString(primitiveType.getTypeLength())
                                : null;
                        }
                        // fallthrough
                    default:
                        colDef.baseType = byte.class;
                        colDef.isArray = true;
                        break;
                }
            } else {
                colDef.baseType = logicalTypeAnnotation.accept(visitor).orElseThrow(() -> {
                    final String logicalTypeString = errorString.getValue();
                    return new UncheckedDeephavenException("Unable to read column " + Arrays.toString(column.getPath())
                            + ((logicalTypeString != null)
                            ? (logicalTypeString + " not supported")
                            : "no mappable logical type annotation found."));
                });
            }
            colDef.accept();
        }
        if (instructionsBuilder == null) {
            return readInstructions;
        }
        return instructionsBuilder.build();
    }

    private static LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Class<?>> getVisitor(
            final Map<String, String> keyValueMetaData,
            final MutableObject<String> errorString,
            final MutableObject<ColumnDescriptor> currentColumn) {
        return new LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<Class<?>>() {
            @Override
            public Optional<Class<?>> visit(final LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
                final ColumnDescriptor column = currentColumn.getValue();
                final String specialType = keyValueMetaData.get(ParquetTableWriter.SPECIAL_TYPE_NAME_PREFIX + column.getPath()[0]);
                if (specialType != null) {
                    if (specialType.equals(ParquetTableWriter.STRING_SET_SPECIAL_TYPE)) {
                        return Optional.of(StringSet.class);
                    }
                    throw new UncheckedDeephavenException("Type " + column.getPrimitiveType()
                            + " for column " + Arrays.toString(column.getPath())
                            + " with unknown or incompatible special type " + specialType);
                } else {
                    return Optional.of(String.class);
                }
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
                errorString.setValue("TimeLogicalType,isAdjustedToUTC=" + timeLogicalType.isAdjustedToUTC());
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
                errorString.setValue("TimestampLogicalType,isAdjustedToUTC=" + timestampLogicalType.isAdjustedToUTC() + ",unit=" + timestampLogicalType.getUnit());
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
                errorString.setValue("IntLogicalType,isSigned=" + intLogicalType.isSigned() + ",bitWidth=" + intLogicalType.getBitWidth());
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

    private static SeekableChannelsProvider getChannelsProvider() {
        return new CachedChannelProvider(new LocalFSChannelProvider(), 1024);
    }
}
