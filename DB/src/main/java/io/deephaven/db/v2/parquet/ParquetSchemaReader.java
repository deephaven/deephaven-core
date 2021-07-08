package io.deephaven.db.v2.parquet;

import io.deephaven.UncheckedDeephavenException;
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

public class ParquetSchemaReader {
    @FunctionalInterface
    public interface ColumnDefinitionConsumer {
        void accept(final ColumnDefinition def);
    }

    public static final class ColumnDefinition {
        // Yes you guessed right.  This is the column name.
        public String name;
        // The parquet type.
        public Class<?> baseType;
        // Some types require special annotations to support regular parquet tools and efficient DH handling.
        // Examples are StringSet and DbArray; a parquet file with a DbIntArray special type metadata annotation,
        // but storing types as repeated int, can be loaded both by other parquet tools and efficiently by DH.
        public String dhSpecialType;
        // Parquet 1.0 did not support logical types; if we encounter types like this this is true.
        // For example. in parquet 1.0 binary columns with no annotation are used to represent strings.
        // They are also used to represent other things that are not strings.  Good luck, may the force be with you.
        public boolean isLegacyType;
        // Your guess is good here.
        public boolean isArray;
        // Your guess is good here.
        public boolean isGrouping;
        // When codec metadata is present (which will be returned as modified read instructions below for actual codec
        // name and args), we expect codec type and component type to be present.  When they are present,
        // codecType and codecComponentType take precedence over any other DH type deducing heuristics (and
        // thus baseType in this structure can be ignored).
        public String codecType;
        public String codecComponentType;
        // We reuse the guts of this poor object between calls to avoid allocating.
        // Like prometheus nailed to a mountain, this poor object has to suffer his guts being eaten forever.
        // Or not forever but at least for one stack frame activation of readParquetSchema and as many columns
        // that function finds in the file.
        void reset() {
            name = null;
            baseType = null;
            dhSpecialType = null;
            isLegacyType = isArray = isGrouping = false;
            codecType = codecComponentType = null;
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
        final ColumnDefinition colDef = new ColumnDefinition();
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
            colDef.dhSpecialType = keyValueMetaData.get(ParquetTableWriter.SPECIAL_TYPE_NAME_PREFIX + colName);
            colDef.isGrouping = groupingCols.contains(colName);
            String codecName = keyValueMetaData.get(ParquetTableWriter.CODEC_NAME_PREFIX + colName);
            String codecArgs = keyValueMetaData.get(ParquetTableWriter.CODEC_ARGS_PREFIX + colName);
            colDef.codecType = keyValueMetaData.get(ParquetTableWriter.CODEC_DATA_TYPE_PREFIX + colName);
            if (codecName != null && !codecName.isEmpty()) {
                if (instructionsBuilder == null) {
                    instructionsBuilder = new ParquetInstructions.Builder();
                }
                instructionsBuilder.addColumnCodec(colName, codecName, codecArgs);
            }
            colDef.isArray = column.getMaxRepetitionLevel() > 0;
            if (colDef.codecType != null && !colDef.codecType.isEmpty()) {
                colDef.codecComponentType = keyValueMetaData.get(ParquetTableWriter.CODEC_COMPONENT_TYPE_PREFIX + colName);
                consumer.accept(colDef);
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
                        if (colDef.dhSpecialType != null) {
                            if (colDef.dhSpecialType.equals(ParquetTableWriter.STRING_SET_SPECIAL_TYPE)) {
                                colDef.dhSpecialType = ParquetTableWriter.STRING_SET_SPECIAL_TYPE;
                            } else {
                                throw new UncheckedDeephavenException(
                                        "BINARY or FIXED_LEN_BYTE_ARRAY type " + column.getPrimitiveType()
                                            + " for column " + Arrays.toString(column.getPath())
                                            + " with unknown special type " + colDef.dhSpecialType);
                            }
                        }
                        if (codecName == null || codecName.isEmpty()) {
                            if (instructionsBuilder == null) {
                                instructionsBuilder = new ParquetInstructions.Builder();
                            }
                            codecName = SimpleByteArrayCodec.class.getName();
                            codecArgs = (typeName == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                                    ? Integer.toString(primitiveType.getTypeLength())
                                    : null;
                            instructionsBuilder.addColumnCodec(colName, codecName, codecArgs);
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
                    String msg = "Unable to read column " + Arrays.toString(column.getPath()) + ": ";
                    msg += (logicalTypeString != null)
                            ? (logicalTypeString + "not supported")
                            : "no mappable logical type annotation found"
                        ;
                    return new UncheckedDeephavenException(msg);
                });
            }
            consumer.accept(colDef);
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
                    if (!specialType.equals(ParquetTableWriter.DBARRAY_SPECIAL_TYPE)) {
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
