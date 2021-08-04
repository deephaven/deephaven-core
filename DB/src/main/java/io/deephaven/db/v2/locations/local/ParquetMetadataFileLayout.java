package io.deephaven.db.v2.locations.local;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.impl.TableLocationKeyFinder;
import io.deephaven.db.v2.locations.parquet.local.ParquetTableLocationKey;
import io.deephaven.db.v2.parquet.ParquetInstructions;
import io.deephaven.parquet.ParquetFileReader;
import io.deephaven.parquet.tempfix.ParquetMetadataConverter;
import io.deephaven.util.type.TypeUtils;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

/**
 * {@link TableLocationKeyFinder Location finder} that will examine a parquet metadata file to discover locations.
 */
public class ParquetMetadataFileLayout implements TableLocationKeyFinder<ParquetTableLocationKey> {

    private static final String METADATA_FILE_NAME = "_metadata";
    private static final String COMMON_METADATA_FILE_NAME = "_common_metadata";

    private final TableDefinition definition;
    private final ParquetInstructions instructions;
    private final List<ParquetTableLocationKey> keys;

    public ParquetMetadataFileLayout(@NotNull final File directory,
                                     @NotNull final ParquetInstructions inputInstructions) {
        final File metadataFile = new File(directory, METADATA_FILE_NAME);
        if (!metadataFile.exists()) {
            throw new TableDataException("Parquet metadata file " + metadataFile + " does not exist");
        }
        final ParquetFileReader metadataFileReader = ParquetTools.getParquetFileReader(metadataFile);

        final ParquetMetadataConverter converter = new ParquetMetadataConverter();
        final ParquetMetadata metadataFileMetadata = convertMetadata(metadataFile, metadataFileReader, converter);
        final Pair<List<ColumnDefinition>, ParquetInstructions> leafSchemaInfo = ParquetTools.convertSchema(metadataFileMetadata, inputInstructions);

        final File commonMetadataFile = new File(directory, COMMON_METADATA_FILE_NAME);
        if (commonMetadataFile.exists()) {
            final Pair<List<ColumnDefinition>, ParquetInstructions> fullSchemaInfo = ParquetTools.convertSchema(
                    convertMetadata(commonMetadataFile, ParquetTools.getParquetFileReader(commonMetadataFile), converter),
                    leafSchemaInfo.getSecond());
            final List<ColumnDefinition> adjustedColumnDefinitions = new ArrayList<>();
            final Map<String, ColumnDefinition> leafDefinitionsMap = leafSchemaInfo.getFirst().stream().collect(toMap(ColumnDefinition::getName, Function.identity()));
            for (final ColumnDefinition fullDefinition : fullSchemaInfo.getFirst()) {
                final ColumnDefinition leafDefinition = leafDefinitionsMap.get(fullDefinition.getName());
                if (leafDefinition == null) {
                    adjustedColumnDefinitions.add(adjustPartitionDefinition(fullDefinition));
                } else if (!fullDefinition.equals(leafDefinition)) {
                    final List<String> differences = new ArrayList<>();
                    //noinspection unchecked
                    fullDefinition.describeDifferences(differences, leafDefinition, "full schema", "file schema", "");
                    throw new TableDataException(String.format("Schema mismatch between %s and %s for column %s: %s",
                            metadataFile, commonMetadataFile, fullDefinition.getName(), differences));
                }
            }
            definition = new TableDefinition(adjustedColumnDefinitions);
            instructions = fullSchemaInfo.getSecond();
        } else {
            definition = new TableDefinition(leafSchemaInfo.getFirst());
            instructions = leafSchemaInfo.getSecond();
        }

        final List<ColumnDefinition> partitioningColumns = definition.getPartitioningColumns();
        final Map<String, ColumnDefinition> partitioningColumnsMap = partitioningColumns.stream().collect(
                toMap(ColumnDefinition::getName, Function.identity(), Assert::neverInvoked, LinkedHashMap::new));
        final Map<String, List<RowGroup>> fileNameToRowGroups =
                metadataFileReader.fileMetaData.getRow_groups().stream().collect(groupingBy(rg -> rg.getColumns().get(0).getFile_path()));
        keys = fileNameToRowGroups.entrySet().stream().map(entry -> {
            final String fileName = entry.getKey();
            final int[] rowGroupOrdinals = entry.getValue().stream().mapToInt(RowGroup::getOrdinal).toArray();

            if (fileName == null || fileName.isEmpty()) {
                throw new TableDataException("Missing parquet file name for row groups " + Arrays.toString(rowGroupOrdinals) + " in " + metadataFile);
            }
            final LinkedHashMap<String, Comparable<?>> partitions = partitioningColumns.isEmpty() ? null : new LinkedHashMap<>();
            if (partitions != null) {
                final String[] paths = fileName.split(File.pathSeparator);
                final int numPartitions = paths.length - 1;
                if (numPartitions != partitioningColumns.size()) {
                    throw new TableDataException("Unexpected number of path elements in " + fileName + " for partitions " + partitions.keySet());
                }
                final boolean useHiveStyle = paths[0].contains("=");
                for (int pi = 0; pi < numPartitions; ++pi) {
                    final String path = paths[pi];
                    final ColumnDefinition columnDefinition;
                    final String partitionKey;
                    final String partitionValueRaw;
                    if (useHiveStyle) {
                        final String[] pathComponents = path.split("=");
                        if (pathComponents.length != 2) {
                            throw new TableDataException("Unexpected path format found for hive-style partitioning from " + fileName + " for " + metadataFile);
                        }
                        partitionKey = instructions.getColumnNameFromParquetColumnName(pathComponents[0]);
                        columnDefinition = partitioningColumnsMap.get(partitionKey);
                        partitionValueRaw = pathComponents[1];
                    } else {
                        columnDefinition = partitioningColumns.get(pi);
                        partitionKey = columnDefinition.getName();
                        partitionValueRaw = path;
                    }
                    final Comparable<?> partitionValue = CONVERSION_FUNCTIONS.get(columnDefinition.getDataType()).apply(partitionValueRaw);
                    if (partitions.containsKey(partitionKey)) {
                        throw new TableDataException("Unexpected duplicate partition key " + partitionKey + " when parsing " + fileName + " for " + metadataFile);
                    }
                    partitions.put(partitionKey, partitionValue);
                }
            }
            final ParquetTableLocationKey tlk = new ParquetTableLocationKey(new File(directory, fileName), partitions);
            tlk.setFileReader(metadataFileReader);
            tlk.setMetadata(metadataFileMetadata);
            tlk.setRowGroupOrdinals(rowGroupOrdinals);
            return tlk;
        }).collect(Collectors.toList());
    }

    public ParquetMetadataFileLayout(@NotNull final File directory) {
        this(directory, ParquetInstructions.EMPTY);
    }

    private static ParquetMetadata convertMetadata(@NotNull final File file,
                                                   @NotNull final ParquetFileReader fileReader,
                                                   @NotNull final ParquetMetadataConverter converter) {
        try {
            return converter.fromParquetMetadata(fileReader.fileMetaData);
        } catch (IOException e) {
            throw new TableDataException("Error while converting file metadata from " + file);
        }
    }

    private static ColumnDefinition adjustPartitionDefinition(@NotNull final ColumnDefinition columnDefinition) {
        if (columnDefinition.getComponentType() != null) {
            return ColumnDefinition.fromGenericType(columnDefinition.getName(), String.class, ColumnDefinition.COLUMNTYPE_PARTITIONING, null);
        }
        final Class<?> dataType = columnDefinition.getDataType();
        if (dataType == boolean.class) {
            return ColumnDefinition.fromGenericType(columnDefinition.getName(), Boolean.class, ColumnDefinition.COLUMNTYPE_PARTITIONING, null);
        }
        if (dataType.isPrimitive()) {
            return columnDefinition.withPartitioning();
        }
        final Class<?> unboxedType = TypeUtils.getUnboxedType(dataType);
        if (unboxedType.isPrimitive()) {
            return ColumnDefinition.fromGenericType(columnDefinition.getName(), unboxedType, ColumnDefinition.COLUMNTYPE_PARTITIONING, null);
        }
        if (dataType == Boolean.class || dataType == String.class || dataType == BigDecimal.class || dataType == BigInteger.class) {
            return columnDefinition.withPartitioning();
        }
        // NB: This fallback includes any kind of timestamp; we don't have a strong grasp of required parsing support at
        //     this time, and preserving the contents as a String allows the user full control.
        return ColumnDefinition.fromGenericType(columnDefinition.getName(), String.class, ColumnDefinition.COLUMNTYPE_PARTITIONING, null);
    }

    private static final Map<Class, Function<String, Comparable<?>>> CONVERSION_FUNCTIONS;

    static {
        final Map<Class, Function<String, Comparable<?>>> conversionFunctionsTemp = new HashMap<>();
        conversionFunctionsTemp.put(Boolean.class, Boolean::parseBoolean);
        conversionFunctionsTemp.put(char.class, str -> {
            Require.eq(str.length(), "length", 1);
            return str.charAt(0);
        });
        conversionFunctionsTemp.put(byte.class, Byte::parseByte);
        conversionFunctionsTemp.put(short.class, Short::parseShort);
        conversionFunctionsTemp.put(int.class, Integer::parseInt);
        conversionFunctionsTemp.put(long.class, Long::parseLong);
        conversionFunctionsTemp.put(float.class, Float::parseFloat);
        conversionFunctionsTemp.put(BigInteger.class, BigInteger::new);
        conversionFunctionsTemp.put(BigDecimal.class, BigDecimal::new);
        conversionFunctionsTemp.put(String.class, str -> str);
        CONVERSION_FUNCTIONS = Collections.unmodifiableMap(conversionFunctionsTemp);
    }

    public TableDefinition getTableDefinition() {
        return definition;
    }

    public ParquetInstructions getInstructions() {
        return instructions;
    }

    @Override
    public void findKeys(@NotNull final Consumer<ParquetTableLocationKey> locationKeyObserver) {
        keys.forEach(locationKeyObserver);
    }
}
