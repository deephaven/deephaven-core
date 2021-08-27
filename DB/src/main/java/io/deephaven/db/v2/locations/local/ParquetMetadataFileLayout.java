package io.deephaven.db.v2.locations.local;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
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
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

/**
 * <p>
 * {@link TableLocationKeyFinder Location finder} that will examine a parquet metadata file to discover locations.
 *
 * <p>
 * Note that we expect to find the following files:
 * <ul>
 * <li>{@code _metadata} - A file containing Parquet metadata for all {@link RowGroup row groups} in all
 * {@code .parquet} files for the entire data set, including schema information non-partitioning columns and key-value
 * metadata</li>
 * <li>{@code _common_metadata} <i>(optional)</i> - A file containing Parquet metadata with schema information that
 * applies to the entire data set, including partitioning columns that are inferred from file paths rather than
 * explicitly written in {@link org.apache.parquet.format.ColumnChunk column chunks} within {@code .parquet} files</li>
 * </ul>
 */
public class ParquetMetadataFileLayout implements TableLocationKeyFinder<ParquetTableLocationKey> {

    public static final String METADATA_FILE_NAME = "_metadata";
    public static final String COMMON_METADATA_FILE_NAME = "_common_metadata";

    private final File metadataFile;
    private final File commonMetadataFile;

    private final TableDefinition definition;
    private final ParquetInstructions instructions;
    private final List<ParquetTableLocationKey> keys;

    public ParquetMetadataFileLayout(@NotNull final File directory) {
        this(directory, ParquetInstructions.EMPTY);
    }

    public ParquetMetadataFileLayout(@NotNull final File directory,
            @NotNull final ParquetInstructions inputInstructions) {
        this(new File(directory, METADATA_FILE_NAME), new File(directory, COMMON_METADATA_FILE_NAME),
                inputInstructions);
    }

    public ParquetMetadataFileLayout(@NotNull final File metadataFile,
            @Nullable final File commonMetadataFile) {
        this(metadataFile, commonMetadataFile, ParquetInstructions.EMPTY);
    }

    public ParquetMetadataFileLayout(@NotNull final File metadataFile,
            @Nullable final File commonMetadataFile,
            @NotNull final ParquetInstructions inputInstructions) {
        this.metadataFile = metadataFile;
        this.commonMetadataFile = commonMetadataFile;
        if (!metadataFile.exists()) {
            throw new TableDataException("Parquet metadata file " + metadataFile + " does not exist");
        }
        final ParquetFileReader metadataFileReader = ParquetTools.getParquetFileReader(metadataFile);

        final ParquetMetadataConverter converter = new ParquetMetadataConverter();
        final ParquetMetadata metadataFileMetadata = convertMetadata(metadataFile, metadataFileReader, converter);
        final Pair<List<ColumnDefinition<?>>, ParquetInstructions> leafSchemaInfo = ParquetTools.convertSchema(
                metadataFileReader.getSchema(),
                metadataFileMetadata.getFileMetaData().getKeyValueMetaData(),
                inputInstructions);

        if (commonMetadataFile != null && commonMetadataFile.exists()) {
            final ParquetFileReader commonMetadataFileReader = ParquetTools.getParquetFileReader(commonMetadataFile);
            final Pair<List<ColumnDefinition<?>>, ParquetInstructions> fullSchemaInfo = ParquetTools.convertSchema(
                    commonMetadataFileReader.getSchema(),
                    convertMetadata(commonMetadataFile, commonMetadataFileReader, converter).getFileMetaData()
                            .getKeyValueMetaData(),
                    leafSchemaInfo.getSecond());
            final List<ColumnDefinition<?>> adjustedColumnDefinitions = new ArrayList<>();
            final Map<String, ColumnDefinition<?>> leafDefinitionsMap =
                    leafSchemaInfo.getFirst().stream().collect(toMap(ColumnDefinition::getName, Function.identity()));
            for (final ColumnDefinition<?> fullDefinition : fullSchemaInfo.getFirst()) {
                final ColumnDefinition<?> leafDefinition = leafDefinitionsMap.get(fullDefinition.getName());
                if (leafDefinition == null) {
                    adjustedColumnDefinitions.add(adjustPartitionDefinition(fullDefinition));
                } else if (fullDefinition.equals(leafDefinition)) {
                    adjustedColumnDefinitions.add(fullDefinition); // No adjustments to apply in this case
                } else {
                    final List<String> differences = new ArrayList<>();
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

        final List<ColumnDefinition<?>> partitioningColumns = definition.getPartitioningColumns();
        final Map<String, ColumnDefinition<?>> partitioningColumnsMap = partitioningColumns.stream().collect(
                toMap(ColumnDefinition::getName, Function.identity(), Assert::neverInvoked, LinkedHashMap::new));
        final Map<String, TIntList> fileNameToRowGroupIndices = new LinkedHashMap<>();
        final List<RowGroup> rowGroups = metadataFileReader.fileMetaData.getRow_groups();
        final int numRowGroups = rowGroups.size();
        for (int rgi = 0; rgi < numRowGroups; ++rgi) {
            fileNameToRowGroupIndices
                    .computeIfAbsent(rowGroups.get(rgi).getColumns().get(0).getFile_path(), fn -> new TIntArrayList())
                    .add(rgi);
        }
        final File directory = metadataFile.getParentFile();
        final MutableInt partitionOrder = new MutableInt(0);
        keys = fileNameToRowGroupIndices.entrySet().stream().map(entry -> {
            final String filePathString = entry.getKey();
            final int[] rowGroupIndices = entry.getValue().toArray();

            if (filePathString == null || filePathString.isEmpty()) {
                throw new TableDataException("Missing parquet file name for row groups "
                        + Arrays.toString(rowGroupIndices) + " in " + metadataFile);
            }
            final LinkedHashMap<String, Comparable<?>> partitions =
                    partitioningColumns.isEmpty() ? null : new LinkedHashMap<>();
            if (partitions != null) {
                final Path filePath = Paths.get(filePathString);
                final int numPartitions = filePath.getNameCount() - 1;
                if (numPartitions != partitioningColumns.size()) {
                    throw new TableDataException("Unexpected number of path elements in " + filePathString
                            + " for partitions " + partitions.keySet());
                }
                final boolean useHiveStyle = filePath.getName(0).toString().contains("=");
                for (int pi = 0; pi < numPartitions; ++pi) {
                    final String pathElement = filePath.getName(pi).toString();
                    final ColumnDefinition<?> columnDefinition;
                    final String partitionKey;
                    final String partitionValueRaw;
                    if (useHiveStyle) {
                        final String[] pathComponents = pathElement.split("=", 2);
                        if (pathComponents.length != 2) {
                            throw new TableDataException(
                                    "Unexpected path format found for hive-style partitioning from " + filePathString
                                            + " for " + metadataFile);
                        }
                        partitionKey = instructions.getColumnNameFromParquetColumnNameOrDefault(pathComponents[0]);
                        columnDefinition = partitioningColumnsMap.get(partitionKey);
                        partitionValueRaw = pathComponents[1];
                    } else {
                        columnDefinition = partitioningColumns.get(pi);
                        partitionKey = columnDefinition.getName();
                        partitionValueRaw = pathElement;
                    }
                    final Comparable<?> partitionValue =
                            CONVERSION_FUNCTIONS.get(columnDefinition.getDataType()).apply(partitionValueRaw);
                    if (partitions.containsKey(partitionKey)) {
                        throw new TableDataException("Unexpected duplicate partition key " + partitionKey
                                + " when parsing " + filePathString + " for " + metadataFile);
                    }
                    partitions.put(partitionKey, partitionValue);
                }
            }
            final ParquetTableLocationKey tlk = new ParquetTableLocationKey(new File(directory, filePathString),
                    partitionOrder.getAndIncrement(), partitions);
            tlk.setFileReader(metadataFileReader);
            tlk.setMetadata(metadataFileMetadata);
            tlk.setRowGroupIndices(rowGroupIndices);
            return tlk;
        }).collect(Collectors.toList());
    }

    public String toString() {
        return ParquetMetadataFileLayout.class.getSimpleName() + '[' + metadataFile + ',' + commonMetadataFile + ']';
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

    private static ColumnDefinition<?> adjustPartitionDefinition(@NotNull final ColumnDefinition<?> columnDefinition) {
        if (columnDefinition.getComponentType() != null) {
            return ColumnDefinition.fromGenericType(columnDefinition.getName(), String.class,
                    ColumnDefinition.COLUMNTYPE_PARTITIONING, null);
        }
        final Class<?> dataType = columnDefinition.getDataType();
        if (dataType == boolean.class) {
            return ColumnDefinition.fromGenericType(columnDefinition.getName(), Boolean.class,
                    ColumnDefinition.COLUMNTYPE_PARTITIONING, null);
        }
        if (dataType.isPrimitive()) {
            return columnDefinition.withPartitioning();
        }
        final Class<?> unboxedType = TypeUtils.getUnboxedType(dataType);
        if (unboxedType != null && unboxedType.isPrimitive()) {
            return ColumnDefinition.fromGenericType(columnDefinition.getName(), unboxedType,
                    ColumnDefinition.COLUMNTYPE_PARTITIONING, null);
        }
        if (dataType == Boolean.class || dataType == String.class || dataType == BigDecimal.class
                || dataType == BigInteger.class) {
            return columnDefinition.withPartitioning();
        }
        // NB: This fallback includes any kind of timestamp; we don't have a strong grasp of required parsing support at
        // this time, and preserving the contents as a String allows the user full control.
        return ColumnDefinition.fromGenericType(columnDefinition.getName(), String.class,
                ColumnDefinition.COLUMNTYPE_PARTITIONING, null);
    }

    private static final Map<Class<?>, Function<String, Comparable<?>>> CONVERSION_FUNCTIONS;

    static {
        final Map<Class<?>, Function<String, Comparable<?>>> conversionFunctionsTemp = new HashMap<>();
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
