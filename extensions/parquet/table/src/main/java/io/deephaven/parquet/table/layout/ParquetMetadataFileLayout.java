//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.layout;

import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import io.deephaven.base.Pair;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.util.PartitionParser;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.impl.TableLocationKeyFinder;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.base.ParquetFileReader;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import io.deephaven.util.type.TypeUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

    public ParquetMetadataFileLayout(
            @NotNull final File directory,
            @NotNull final ParquetInstructions inputInstructions) {
        this(new File(directory, METADATA_FILE_NAME), new File(directory, COMMON_METADATA_FILE_NAME),
                inputInstructions);
    }

    public ParquetMetadataFileLayout(
            @NotNull final File metadataFile,
            @Nullable final File commonMetadataFile) {
        this(metadataFile, commonMetadataFile, ParquetInstructions.EMPTY);
    }

    public ParquetMetadataFileLayout(
            @NotNull final File metadataFile,
            @Nullable final File commonMetadataFile,
            @NotNull final ParquetInstructions inputInstructions) {
        if (inputInstructions.isRefreshing()) {
            throw new IllegalArgumentException("ParquetMetadataFileLayout does not support refreshing");
        }
        this.metadataFile = metadataFile;
        this.commonMetadataFile = commonMetadataFile;
        if (!metadataFile.exists()) {
            throw new TableDataException(String.format("Parquet metadata file %s does not exist", metadataFile));
        }
        final ParquetFileReader metadataFileReader = ParquetTools.getParquetFileReader(metadataFile, inputInstructions);

        final ParquetMetadataConverter converter = new ParquetMetadataConverter();
        final ParquetMetadata metadataFileMetadata = convertMetadata(metadataFile, metadataFileReader, converter);
        final Pair<List<ColumnDefinition<?>>, ParquetInstructions> leafSchemaInfo = ParquetTools.convertSchema(
                metadataFileReader.getSchema(),
                metadataFileMetadata.getFileMetaData().getKeyValueMetaData(),
                inputInstructions);

        if (commonMetadataFile != null && commonMetadataFile.exists()) {
            final ParquetFileReader commonMetadataFileReader =
                    ParquetTools.getParquetFileReader(commonMetadataFile, inputInstructions);
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
                    fullDefinition.describeDifferences(differences, leafDefinition, "full schema", "file schema",
                            "", false);
                    throw new TableDataException(String.format("Schema mismatch between %s and %s for column %s: %s",
                            metadataFile, commonMetadataFile, fullDefinition.getName(), differences));
                }
            }
            definition = TableDefinition.of(adjustedColumnDefinitions);
            instructions = fullSchemaInfo.getSecond();
        } else {
            definition = TableDefinition.of(leafSchemaInfo.getFirst());
            instructions = leafSchemaInfo.getSecond();
        }

        final List<ColumnDefinition<?>> partitioningColumns = definition.getPartitioningColumns();
        final Map<String, PartitionParser> partitionKeyToParser = partitioningColumns.stream().collect(toMap(
                ColumnDefinition::getName,
                cd -> PartitionParser.lookupSupported(cd.getDataType(), cd.getComponentType())));
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
                throw new TableDataException(String.format(
                        "Missing parquet file name for row groups %s in %s",
                        Arrays.toString(rowGroupIndices), metadataFile));
            }
            final LinkedHashMap<String, Comparable<?>> partitions =
                    partitioningColumns.isEmpty() ? null : new LinkedHashMap<>();
            if (partitions != null) {
                final Path filePath = Paths.get(filePathString);
                final int numPartitions = filePath.getNameCount() - 1;
                if (numPartitions != partitioningColumns.size()) {
                    throw new TableDataException(String.format(
                            "Unexpected number of path elements in %s for partitions %s",
                            filePathString, partitions.keySet()));
                }
                final boolean useHiveStyle = filePath.getName(0).toString().contains("=");
                for (int pi = 0; pi < numPartitions; ++pi) {
                    final String pathElement = filePath.getName(pi).toString();
                    final String partitionKey;
                    final String partitionValueRaw;
                    if (useHiveStyle) {
                        final String[] pathComponents = pathElement.split("=", 2);
                        if (pathComponents.length != 2) {
                            throw new TableDataException(String.format(
                                    "Unexpected path format found for hive-style partitioning from %s for %s",
                                    filePathString, metadataFile));
                        }
                        partitionKey = instructions.getColumnNameFromParquetColumnNameOrDefault(pathComponents[0]);
                        partitionValueRaw = pathComponents[1];
                    } else {
                        partitionKey = partitioningColumns.get(pi).getName();
                        partitionValueRaw = pathElement;
                    }
                    final Comparable<?> partitionValue =
                            partitionKeyToParser.get(partitionKey).parse(partitionValueRaw);
                    if (partitions.containsKey(partitionKey)) {
                        throw new TableDataException(String.format(
                                "Unexpected duplicate partition key %s when parsing %s for %s",
                                partitionKey, filePathString, metadataFile));
                    }
                    partitions.put(partitionKey, partitionValue);
                }
            }
            final ParquetTableLocationKey tlk = new ParquetTableLocationKey(new File(directory, filePathString),
                    partitionOrder.getAndIncrement(), partitions, inputInstructions);
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
        // Primitive booleans should be boxed
        final Class<?> dataType = columnDefinition.getDataType();
        if (dataType == boolean.class) {
            return ColumnDefinition.fromGenericType(
                    columnDefinition.getName(), Boolean.class, null, ColumnDefinition.ColumnType.Partitioning);
        }

        // Non-boolean primitives and boxed Booleans are supported as-is
        if (dataType.isPrimitive() || dataType == Boolean.class) {
            return columnDefinition.withPartitioning();
        }

        // Non-boolean boxed primitives should be unboxed
        final Class<?> unboxedType = TypeUtils.getUnboxedTypeIfBoxed(dataType);
        if (unboxedType != dataType) {
            return ColumnDefinition.fromGenericType(
                    columnDefinition.getName(), unboxedType, null, ColumnDefinition.ColumnType.Partitioning);
        }

        // Object types we know how to parse are supported as-is
        if (PartitionParser.lookup(dataType, columnDefinition.getComponentType()) != null) {
            return columnDefinition.withPartitioning();
        }

        // Fall back to String for all other types
        return ColumnDefinition.fromGenericType(
                columnDefinition.getName(), String.class, null, ColumnDefinition.ColumnType.Partitioning);
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
