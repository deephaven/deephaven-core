//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.util.NameValidator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.parquet.impl.ParquetSchemaUtil;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.metadata.SortColumnInfo;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.deephaven.parquet.table.ParquetTableWriter.GROUPING_BEGIN_POS_COLUMN_NAME;
import static io.deephaven.parquet.table.ParquetTableWriter.GROUPING_END_POS_COLUMN_NAME;
import static io.deephaven.parquet.table.ParquetTableWriter.GROUPING_KEY_COLUMN_NAME;
import static io.deephaven.parquet.table.ParquetTableWriter.INDEX_ROW_SET_COLUMN_NAME;

/**
 * The one column resolver for the file of a {@link ParquetTableLocation}. It finds each Deephaven column with the
 * instructions' column resolver, if the instructions provide one, or else by their renames, whose default is the
 * identity. A column resolver is the alternative to renames; with one, the renames are ignored.
 * <p>
 * It also translates the file's metadata, which names parquet columns, to the Deephaven columns that read them. A
 * column name that is not renamed has only one possible reader, the Deephaven column with that name, so translating it
 * is a check of that column alone; with no renames and no column resolver, the common case, that is every name.
 * Otherwise the reverse direction is built from the forward one, on first use, since building it resolves every
 * Deephaven column.
 */
final class ParquetTableLocationColumnResolver implements ParquetColumnResolver {

    /**
     * Returns a column resolver for a legacy grouping table with the parquet schema {@code groupingSchema}, whose key
     * and start and end positions have fixed names. A column with more than one level, such as a key column of an array
     * or vector type, is at the path to its leaf.
     */
    @NotNull
    static ParquetColumnResolver forGrouping(@NotNull final MessageType groupingSchema) {
        final Map<String, List<String>> leafPathsByParquetColumnName = new HashMap<>();
        for (final String[] leafPath : ParquetSchemaUtil.paths(groupingSchema)) {
            leafPathsByParquetColumnName.putIfAbsent(leafPath[0], List.of(leafPath));
        }
        final ParquetColumnResolverMap.Builder builder = ParquetColumnResolverMap.builder();
        for (final String columnName : List.of(
                GROUPING_KEY_COLUMN_NAME, GROUPING_BEGIN_POS_COLUMN_NAME, GROUPING_END_POS_COLUMN_NAME)) {
            builder.putMap(columnName, leafPathsByParquetColumnName.getOrDefault(columnName, List.of(columnName)));
        }
        return builder.build();
    }

    private final ParquetInstructions readInstructions;
    private final MessageType schema;
    /** The path to the leaf of each top-level column of the file that has more than one level. */
    private final Map<String, String[]> pathsByParquetColumnName;
    /** The instructions' column resolver, or {@code null} to follow their renames. */
    @Nullable
    private final ParquetColumnResolver instructionsResolver;
    /** The location's partitioning columns, which the file does not have. */
    private final Set<String> partitionKeys;

    /** See {@link #metadataNames()}. */
    private volatile MetadataNames metadataNames;

    /**
     * @param readInstructions The instructions for reading the file
     * @param schema The file's parquet schema
     * @param pathsByParquetColumnName The path to the leaf of each top-level column of the file that has more than one
     *        level
     * @param instructionsResolver The instructions' column resolver for the file, or {@code null} if they have none
     * @param partitionKeys The location's partitioning columns
     */
    ParquetTableLocationColumnResolver(
            @NotNull final ParquetInstructions readInstructions,
            @NotNull final MessageType schema,
            @NotNull final Map<String, String[]> pathsByParquetColumnName,
            @Nullable final ParquetColumnResolver instructionsResolver,
            @NotNull final Set<String> partitionKeys) {
        this.readInstructions = readInstructions;
        this.schema = schema;
        this.pathsByParquetColumnName = pathsByParquetColumnName;
        this.instructionsResolver = instructionsResolver;
        this.partitionKeys = partitionKeys;
    }

    @Override
    public Optional<List<String>> of(@NotNull final String columnName) {
        if (instructionsResolver != null) {
            return instructionsResolver.of(columnName);
        }
        final String parquetColumnName = readInstructions.getParquetColumnNameFromColumnNameOrDefault(columnName);
        final String[] columnPath = pathsByParquetColumnName.get(parquetColumnName);
        return Optional.of(columnPath == null ? List.of(parquetColumnName) : List.of(columnPath));
    }

    /**
     * Returns the Deephaven column that reads the top-level parquet column {@code parquetColumnName} whole, or
     * {@code null} if none can be identified.
     */
    @Nullable
    String findDeephavenColumnName(@NotNull final String parquetColumnName) {
        if (renamesLeaveAlone(parquetColumnName)) {
            // The only possible reader is the Deephaven column with the same name, which cannot exist when that name is
            // not a valid Deephaven column name; with no rename to read it, such a parquet column has no reader
            final String columnName = parquetColumnName;
            return NameValidator.isValidColumnName(columnName)
                    && readsFile(columnName)
                    && readsWholeColumn(of(columnName).orElse(List.of())) ? columnName : null;
        }
        return metadataNames().deephavenByParquet.get(parquetColumnName);
    }

    /**
     * Translates the file's sort columns, which name parquet columns, to sort columns that name the Deephaven columns
     * reading them. A sort is only meaningful as a prefix -- each column is sorted within runs of the columns before it
     * -- so translation stops at the first column that cannot be identified.
     * <p>
     * The sortedness itself is trusted rather than verified, as
     * {@link io.deephaven.engine.table.impl.SortedColumnsAttribute} documents. What this guards is the identity of the
     * column. Files written before the writer recorded parquet column names recorded the Deephaven name at write time
     * instead; the two agree unless the sorted column was renamed on write, in which case the name is normally absent
     * from the file and the sort is dropped. The one case that cannot be detected is a write that swapped two column
     * names, so that the recorded name belongs to the other column.
     */
    @NotNull
    List<SortColumn> translateToDeephavenSortColumns(@NotNull final List<SortColumnInfo> parquetSortColumns) {
        final List<SortColumn> sortColumns = new ArrayList<>(parquetSortColumns.size());
        for (final SortColumnInfo parquetSortColumn : parquetSortColumns) {
            final String columnName = findDeephavenColumnName(parquetSortColumn.columnName());
            if (columnName == null) {
                break;
            }
            sortColumns.add(parquetSortColumn.sortDirection() == SortColumnInfo.SortDirection.Ascending
                    ? SortColumn.asc(ColumnName.of(columnName))
                    : SortColumn.desc(ColumnName.of(columnName)));
        }
        return Collections.unmodifiableList(sortColumns);
    }

    /**
     * Returns a column resolver for the table of a data index on the Deephaven columns {@code keyColumnNames}, whose
     * file has the parquet schema {@code indexSchema}. A data index table is written with the parquet column names of
     * the file it indexes, so each key column is at the path where this resolver finds the column it indexes, or, for a
     * partitioning column, which this file does not have, at the top-level column the metadata records it under.
     * Returns {@code null} if any key column cannot be translated.
     * <p>
     * The row set column is written as {@code dh_row_set}, unless a key column is written under that name; see
     * {@code ParquetTableWriter}. Earlier versions of the writer also applied the indexed table's renames to the row
     * set column: when the instructions renamed a column of the indexed table named {@code dh_row_set}, the row set
     * column was written under that column's parquet name. Under any other name, the row set column is the only
     * top-level column that is not a key column.
     */
    @Nullable
    ParquetColumnResolver forDataIndex(
            @NotNull final MessageType indexSchema,
            @NotNull final String... keyColumnNames) {
        final String[] parquetKeyColumnNames = findIndexParquetColumnNames(Arrays.asList(keyColumnNames));
        if (parquetKeyColumnNames == null) {
            return null;
        }
        final ParquetColumnResolverMap.Builder builder = ParquetColumnResolverMap.builder();
        for (int ki = 0; ki < keyColumnNames.length; ++ki) {
            List<String> columnPath;
            try {
                columnPath = of(keyColumnNames[ki]).orElse(List.of());
            } catch (TableDataException e) {
                columnPath = List.of();
            }
            builder.putMap(keyColumnNames[ki],
                    !columnPath.isEmpty() && columnPath.get(0).equals(parquetKeyColumnNames[ki])
                            ? columnPath
                            : List.of(parquetKeyColumnNames[ki]));
        }
        return builder.putMap(INDEX_ROW_SET_COLUMN_NAME,
                List.of(rowSetParquetColumnName(indexSchema, parquetKeyColumnNames))).build();
    }

    /**
     * Returns the parquet name of the row set column in the file, with the parquet schema {@code indexSchema}, of a
     * data index on the Deephaven columns {@code keyColumnNames}; see {@link #forDataIndex(MessageType, String...)}.
     * Returns {@code dh_row_set} if any key column cannot be translated.
     */
    @NotNull
    String findRowSetParquetColumnName(
            @NotNull final MessageType indexSchema,
            @NotNull final String... keyColumnNames) {
        final String[] parquetKeyColumnNames = findIndexParquetColumnNames(Arrays.asList(keyColumnNames));
        return parquetKeyColumnNames == null
                ? INDEX_ROW_SET_COLUMN_NAME
                : rowSetParquetColumnName(indexSchema, parquetKeyColumnNames);
    }

    /**
     * Returns the parquet name of the row set column in a data index file with the parquet schema {@code indexSchema},
     * whose key columns have the top-level parquet names {@code parquetKeyColumnNames}: {@code dh_row_set} if a column
     * that is not a key column has that name, or else the only column that is not a key column, if there is exactly
     * one. A key column may have the parquet name {@code dh_row_set}, when the indexed table's instructions map a key
     * column to that name.
     */
    @NotNull
    private static String rowSetParquetColumnName(
            @NotNull final MessageType indexSchema,
            @NotNull final String[] parquetKeyColumnNames) {
        final Set<String> parquetKeyColumnNameSet = new HashSet<>(Arrays.asList(parquetKeyColumnNames));
        final List<String> nonKeyParquetColumnNames = indexSchema.getFields().stream()
                .map(Type::getName)
                .filter(parquetColumnName -> !parquetKeyColumnNameSet.contains(parquetColumnName))
                .collect(Collectors.toList());
        if (nonKeyParquetColumnNames.contains(INDEX_ROW_SET_COLUMN_NAME) || nonKeyParquetColumnNames.size() != 1) {
            return INDEX_ROW_SET_COLUMN_NAME;
        }
        return nonKeyParquetColumnNames.get(0);
    }

    /**
     * Returns the definition to read the table of a data index on the Deephaven columns {@code keyColumnNames} with:
     * each key column as the table definition defines it, so the keys have that column's type, and the row set column.
     * Returns {@code null} if there is no table definition, or it does not define a key column.
     */
    @Nullable
    TableDefinition dataIndexDefinition(@NotNull final String... keyColumnNames) {
        final TableDefinition definition = readInstructions.getTableDefinition().orElse(null);
        if (definition == null) {
            return null;
        }
        final List<ColumnDefinition<?>> columns = new ArrayList<>(keyColumnNames.length + 1);
        for (final String keyColumnName : keyColumnNames) {
            final ColumnDefinition<?> keyColumn = definition.getColumn(keyColumnName);
            if (keyColumn == null) {
                return null;
            }
            columns.add(keyColumn.withNormal());
        }
        columns.add(ColumnDefinition.fromGenericType(INDEX_ROW_SET_COLUMN_NAME, RowSet.class));
        return TableDefinition.of(columns);
    }

    /**
     * Returns the definition to read a legacy grouping table with: its key column, under its fixed name but as the
     * table definition defines the column {@code keyColumnName} it indexes, and its start and end positions. Returns
     * {@code null} if there is no table definition, or it does not define the key column.
     */
    @Nullable
    TableDefinition groupingDefinition(@NotNull final String keyColumnName) {
        final ColumnDefinition<?> keyColumn = readInstructions.getTableDefinition()
                .map(definition -> definition.getColumn(keyColumnName))
                .orElse(null);
        return keyColumn == null
                ? null
                : TableDefinition.of(
                        keyColumn.withNormal().withName(GROUPING_KEY_COLUMN_NAME),
                        ColumnDefinition.ofLong(GROUPING_BEGIN_POS_COLUMN_NAME),
                        ColumnDefinition.ofLong(GROUPING_END_POS_COLUMN_NAME));
    }

    /**
     * Returns the parquet names the metadata records data index key columns under, given their Deephaven names, or
     * {@code null} if any of them has none.
     */
    @Nullable
    String[] findIndexParquetColumnNames(@NotNull final Collection<String> columnNames) {
        return translateAll(columnNames, columnName -> renamesLeaveAlone(columnName)
                ? isIndexKey(columnName) ? columnName : null
                : metadataNames().indexParquetByDeephaven.get(columnName));
    }

    /**
     * Returns the Deephaven names of data index key columns, given the parquet names the metadata records them under,
     * or {@code null} if any of them has none.
     */
    @Nullable
    String[] findIndexDeephavenColumnNames(@NotNull final Collection<String> parquetColumnNames) {
        return translateAll(parquetColumnNames, parquetColumnName -> renamesLeaveAlone(parquetColumnName)
                ? isIndexKey(parquetColumnName) ? parquetColumnName : null
                : metadataNames().indexDeephavenByParquet.get(parquetColumnName));
    }

    /**
     * Whether, without a column resolver, the renames leave {@code name} alone in both directions. The Deephaven column
     * with that name, if there is one, is then the one reader of the parquet column with that name: any other reader
     * would need a rename of either. This is every name when there are no renames.
     */
    private boolean renamesLeaveAlone(@NotNull final String name) {
        if (instructionsResolver != null
                || !name.equals(readInstructions.getParquetColumnNameFromColumnNameOrDefault(name))) {
            return false;
        }
        final String renamedFrom = readInstructions.getColumnNameFromParquetColumnName(name);
        return renamedFrom == null || renamedFrom.equals(name);
    }

    /**
     * For a name the renames leave alone, whether it names a data index key column, by the same rules as
     * {@link #metadataNames()}: a whole column of the file, read by the Deephaven column with that name, or a
     * partitioning column, which the file does not have, but not the index table's row set column. A file column that
     * has a partitioning column's name is not a key: the partitioning column's values come from the location, not from
     * that file column.
     */
    private boolean isIndexKey(@NotNull final String name) {
        if (INDEX_ROW_SET_COLUMN_NAME.equals(name)) {
            return false;
        }
        if (schema.containsField(name)) {
            return readsFile(name) && readsWholeColumn(of(name).orElse(List.of()));
        }
        return isPartitioning(name);
    }

    /**
     * Whether the Deephaven column {@code columnName} may read the file: it is in the table definition, or there is no
     * definition, as for a location built without one, and it is not a partitioning column, whose values come from the
     * location rather than the file.
     */
    private boolean readsFile(@NotNull final String columnName) {
        return !isPartitioning(columnName) && readInstructions.getTableDefinition()
                .map(definition -> definition.getColumn(columnName) != null)
                .orElse(true);
    }

    /**
     * Whether the Deephaven column {@code columnName} is a partitioning column: one of the location's partition keys,
     * or a partitioning column of the table definition.
     */
    private boolean isPartitioning(@NotNull final String columnName) {
        return partitionKeys.contains(columnName) || readInstructions.getTableDefinition()
                .map(definition -> definition.getColumn(columnName))
                .filter(ColumnDefinition::isPartitioning)
                .isPresent();
    }

    /**
     * Builds, on first use, the translation of the file's metadata names to Deephaven column names, for the names the
     * renames do not leave alone, or for every name with a column resolver.
     */
    @NotNull
    private MetadataNames metadataNames() {
        MetadataNames local;
        if ((local = metadataNames) != null) {
            return local;
        }
        synchronized (this) {
            if ((local = metadataNames) != null) {
                return local;
            }
            // Each top-level parquet column, mapped to the Deephaven column that reads it whole. A parquet column read
            // by more than one Deephaven column is left out: each of those Deephaven columns still reads the parquet
            // column, but none can be identified as the Deephaven column its metadata describes, so none gets its
            // sortedness or indexes. Partitioning columns read nothing from the file, even when a rename maps a
            // partitioning column's name to a file column.
            final Map<String, String> deephavenByParquet = new HashMap<>();
            final Set<String> readByMany = new HashSet<>();
            for (final String columnName : candidateColumnNames()) {
                if (!readsFile(columnName)) {
                    continue;
                }
                final List<String> columnPath;
                try {
                    columnPath = of(columnName).orElse(List.of());
                } catch (TableDataException e) {
                    // The column cannot be resolved in this file, so it reads none of its columns; reading it reports
                    // the error
                    continue;
                }
                if (readsWholeColumn(columnPath)
                        && deephavenByParquet.putIfAbsent(columnPath.get(0), columnName) != null) {
                    readByMany.add(columnPath.get(0));
                }
            }
            deephavenByParquet.keySet().removeAll(readByMany);

            // A data index's key columns may also be partitioning columns, which are not read from the file. The
            // metadata records them under their parquet names, which no file column may have: the renamed name, or with
            // a column resolver, which ignores renames, the column's own. A key column named like the index table's row
            // set column cannot be told apart from it.
            final Map<String, String> indexDeephavenByParquet = new HashMap<>(deephavenByParquet);
            final Set<String> partitioningColumnNames = new HashSet<>(partitionKeys);
            readInstructions.getTableDefinition().ifPresent(definition -> definition.getColumnStream()
                    .filter(ColumnDefinition::isPartitioning)
                    .forEach(column -> partitioningColumnNames.add(column.getName())));
            for (final String columnName : partitioningColumnNames) {
                final String parquetColumnName = instructionsResolver == null
                        ? readInstructions.getParquetColumnNameFromColumnNameOrDefault(columnName)
                        : columnName;
                if (!schema.containsField(parquetColumnName) && (instructionsResolver != null || columnName.equals(
                        readInstructions.getColumnNameFromParquetColumnNameOrDefault(parquetColumnName)))) {
                    indexDeephavenByParquet.put(parquetColumnName, columnName);
                }
            }
            indexDeephavenByParquet.values().removeIf(INDEX_ROW_SET_COLUMN_NAME::equals);
            return metadataNames = new MetadataNames(deephavenByParquet, indexDeephavenByParquet);
        }
    }

    /**
     * The Deephaven columns that may read this file: those of the table definition or, for a location built without
     * one, the names the renames give the file's top-level columns.
     */
    @NotNull
    private Collection<String> candidateColumnNames() {
        return readInstructions.getTableDefinition()
                .<Collection<String>>map(TableDefinition::getColumnNames)
                .orElseGet(() -> schema.getFields().stream()
                        .map(field -> readInstructions.getColumnNameFromParquetColumnNameOrDefault(field.getName()))
                        .collect(Collectors.toCollection(LinkedHashSet::new)));
    }

    /**
     * Whether {@code columnPath} reads one whole top-level column of the file: the column itself, or its only leaf, as
     * for a LIST column such as a vector, rather than one field of a column with several.
     */
    private boolean readsWholeColumn(@NotNull final List<String> columnPath) {
        if (columnPath.isEmpty() || !schema.containsField(columnPath.get(0))) {
            return false;
        }
        Type type = schema.getType(columnPath.get(0));
        for (final String fieldName : columnPath.subList(1, columnPath.size())) {
            if (type.isPrimitive()
                    || type.asGroupType().getFieldCount() != 1
                    || !type.asGroupType().getType(0).getName().equals(fieldName)) {
                return false;
            }
            type = type.asGroupType().getType(0);
        }
        return true;
    }

    @Nullable
    private static String[] translateAll(
            @NotNull final Collection<String> names,
            @NotNull final Function<String, String> translation) {
        final String[] translated = new String[names.size()];
        int ni = 0;
        for (final String name : names) {
            if ((translated[ni++] = translation.apply(name)) == null) {
                return null;
            }
        }
        return translated;
    }

    /**
     * The translation of the file's metadata names to Deephaven column names; see {@link #metadataNames()}.
     */
    private static final class MetadataNames {
        /** Each top-level parquet column, mapped to the one Deephaven column that reads it whole. */
        private final Map<String, String> deephavenByParquet;
        /**
         * Like {@link #deephavenByParquet}, for the key columns of a data index, which may also be partitioning columns
         * that are not read from the file.
         */
        private final Map<String, String> indexDeephavenByParquet;
        /** The inverse of {@link #indexDeephavenByParquet}. */
        private final Map<String, String> indexParquetByDeephaven;

        private MetadataNames(
                @NotNull final Map<String, String> deephavenByParquet,
                @NotNull final Map<String, String> indexDeephavenByParquet) {
            this.deephavenByParquet = Collections.unmodifiableMap(deephavenByParquet);
            this.indexDeephavenByParquet = Collections.unmodifiableMap(indexDeephavenByParquet);
            final Map<String, String> indexParquetByDeephaven = new HashMap<>(indexDeephavenByParquet.size());
            indexDeephavenByParquet.forEach(
                    (parquetColumnName, columnName) -> indexParquetByDeephaven.put(columnName, parquetColumnName));
            this.indexParquetByDeephaven = Collections.unmodifiableMap(indexParquetByDeephaven);
        }
    }
}
