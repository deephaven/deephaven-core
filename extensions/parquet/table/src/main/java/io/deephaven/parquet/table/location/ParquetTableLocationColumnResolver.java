//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.parquet.table.ParquetInstructions;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

import static io.deephaven.parquet.table.ParquetTableWriter.INDEX_ROW_SET_COLUMN_NAME;

/**
 * The one column resolver for the file of a {@link ParquetTableLocation}. It finds each Deephaven column with the
 * instructions' column resolver, if they have one, or else by their renames, whose default is the identity. A column
 * resolver is the alternative to renames; with one, the renames are ignored.
 * <p>
 * It also translates the file's metadata, which names parquet columns, to the Deephaven columns that read them. A name
 * the renames leave alone has one possible reader, the Deephaven column with that name, so translating it is a check of
 * that column alone; with no renames and no column resolver, the common case, that is every name. Otherwise the reverse
 * direction is built from the forward one, on first use, since building it resolves every Deephaven column.
 */
final class ParquetTableLocationColumnResolver implements ParquetColumnResolver {

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
            return isDefined(parquetColumnName) && readsWholeColumn(of(parquetColumnName).orElse(List.of()))
                    ? parquetColumnName
                    : null;
        }
        return metadataNames().deephavenByParquet.get(parquetColumnName);
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
     * partitioning column, which the file does not have, but not the index table's row set column.
     */
    private boolean isIndexKey(@NotNull final String name) {
        if (INDEX_ROW_SET_COLUMN_NAME.equals(name)) {
            return false;
        }
        if (schema.containsField(name)) {
            return isDefined(name) && readsWholeColumn(of(name).orElse(List.of()));
        }
        return partitionKeys.contains(name) || readInstructions.getTableDefinition()
                .map(definition -> definition.getColumn(name))
                .filter(ColumnDefinition::isPartitioning)
                .isPresent();
    }

    /**
     * Whether the Deephaven column {@code columnName} may read the file: it is in the table definition, or there is no
     * definition, as for a location built without one.
     */
    private boolean isDefined(@NotNull final String columnName) {
        return readInstructions.getTableDefinition()
                .map(definition -> definition.getColumn(columnName) != null)
                .orElse(true);
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
            // by more than one Deephaven column is left out: each still reads it, but none can be identified as the one
            // its metadata describes, so none gets its sortedness or indexes.
            final Map<String, String> deephavenByParquet = new HashMap<>();
            final Set<String> readByMany = new HashSet<>();
            for (final String columnName : candidateColumnNames()) {
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
     * one, those the renames name the file's top-level columns.
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
