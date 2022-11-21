/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.treetable;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.exceptions.UncheckedTableException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.time.DateTime;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.BooleanUtils;
import io.deephaven.engine.util.ColumnFormattingValues;
import io.deephaven.engine.table.impl.HierarchicalTableInfo;
import io.deephaven.engine.table.impl.RollupInfo;
import io.deephaven.engine.table.TableMap;
import io.deephaven.engine.table.ColumnSource;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.Collectors;

class SnapshotState {
    @SuppressWarnings("unchecked")
    private static final Pair<String, Object>[] EMPTY_CONSTITUENT_ARRAY = new Pair[0];

    private final BaseHierarchicalTable baseTable;
    private final int childColumnIndex;

    /**
     * A List of column-name, data array mappings. The arrays in this mapping are references to items in dataMatrix
     * below
     */
    private final List<Pair<String, Object>> data = new ArrayList<>();

    /** A rectangular matrix of column x data values for the snapshot. */
    private final Object[] dataMatrix;

    /** Copiers which copy data from the rolled up table to the snapshot result */
    private Copier[] columnCopiers;
    private BitSet columns;

    /** How many rows have actually been copied into the snapshot */
    private int copied = 0;

    /** The number of total rows that have been skipped by the snapshot algorithm before data is collected */
    int skippedRows = 0;

    /** The number of rows that have been consumed and will be added to the viewport */
    int consumed = 0;

    /** The total number of rows in the tree */
    long totalRowCount = 0;

    /** The actual adjusted size of the viewport */
    int actualViewportSize = 0;

    BitSet childPresenceColumn = new BitSet(0);
    Object[] tableKeyColumn = CollectionUtil.ZERO_LENGTH_OBJECT_ARRAY;

    // region Rollup Only Data
    /** Mapping of aggregated column name to source column name */
    private final Map<String, String> aggToSourceMap;

    /** Copiers that copy data from the original table to the intermediate columns. These are for rollups only */
    private Map<String, Copier> constituentCopiers;

    /** Analagous to {@link #data} except containing entries for constituent rows */
    private final Map<String, Pair<String, Object>> constituentData;

    /** A set containing the constituent columns that must be included with the snapshot result */
    private final Set<String> includedConstituentColumns;

    /** Indicator for if this rollup contains constiuent rows. */
    private final boolean includeConstituents;

    private final HierarchicalTableInfo info;

    /**
     * Single snapshot state -- Set to true if the snapshot in progress included data from an include-constituent Rollup
     * leaf table
     */
    private boolean includesLeafData = false;
    // endregion

    interface Copier {
        void copy(boolean usePrev, ColumnSource columnSource, RowSet.Iterator it, Object target, int offset,
                Table table,
                TableMap tableMap, BitSet childPresenceColumn);
    }

    SnapshotState(BaseHierarchicalTable baseTable, String hierarchicalColumnName) {
        this.baseTable = baseTable;
        this.info = baseTable.getInfo();
        includeConstituents =
                (info instanceof RollupInfo && ((RollupInfo) info).getLeafType() == RollupInfo.LeafType.Constituent);

        final TableDefinition definition = baseTable.getDefinition();
        childColumnIndex = definition.getColumnIndex(definition.getColumn(hierarchicalColumnName));

        // Only do the alternates if we're a rollup including constituent rows @ the leaf
        if (includeConstituents) {
            final Table source = baseTable.getSourceTable();
            constituentCopiers = new HashMap<>();
            constituentData = new HashMap<>();
            aggToSourceMap = ((RollupInfo) info).getMatchPairs()
                    .stream()
                    .filter(p -> source.hasColumns(p.rightColumn)) // Filter out any columns that don't exist in the
                                                                   // parent
                                                                   // this is a concern for the Count aggregation.
                    .collect(Collectors.toMap(MatchPair::leftColumn, MatchPair::rightColumn));
            includedConstituentColumns = new HashSet<>();
        } else {
            constituentCopiers = null;
            constituentData = null;
            aggToSourceMap = null;
            includedConstituentColumns = null;
        }

        makeCopiers();
        dataMatrix = new Object[definition.numColumns()];
    }

    void beginSnapshot(Map<Object, TableDetails> tablesByKey, BitSet requestedColumns, long firstViewportRow,
            long lastViewportRow) {
        // Compute snapshot boundaries
        includesLeafData = false;
        skippedRows = consumed = copied = 0;
        childPresenceColumn.clear();
        totalRowCount = tablesByKey.values().stream()
                .filter(v -> !v.isRemoved())
                .mapToLong(v -> v.getTable().size()).sum();

        final int newViewportSize =
                (int) (Math.min(lastViewportRow + 1, totalRowCount) - Math.min(firstViewportRow, totalRowCount));

        // Do any allocation required if we have to.
        // Update the final data matrix with the proper set of requeted columns.
        if (ensureSpace(newViewportSize, tablesByKey.get(TreeConstants.ROOT_TABLE_KEY))
                || !requestedColumns.equals(columns)) {
            this.columns = requestedColumns;

            // Properly populate the dataMatrix
            for (int ii = 0; ii < data.size(); ii++) {
                if (requestedColumns.get(ii)) {
                    final Pair<String, Object> dataPair = data.get(ii);
                    dataMatrix[ii] = dataPair.second;

                    // If including constituents, populate the set of original columns that we actually need, to save us
                    // work
                    // later on. Ignore the by columns, they do not get modified.
                    if (includeConstituents) { // Do we include constituents?
                        final String sourceName = aggToSourceMap.get(dataPair.first);
                        if (!io.deephaven.engine.util.string.StringUtils.isNullOrEmpty(sourceName) &&
                                !((RollupInfo) info).getSelectColumnNames().contains(dataPair.first) && // Is it one of
                                                                                                        // the grouping
                                                                                                        // columns?
                                !RollupInfo.ROLLUP_COLUMN.equals(dataPair.first) && // Is it the magic rollup column?
                                !ColumnFormattingValues.isFormattingColumn(dataPair.first)) { // Is it a formatting
                                                                                              // column?
                            includedConstituentColumns.add(sourceName);
                        }
                    }
                } else {
                    dataMatrix[ii] = null;
                }
            }
        }
    }

    /**
     * Copy data from the specified table into the resultant data matrix at the offsets specified in the viewport state.
     *
     * @implNote This method populates an additional data column that maps each row back to the table they came from by
     *           it's table key, so that clients can map rows back into the tree structure.
     *
     * @param usePrev if the snapshot should use previous values.
     * @param snapshotRowSet A RowSet containing the rows to copy from the source table.
     */
    void addToSnapshot(boolean usePrev, Table table, Object tableKey, TableMap tableMap, RowSet snapshotRowSet) {
        Assert.leq(copied + snapshotRowSet.size(), "dataOffset + snapshotRowSet.size()", actualViewportSize,
                "viewport size");

        if (table.hasAttribute(Table.ROLLUP_LEAF_ATTRIBUTE) && includeConstituents) {
            addToSnapshotConstituent(usePrev, table, tableMap, snapshotRowSet);
        } else {
            addToSnapshotNormal(usePrev, table, tableMap, snapshotRowSet);
        }

        // Associate the rows with this table.
        Arrays.fill(tableKeyColumn, copied, copied + snapshotRowSet.intSize(), tableKey);

        copied += snapshotRowSet.size();
    }

    /**
     * Copy data directly from the table in question.
     */
    private void addToSnapshotNormal(boolean usePrev, Table table, TableMap tableMap, RowSet snapshotRowSet) {
        for (int ii = 0; ii < data.size(); ii++) {
            if (!columns.get(ii)) {
                continue;
            }

            final ColumnSource cs = table.getColumnSource(data.get(ii).first);
            columnCopiers[ii].copy(usePrev, cs, snapshotRowSet.iterator(), data.get(ii).second, copied, table, tableMap,
                    childPresenceColumn);
        }
    }

    /**
     * Copy the data from the table in question, assuming that the table is a constituent table. This means that we need
     * to copy the data to the alternate data set because column types may be reused or changed.
     */
    private void addToSnapshotConstituent(boolean usePrev, Table table, TableMap tableMap, RowSet snapshotRowSet) {
        includesLeafData = true;
        includedConstituentColumns.forEach(cn -> {
            final ColumnSource<?> cs = table.getColumnSource(cn);
            constituentCopiers.get(cn).copy(usePrev, cs, snapshotRowSet.iterator(), constituentData.get(cn).getSecond(),
                    copied, table, tableMap, childPresenceColumn);
        });

        final Map<String, ? extends ColumnSource> columnSourceMap = table.getColumnSourceMap();
        // Include the by columns, and any formatting columns
        for (int ii = 0; ii < data.size(); ii++) {
            final String columnName = data.get(ii).first;
            if (columns.get(ii) &&
                    (((RollupInfo) info).getSelectColumnNames().contains(columnName) ||
                            ColumnFormattingValues.isFormattingColumn(columnName))) {

                // In the case of constituent rows, because column types can change, we may have omitted formatting
                // columns
                // so we'll allow those to not exist and be null-filled
                final ColumnSource<?> cs = columnSourceMap.get(columnName);
                if (cs != null) {
                    columnCopiers[ii].copy(usePrev, cs, snapshotRowSet.iterator(), data.get(ii).second, copied, table,
                            tableMap, childPresenceColumn);
                } else if (!ColumnFormattingValues.isFormattingColumn(columnName)) {
                    throw new UncheckedTableException(
                            "Column " + columnName + " does not exist. Available column names are [" +
                                    String.join(",", columnSourceMap.keySet()) + "]");
                }
            }
        }
    }

    /**
     * Allocate a type-correct matrix to store the resultant snapshot into. It will create Columns+1 columns to allow
     * for the extra table-mapping column.
     *
     * @param requestedViewportSize The total size of the snapshot to allocate.
     * @param rootData Any table in the tree, to read column types from
     *
     * @return True if reallocation was done, false otherwise.
     */
    private boolean ensureSpace(int requestedViewportSize, TableDetails rootData) {
        if (requestedViewportSize == actualViewportSize) {
            // Since viewports will always be exactly sized, we don't need to iterate the arrays and null values out.
            // Copiers will
            // always overwrite.
            return false;
        }

        actualViewportSize = requestedViewportSize;
        data.clear();

        rootData.getTable().getDefinition().getColumns()
                .forEach(col -> data.add(makeData(col, requestedViewportSize)));

        tableKeyColumn = new Object[requestedViewportSize];
        childPresenceColumn = new BitSet(requestedViewportSize);

        if (includeConstituents) {
            // TODO: With a little bit of creativity we can re-use the existing column array if the original and rolldup
            // TODO: column types are the same. I will do this in a second pass, after this one works.
            final Table originalTable = baseTable.getSourceTable();
            constituentData.clear();
            originalTable.getDefinition().getColumns()
                    .forEach(col -> constituentData.put(col.getName(), makeData(col, requestedViewportSize)));
        }

        return true;
    }

    /**
     * Create a Pair containing the name and appropriately typed and sized array store for a particular column.
     *
     * @param col the column to allocate space for.
     * @param requestedViewportSize the requested viewport size.
     *
     * @return A {@link Pair} of (Column Name, data array)
     */
    private Pair<String, Object> makeData(ColumnDefinition col, int requestedViewportSize) {
        final Class type = col.getDataType();
        final String name = col.getName();
        if (type == DateTime.class) {
            return new Pair<>(name, new long[requestedViewportSize]);
        } else if (type == boolean.class || type == Boolean.class) {
            return new Pair<>(name, new byte[requestedViewportSize]);
        } else if (type == long.class || type == Long.class) {
            return new Pair<>(name, new long[requestedViewportSize]);
        } else if (type == int.class || type == Integer.class) {
            return new Pair<>(name, new int[requestedViewportSize]);
        } else if (type == short.class || type == Short.class) {
            return new Pair<>(name, new short[requestedViewportSize]);
        } else if (type == byte.class || type == Byte.class) {
            return new Pair<>(name, new byte[requestedViewportSize]);
        } else if (type == char.class || type == Character.class) {
            return new Pair<>(name, new char[requestedViewportSize]);
        } else if (type == double.class || type == Double.class) {
            return new Pair<>(name, new double[requestedViewportSize]);
        } else if (type == float.class || type == Float.class) {
            return new Pair<>(name, new float[requestedViewportSize]);
//        } else if (type == SmartKey.class) {
//            return new Pair<>(name, new Object[requestedViewportSize]);
        } else {
            return new Pair<>(name, Array.newInstance(type, requestedViewportSize));
        }
    }

    /**
     * Create all of the {@link Copier Copiers} to copy table data into the snapshot matrix.
     */
    private void makeCopiers() {
        final ColumnSource[] columnSources =
                baseTable.getColumnSources().toArray(ColumnSource.ZERO_LENGTH_COLUMN_SOURCE_ARRAY);
        columnCopiers = new Copier[columnSources.length];

        for (int ii = 0; ii < columnSources.length; ii++) {
            final Class type = columnSources[ii].getType();
            columnCopiers[ii] = ii == childColumnIndex ? makeChildCopier(type) : makeCopier(type);
        }

        if (includeConstituents) {
            final Table originalTable = baseTable.getSourceTable();
            final List<MatchPair> matchPairs = ((RollupInfo) info).getMatchPairs();

            matchPairs.stream()
                    .filter(p -> originalTable.hasColumns(p.rightColumn))
                    .forEach(col -> constituentCopiers.computeIfAbsent(col.rightColumn,
                            (name) -> makeCopier(originalTable.getDefinition().getColumn(name).getDataType())));
        }
    }

    /**
     * Create a copier to copy the keys of child tables only if the child tables are of > 0 size
     *
     * @param type The type of the child column
     * @return A Copier for the child key column
     */
    private Copier makeChildCopier(Class type) {
        if (type == DateTime.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                Assert.neqNull(tableMap, "Child table map");
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    final DateTime keyVal =
                            (DateTime) (usePrev ? columnSource.getPrev(next) : columnSource.get(next));
                    final Table child = tableMap.get(keyVal);
                    ((long[]) target)[offset] = keyVal.getNanos();
                    childPresenceColumn.set(offset++, child != null && child.size() > 0);
                }
            };
        } else if (type == boolean.class || type == Boolean.class) {
            throw new UnsupportedOperationException("Booleans can't be used as child IDs");
        } else if (type == long.class || type == Long.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                Assert.neqNull(tableMap, "Child table map");
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    final long keyVal = usePrev ? columnSource.getPrevLong(next) : columnSource.getLong(next);
                    final Table child = tableMap.get(keyVal);
                    ((long[]) target)[offset] = keyVal;
                    childPresenceColumn.set(offset++, child != null && child.size() > 0);
                }
            };
        } else if (type == int.class || type == Integer.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                Assert.neqNull(tableMap, "Child table map");
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    final int keyVal = usePrev ? columnSource.getPrevInt(next) : columnSource.getInt(next);
                    final Table child = tableMap.get(keyVal);
                    ((int[]) target)[offset] = keyVal;
                    childPresenceColumn.set(offset++, child != null && child.size() > 0);
                }
            };
        } else if (type == short.class || type == Short.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                Assert.neqNull(tableMap, "Child table map");
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    final short keyVal = usePrev ? columnSource.getPrevShort(next) : columnSource.getShort(next);
                    final Table child = tableMap.get(keyVal);
                    ((short[]) target)[offset] = keyVal;
                    childPresenceColumn.set(offset++, child != null && child.size() > 0);
                }
            };
        } else if (type == byte.class || type == Byte.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                Assert.neqNull(tableMap, "Child table map");
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    final byte keyVal = usePrev ? columnSource.getPrevByte(next) : columnSource.getByte(next);
                    final Table child = tableMap.get(keyVal);
                    ((byte[]) target)[offset] = keyVal;
                    childPresenceColumn.set(offset++, child != null && child.size() > 0);
                }
            };
        } else if (type == char.class || type == Character.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                Assert.neqNull(tableMap, "Child table map");
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    final char keyVal = usePrev ? columnSource.getPrevChar(next) : columnSource.getChar(next);
                    final Table child = tableMap.get(keyVal);
                    ((char[]) target)[offset] = keyVal;
                    childPresenceColumn.set(offset++, child != null && child.size() > 0);
                }
            };
        } else if (type == double.class || type == Double.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                Assert.neqNull(tableMap, "Child table map");
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    final double keyVal = usePrev ? columnSource.getPrevDouble(next) : columnSource.getDouble(next);
                    final Table child = tableMap.get(keyVal);
                    ((double[]) target)[offset] = keyVal;
                    childPresenceColumn.set(offset++, child != null && child.size() > 0);
                }
            };
        } else if (type == float.class || type == Float.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                Assert.neqNull(tableMap, "Child table map");
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    final float keyVal = usePrev ? columnSource.getPrevFloat(next) : columnSource.getFloat(next);
                    final Table child = tableMap.get(keyVal);
                    ((float[]) target)[offset] = keyVal;
                    childPresenceColumn.set(offset++, child != null && child.size() > 0);
                }
            };
        }

        return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
            Assert.neqNull(tableMap, "Child table map");
            while (it.hasNext()) {
                final long next = it.nextLong();
                final Object keyVal = usePrev ? columnSource.getPrev(next) : columnSource.get(next);
                final Table child = tableMap.get(keyVal);
                ((Object[]) target)[offset] = keyVal;
                childPresenceColumn.set(offset++, child != null && child.size() > 0);
            }
        };
    }

    /**
     * Create a {@link Copier} that will copy data from columns into the local snapshot.
     */
    private Copier makeCopier(Class type) {
        if (type == DateTime.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    final DateTime dateTime =
                            (DateTime) (usePrev ? columnSource.getPrev(next) : columnSource.get(next));
                    ((long[]) target)[offset++] = dateTime == null ? QueryConstants.NULL_LONG : dateTime.getNanos();
                }
            };
        } else if (type == boolean.class || type == Boolean.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    ((byte[]) target)[offset++] = BooleanUtils
                            .booleanAsByte((Boolean) (usePrev ? columnSource.getPrev(next) : columnSource.get(next)));
                }
            };
        } else if (type == long.class || type == Long.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    ((long[]) target)[offset++] = usePrev ? columnSource.getPrevLong(next) : columnSource.getLong(next);
                }
            };
        } else if (type == int.class || type == Integer.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    ((int[]) target)[offset++] = usePrev ? columnSource.getPrevInt(next) : columnSource.getInt(next);
                }
            };
        } else if (type == short.class || type == Short.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    ((short[]) target)[offset++] =
                            usePrev ? columnSource.getPrevShort(next) : columnSource.getShort(next);
                }
            };
        } else if (type == byte.class || type == Byte.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    ((byte[]) target)[offset++] = usePrev ? columnSource.getPrevByte(next) : columnSource.getByte(next);
                }
            };
        } else if (type == char.class || type == Character.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    ((char[]) target)[offset++] = usePrev ? columnSource.getPrevChar(next) : columnSource.getChar(next);
                }
            };
        } else if (type == double.class || type == Double.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    ((double[]) target)[offset++] =
                            usePrev ? columnSource.getPrevDouble(next) : columnSource.getDouble(next);
                }
            };
        } else if (type == float.class || type == Float.class) {
            return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
                while (it.hasNext()) {
                    final long next = it.nextLong();
                    ((float[]) target)[offset++] =
                            usePrev ? columnSource.getPrevFloat(next) : columnSource.getFloat(next);
                }
            };
        }

        return (usePrev, columnSource, it, target, offset, table, tableMap, childPresenceColumn) -> {
            while (it.hasNext()) {
                final long next = it.nextLong();
                ((Object[]) target)[offset++] = usePrev ? columnSource.getPrev(next) : columnSource.get(next);
            }
        };
    }

    Object[] getDataMatrix() {
        return dataMatrix;
    }

    /**
     * Get any required alternate data columns. This is happens when a rollup is showing constituent rows and those rows
     * are part of the viewport.
     *
     */
    Pair<String, Object>[] getRequiredConstituents() {
        if (!includesLeafData) {
            return EMPTY_CONSTITUENT_ARRAY;
        }

        // noinspection unchecked
        return includedConstituentColumns.stream()
                .map(constituentData::get)
                .toArray(Pair[]::new);
    }
}
