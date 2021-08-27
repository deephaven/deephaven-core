/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.sources.AbstractColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Utility to take a set of tables, each of which is sorted; and merge them together into a single table, which will
 * also be sorted. For now we do not support refreshing tables, just zipping together tables that are already sorted and
 * will not tick.
 *
 * To handle ticking tables; the data structures would need to be changed, we're storing everything in parallel arrays
 * and to tick we would need to shift those around. Handling append only could work; but there would be a lot of
 * shifting if the data arrives particularly out of order.
 */
public class MergeSortedHelper {
    private static class TableCursor implements Comparable<TableCursor> {
        private final ColumnSource<? extends Comparable> keyColumnSource;
        private final Index.Iterator iterator;
        private final int tableIndex;
        Comparable currentKey;
        private boolean done = false;
        private long currentIndex;

        TableCursor(Table table, String keyColumn, int tableIndex) {
            this.tableIndex = tableIndex;
            keyColumnSource = table.getColumnSource(keyColumn);
            iterator = table.getIndex().iterator();
            advance();
        }

        void advance() {
            if (iterator.hasNext()) {
                currentIndex = iterator.nextLong();
                currentKey = keyColumnSource.get(currentIndex);
            } else {
                done = true;
            }
        }

        @Override
        public int compareTo(@NotNull TableCursor other) {
            if (other.done) {
                if (this.done) {
                    return 0;
                } else {
                    return -1;
                }
            } else if (this.done) {
                return 1;
            }

            // noinspection unchecked
            final int compareTo = this.currentKey.compareTo(other.currentKey);
            if (compareTo == 0) {
                return this.tableIndex - other.tableIndex;
            }
            return compareTo;
        }

        public boolean isDone() {
            return done;
        }

        public long getCurrentIndex() {
            return currentIndex;
        }

        public int getTableIndex() {
            return tableIndex;
        }
    }

    public static Table mergeSortedHelper(String keyColumn, Collection<Table> tables) {
        PriorityQueue<TableCursor> priorityQueue = new PriorityQueue<>();

        LinkedHashMap<String, SortedMergeColumnSource<?>> columnSources = new LinkedHashMap<>();
        TIntArrayList tableList = new TIntArrayList();
        TLongArrayList indexList = new TLongArrayList();

        int tableIndex = 0;
        for (Table table : tables) {
            if (!(table instanceof BaseTable)) {
                throw new UnsupportedOperationException("Can not perform mergeSorted unless you pass in a BaseTable!");
            }
            if (((BaseTable) table).isRefreshing()) {
                throw new UnsupportedOperationException("mergeSorted does not yet support refreshing tables!");
            }

            if (tableIndex == 0) {
                for (Map.Entry<String, ? extends ColumnSource<?>> entry : table.getColumnSourceMap().entrySet()) {
                    columnSources.put(entry.getKey(),
                            new SortedMergeColumnSource<>(tableList, indexList, entry.getValue()));
                }
            } else {
                if (!table.getColumnSourceMap().keySet().equals(columnSources.keySet())) {
                    throw new RuntimeException(
                            "Incompatible column sources: " + Arrays.toString(columnSources.keySet().toArray())
                                    + " and " + Arrays.toString(table.getColumnSourceMap().keySet().toArray()));
                }
                for (Map.Entry<String, ? extends ColumnSource<?>> entry : table.getColumnSourceMap().entrySet()) {
                    // noinspection unchecked,rawtypes
                    columnSources.get(entry.getKey()).addSource((ColumnSource) entry.getValue());
                }
            }

            final TableCursor tableCursor = new TableCursor(table, keyColumn, tableIndex);
            if (!tableCursor.isDone()) {
                priorityQueue.add(tableCursor);
            }

            tableIndex++;
        }


        while (!priorityQueue.isEmpty()) {
            TableCursor head = priorityQueue.poll();
            tableList.add(head.getTableIndex());
            indexList.add(head.getCurrentIndex());
            head.advance();
            if (!head.isDone()) {
                priorityQueue.add(head);
            }
        }

        return new QueryTable(Index.FACTORY.getFlatIndex(tableList.size()), columnSources);
    }

    static public class SortedMergeColumnSource<T> extends AbstractColumnSource<T> {
        private final TIntArrayList tableIndex;
        private final TLongArrayList columnIndex;
        private final ArrayList<ColumnSource<T>> innerSources;

        @Override
        public Class<?> getComponentType() {
            return innerSources.get(0).getComponentType();
        }

        public SortedMergeColumnSource(TIntArrayList tableIndex, TLongArrayList columnIndex,
                ColumnSource<T> firstSource) {
            super(firstSource.getType());
            this.tableIndex = tableIndex;
            this.columnIndex = columnIndex;
            this.innerSources = new ArrayList<>();
            innerSources.add(firstSource);
        }

        void addSource(ColumnSource<T> source) {
            innerSources.add(source);
            Require.eq(source.getType(), "source.getType()", innerSources.get(0).getType(),
                    "innerSources.get(0).getType()");
        }

        @Override
        public T get(long index) {
            int table = tableIndex.getQuick((int) index);
            long indexKey = columnIndex.getQuick((int) index);
            return innerSources.get(table).get(indexKey);
        }

        @Override
        public Boolean getBoolean(long index) {
            int table = tableIndex.getQuick((int) index);
            long indexKey = columnIndex.getQuick((int) index);
            return innerSources.get(table).getBoolean(indexKey);
        }

        @Override
        public byte getByte(long index) {
            int table = tableIndex.getQuick((int) index);
            long indexKey = columnIndex.getQuick((int) index);
            return innerSources.get(table).getByte(indexKey);
        }

        @Override
        public char getChar(long index) {
            int table = tableIndex.getQuick((int) index);
            long indexKey = columnIndex.getQuick((int) index);
            return innerSources.get(table).getChar(indexKey);
        }

        @Override
        public double getDouble(long index) {
            int table = tableIndex.getQuick((int) index);
            long indexKey = columnIndex.getQuick((int) index);
            return innerSources.get(table).getDouble(indexKey);
        }

        @Override
        public float getFloat(long index) {
            int table = tableIndex.getQuick((int) index);
            long indexKey = columnIndex.getQuick((int) index);
            return innerSources.get(table).getFloat(indexKey);
        }

        @Override
        public int getInt(long index) {
            int table = tableIndex.getQuick((int) index);
            long indexKey = columnIndex.getQuick((int) index);
            return innerSources.get(table).getInt(indexKey);

        }

        @Override
        public long getLong(long index) {
            int table = tableIndex.getQuick((int) index);
            long indexKey = columnIndex.getQuick((int) index);
            return innerSources.get(table).getLong(indexKey);
        }

        @Override
        public short getShort(long index) {
            int table = tableIndex.getQuick((int) index);
            long indexKey = columnIndex.getQuick((int) index);
            return innerSources.get(table).getShort(indexKey);
        }

        @Override
        public T getPrev(long index) {
            return get(index);
        }

        @Override
        public Boolean getPrevBoolean(long index) {
            return getBoolean(index);
        }

        @Override
        public byte getPrevByte(long index) {
            return getByte(index);
        }

        @Override
        public char getPrevChar(long index) {
            return getChar(index);
        }

        @Override
        public double getPrevDouble(long index) {
            return getDouble(index);
        }

        @Override
        public float getPrevFloat(long index) {
            return getFloat(index);
        }

        @Override
        public int getPrevInt(long index) {
            return getInt(index);
        }

        @Override
        public long getPrevLong(long index) {
            return getLong(index);
        }

        @Override
        public short getPrevShort(long index) {
            return getShort(index);
        }

        @Override
        public boolean isImmutable() {
            return true;
        }
    }
}
