package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;

import java.util.Objects;

/**
 * Divide a table evenly into slices and put the slices into a TableMap.
 *
 * This enables the use of parallelism on a table without the full cost of a byExternal operation.
 */
public class EvenlyDividedTableMap {
    private EvenlyDividedTableMap() {} // static use only

    private static class Division {
        final long start;
        final long end;

        private Division(long start, long end) {
            this.start = start;
            this.end = end;
        }

        @Override
        public String toString() {
            return "Division{" +
                    "start=" + start +
                    ", end=" + end +
                    '}';
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final Division division = (Division) o;
            return start == division.start &&
                    end == division.end;
        }

        @Override
        public int hashCode() {
            return Objects.hash(start, end);
        }
    }

    /**
     * Divide the table into even slices, inserting the results into a TableMap.
     *
     * <p>
     * This method is intended to be used to enable parallelism by creating a TableMap without the cost of a full
     * byExternal. It is important to note that any natural boundaries in the data are not respected, thus when doing
     * operations in parallel and calling {@link TransformableTableMap#merge()} the result is very likely not the same
     * as doing the operations individually. Care must be taken to preserve your desired semantics.
     * </p>
     *
     * @param table the table to divide
     * @param divisions how many divisions should be created, you may want to align this value with
     *        {@link TableMapTransformThreadPool#TRANSFORM_THREADS}.
     * @param minimumSize the minimum size of each division
     * @return a TableMap with constituents representing the slices of the original table
     */
    static TableMap makeEvenlyDividedTableMap(Table table, int divisions, int minimumSize) {
        if (table instanceof UncoalescedTable) {
            table = table.coalesce();
        }
        final QueryTable queryTable = (QueryTable) table;
        final long tableSize = queryTable.size();
        final long divisionSize = Math.max((tableSize + divisions - 1) / divisions, minimumSize);

        final LocalTableMap localTableMap = new LocalTableMap(null);

        long start;
        for (start = 0; start < tableSize; start += divisionSize) {
            localTableMap.put(new Division(start, start + divisionSize), queryTable.slice(start, start + divisionSize));
        }
        if (queryTable.isRefreshing()) {
            localTableMap.setRefreshing(true);
            final long fStart = start;
            final ShiftAwareListener listener =
                    new InstrumentedShiftAwareListenerAdapter("tablemap division", queryTable, false) {
                        long currentEnd = fStart;

                        @Override
                        public void onUpdate(Update upstream) {
                            if (queryTable.getIndex().size() > currentEnd) {
                                // we should slice the table again and make a new segment for our tablemap
                                while (currentEnd < queryTable.getIndex().size()) {
                                    localTableMap.put(new Division(currentEnd, currentEnd + divisionSize),
                                            queryTable.slice(currentEnd, currentEnd + divisionSize));
                                    currentEnd += divisionSize;
                                }
                            }
                        }
                    };
            queryTable.listenForUpdates(listener);
            localTableMap.manage(listener);
            localTableMap.addParentReference(listener);
            localTableMap.addParentReference(queryTable);
        }

        return localTableMap;
    }
}
