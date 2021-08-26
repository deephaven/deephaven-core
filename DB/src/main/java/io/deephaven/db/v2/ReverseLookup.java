package io.deephaven.db.v2;

/**
 * For hierarchical table display, identify the row index key that corresponds to a given logical
 * key.
 */
public interface ReverseLookup {
    /**
     * Gets the index value where key exists in the table, or the no-entry-value if it is not found
     * in the table.
     *
     * @param key a single object for a single column, or a
     *        {@link io.deephaven.datastructures.util.SmartKey} for multiple columns
     * @return the row index where key exists in the table
     */
    long get(Object key);

    /**
     * Gets the index value where key previously in the table, or the no-entry-value if it is was
     * not found in the table.
     *
     * @param key a single object for a single column, or a
     *        {@link io.deephaven.datastructures.util.SmartKey} for multiple columns
     *
     * @return the row index where key previously existed in the table
     */
    long getPrev(Object key);

    /**
     * Returns the value that will be returned from {@link #get} or if no entry exists for a given
     * key.
     */
    long getNoEntryValue();

    /**
     * @return the key columns this reverse lookup is indexed on
     */
    String[] getKeyColumns();

    /**
     * A null implementation of a reverse lookup, suitable for a table without any key columns.
     */
    class Null implements ReverseLookup {
        private final String[] keyColumns;

        public Null(String... keyColumns) {
            this.keyColumns = keyColumns;
        }

        @Override
        public long get(Object key) {
            return -1;
        }

        @Override
        public long getPrev(Object key) {
            return -1;
        }

        @Override
        public long getNoEntryValue() {
            return -1;
        }

        @Override
        public String[] getKeyColumns() {
            return keyColumns;
        }

    }

    Null NULL = new Null();
}
