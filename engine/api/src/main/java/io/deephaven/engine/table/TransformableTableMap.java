package io.deephaven.engine.table;

import java.util.function.BiFunction;

/**
 * This object can be merged to produce a single coalesced Table.
 * <p>
 * This is used by TableMap and TableMapProxyHandlers to expose the {@link TransformableTableMap#merge} operation to
 * users.
 */
public interface TransformableTableMap {

    /**
     * Merges all the component tables into a single Table.
     *
     * @return all of our component tables merged into a single Table.
     */
    Table merge();

    /**
     * If you're a TableMap return this, otherwise if you're a Proxy return the underlying TableMap.
     *
     * @return a TableMap representation of this object
     */
    TableMap asTableMap();


    /**
     * Create a Table out of this TableMap's values.
     *
     * <p>
     * Creates a proxy object that in many respects acts like a Table, you can perform many of the table operations on
     * it, which are then applied using {@link TableMap#transformTables(java.util.function.Function)} or
     * {@link TableMap#transformTablesWithMap(TableMap, BiFunction)} if the right hand side of an operation is another
     * TableMap.
     * </p>
     *
     * <p>
     * The returned table acts as if it were an uncoalesced table; when two of our Proxy objects are operated on
     * together, e.g., by a {@link Table#join}) operation, then tables with identical keys are used. If strictKeys is
     * set, an error occurs if the two TableMaps do not have identical keySets.
     * </p>
     *
     * <p>
     * Supported operations include those which return a {@link Table}, {@link Table#size()},
     * {@link Table#getDefinition()} and operations to retrieve attributes. Operations which retrieve data (such as
     * {@link Table#getRowSet()}} or {@link Table#getColumn(int)} require a coalesce operation. If allowCoalesce is not
     * set to true, then the coalescing operations will fail with an {@link IllegalArgumentException}.
     * </p>
     *
     * @param strictKeys if we should fail when our RHS TableMap does not have the same keySet
     * @param allowCoalesce if we should allow this TableMap to be automatically coalesced into a table
     * @param sanityCheckJoins if we should sanity check join keys, meaning that we should refuse to perform any joins
     *        if the join keys would span two segments of the TableMap. This option is safer, but requires additional
     *        work on the query engine to perform the safety checks.
     * @return a Table object that performs operations by segment
     */
    Table asTable(boolean strictKeys, boolean allowCoalesce, boolean sanityCheckJoins);

    /**
     * Execute asTable with the default builder options.
     *
     * @return a Table object that performs operations by segment
     */
    default Table asTable() {
        return new AsTableBuilder(this).build();
    }

    /**
     * Create a builder object for calling asTable without having to specify all of the parameters.
     *
     * @return an AsTableBuilder
     */
    default AsTableBuilder asTableBuilder() {
        return new AsTableBuilder(this);
    }

    /**
     * Builder object for a TableMapProxy.
     * <p>
     * By default strict keys and join sanity check are enabled; but coalescing is not. This gives you the safest
     * possible asTable call.
     */
    class AsTableBuilder {
        private final TransformableTableMap transformableTableMap;

        private boolean strictKeys = true;
        private boolean allowCoalesce = false;
        private boolean sanityCheckJoins = true;

        /**
         * Create a builder object for the provided TransformableTableMap.
         *
         * @param transformableTableMap the TransformableTableMap to convert into a Table
         */
        public AsTableBuilder(TransformableTableMap transformableTableMap) {
            this.transformableTableMap = transformableTableMap;
        }

        /**
         * Create a Table object using this builder's current parameters.
         *
         * @return a Table object using this builder's current parameters
         */
        public Table build() {
            return transformableTableMap.asTable(strictKeys, allowCoalesce, sanityCheckJoins);
        }

        /**
         * Set if operations should fail when our RHS TableMap does not have the same keySet.
         *
         * <p>
         * True by default.
         * </p>
         *
         * @param strictKeys if operations should fail when our RHS TableMap does not have the same keySet
         * @return this builder
         */
        public AsTableBuilder strictKeys(boolean strictKeys) {
            this.strictKeys = strictKeys;
            return this;
        }

        /**
         * Set if operations should allow this TableMap to be automatically coalesced into a table.
         *
         * <p>
         * False by default.
         * </p>
         *
         * @param allowCoalesce if operations should allow this TableMap to be automatically coalesced into a table
         * @return this builder
         */
        public AsTableBuilder allowCoalesce(boolean allowCoalesce) {
            this.allowCoalesce = allowCoalesce;
            return this;
        }

        /**
         * Set if join operations should include additional sanity checking.
         *
         * <p>
         * True by default.
         * </p>
         *
         * @param sanityCheckJoins if we should sanity check join keys, meaning that we should refuse to perform any
         *        joins if the join keys would span two segments of the TableMap. This option is safer, but requires
         *        additional work on the query engine to perform the safety checks.
         * @return this builder
         */
        public AsTableBuilder sanityCheckJoin(boolean sanityCheckJoins) {
            this.sanityCheckJoins = sanityCheckJoins;
            return this;
        }
    }
}
