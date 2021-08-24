package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import com.google.common.collect.Iterators;
import io.deephaven.db.v2.sources.ColumnSource;
import gnu.trove.impl.Constants;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.BitSet;
import java.util.Map;

/**
 * Data structure to represent a subset of columns, used for propagating modifications via
 * {@link ShiftAwareListener.Update} instances.
 */
public class ModifiedColumnSet {

    private ModifiedColumnSet() {
        columns = null;
        columnNames = null;
        idMap = null;
        dirtyColumns = null;
    }

    /**
     * A static 'special' ModifiedColumnSet that pretends all columns are dirty. Useful for
     * backwards compatibility and convenience.
     */
    public static final ModifiedColumnSet ALL = new ModifiedColumnSet() {
        @Override
        public Transformer newTransformer(String[] columnNames, ModifiedColumnSet[] columnSets) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BitSet extractAsBitSet() {
            throw new UnsupportedOperationException(
                "Cannot extract BitSet when number of columns is unknown.");
        }

        @Override
        public ModifiedColumnSet copy() {
            return this;
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAllDirty() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return 1; // must simply be non-empty
        }

        @Override
        public boolean empty() {
            return false;
        }

        @Override
        public int numColumns() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAll(String[] columnNames) {
            // no-op; they are already set
        }

        @Override
        public void setAll(ModifiedColumnSet columnSet) {
            // no-op; they are already set
        }

        @Override
        public void setColumnWithIndex(int columnIndex) {
            // no-op; it is already set
        }

        @Override
        public void clearAll(String[] columnNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearAll(ModifiedColumnSet columnSet) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAny(ModifiedColumnSet columnSet) {
            return columnSet.size() > 0;
        }

        @Override
        public boolean containsAll(ModifiedColumnSet columnSet) {
            return columnSet.equals(this);
        }

        @Override
        public String toString() {
            return "{ALL}";
        }

        @Override
        public String toString(int maxColumns) {
            return "{ALL}";
        }
    };

    /**
     * A static 'special' ModifiedColumnSet that pretends no columns are dirty.
     */
    public static final ModifiedColumnSet EMPTY = new ModifiedColumnSet() {
        @Override
        public Transformer newTransformer(String[] columnNames, ModifiedColumnSet[] columnSets) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BitSet extractAsBitSet() {
            return new BitSet(0);
        }

        @Override
        public ModifiedColumnSet copy() {
            return this;
        }

        @Override
        public void clear() {
            // empty is always clear
        }

        @Override
        public void setAllDirty() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean empty() {
            return true;
        }

        @Override
        public int numColumns() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAll(String[] columnNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setAll(ModifiedColumnSet columnSet) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setColumnWithIndex(int columnIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearAll(String[] columnNames) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clearAll(ModifiedColumnSet columnSet) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAny(ModifiedColumnSet columnSet) {
            return false;
        }

        @Override
        public boolean containsAll(ModifiedColumnSet columnSet) {
            return true;
        }

        @Override
        public String toString() {
            return "{EMPTY}";
        }

        @Override
        public String toString(int maxColumns) {
            return "{EMPTY}";
        }
    };

    // TODO: combine TableDefinition, ColumnSourceMap, ColumnNames, and IdMap into reusable shared
    // object state.
    // We'll use this to fail-fast when two incompatible MCSs interact.
    private final Map<String, ColumnSource> columns;
    private final String[] columnNames;
    private final TObjectIntHashMap<String> idMap;

    // Represents which columns are dirty.
    private final BitSet dirtyColumns;

    public BitSet extractAsBitSet() {
        return (BitSet) dirtyColumns.clone();
    }

    /**
     * A helper utility that simplifies propagating modified columns to a child table.
     */
    public interface Transformer {
        /**
         * Propagates changes from one {@link ModifiedColumnSet} to another ModifiedColumnSet that
         * contextually represent different tables. Clears the output set prior to transforming.
         * 
         * @param input source table's columns that changed
         * @param output result table's columns to propagate dirty columns to
         */
        default void clearAndTransform(final ModifiedColumnSet input,
            final ModifiedColumnSet output) {
            output.clear();
            transform(input, output);
        }

        /**
         * Propagates changes from {@code input} {@link ModifiedColumnSet} to {@code output}
         * ModifiedColumnSet. Does not clear the {@code output} before propagating.
         * 
         * @param input source table's columns that changed (null implies no modified columns)
         * @param output result table's columns to propagate dirty columns to
         */
        default void transform(final ModifiedColumnSet input, final ModifiedColumnSet output) {
            if (input == ALL) {
                output.setAllDirty();
                return;
            }
            if (input == null || input.empty()) {
                return;
            }
            transformLambda(input, output);
        }

        /**
         * Do not invoke directly.
         */
        void transformLambda(final ModifiedColumnSet input, final ModifiedColumnSet output);
    }

    /**
     * Create an empty ModifiedColumnSet from the provided Column Source Map. Note: prefer to use
     * the copy constructor on future objects that share this CSM to minimize duplicating state.
     * 
     * @param columns The column source map this ModifiedColumnSet will use.
     */
    public ModifiedColumnSet(final Map<String, ColumnSource> columns) {
        this.columns = columns;
        columnNames = columns.keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
        idMap = new TObjectIntHashMap<>(columnNames.length, Constants.DEFAULT_LOAD_FACTOR, -1);
        for (int i = 0; i < columnNames.length; ++i) {
            idMap.put(columnNames[i], i);
        }
        dirtyColumns = new BitSet(columnNames.length);
    }

    /**
     * Create a new ModifiedColumnSet that shares all immutable state with the provided MCS. The
     * dirty set will initially be empty.
     * 
     * @param other The ModifiedColumnSet to borrow immutable state from.
     */
    public ModifiedColumnSet(final ModifiedColumnSet other) {
        if (other == ModifiedColumnSet.ALL || other == ModifiedColumnSet.EMPTY) {
            throw new IllegalArgumentException("Cannot base a new ModifiedColumnSet on ALL/EMPTY.");
        }
        columns = other.columns;
        columnNames = other.columnNames;
        idMap = other.idMap;

        dirtyColumns = new BitSet(columnNames.length);
    }

    /**
     * Create a transformer that is compatible with the class of ModifiedColumnSets that share a
     * ColumnSourceMap.
     * 
     * @param columnNames input columns to check for dirtiness
     * @param columnSets output columns to mark dirty when input column is dirty
     * @return a new Transformer instance
     */
    public Transformer newTransformer(final String[] columnNames,
        final ModifiedColumnSet[] columnSets) {
        Assert.eq(columnNames.length, "columnNames.length", columnSets.length, "columnSets.length");
        final int[] columnBits = new int[columnNames.length];
        for (int i = 0; i < columnNames.length; ++i) {
            final int bitIndex = idMap.get(columnNames[i]);
            if (bitIndex == idMap.getNoEntryValue()) {
                throw new IllegalArgumentException(
                    "Unknown column while constructing ModifiedColumnSet: " + columnNames[i]);
            }
            columnBits[i] = bitIndex;
            Assert.eq(columnSets[0].columns, "columnSets[0].columns", columnSets[i].columns,
                "columnSets[i].columns");
        }

        return (input, output) -> {
            verifyCompatibilityWith(input);
            for (int i = 0; i < columnBits.length; ++i) {
                if (input.dirtyColumns.get(columnBits[i])) {
                    output.setAll(columnSets[i]);
                }
            }
        };
    }

    /**
     * Create a transformer that uses an identity mapping from one ColumnSourceMap to another. The
     * two CSM's must have equivalent column names and column ordering.
     * 
     * @param newColumns the column source map for result table
     * @return a simple Transformer that makes a cheap, but CSM compatible copy
     */
    public Transformer newIdentityTransformer(final Map<String, ColumnSource> newColumns) {
        if (columns == newColumns) {
            throw new IllegalArgumentException(
                "Do not use a transformer when it is correct to pass-through the ModifiedColumnSet.");
        } else if (!Iterators.elementsEqual(columns.keySet().iterator(),
            newColumns.keySet().iterator())) {
            throw new IllegalArgumentException(
                "Result column names are incompatible with parent column names.");
        }

        return (input, output) -> {
            if (input.columns != columns || output.columns != newColumns) {
                throw new IllegalArgumentException(
                    "Provided ModifiedColumnSets are not compatible with this Transformer!");
            }
            output.dirtyColumns.or(input.dirtyColumns);
        };
    }

    /**
     * @return whether or not this modified column set is compatible with the provided set
     */
    public boolean isCompatibleWith(final ModifiedColumnSet columnSet) {
        if (this == ModifiedColumnSet.ALL || this == ModifiedColumnSet.EMPTY
            || columnSet == ModifiedColumnSet.ALL || columnSet == ModifiedColumnSet.EMPTY) {
            return true;
        }
        // They are compatible iff column names and column orders are identical. To be cheaper
        // though, we're
        // not going to compare those - we'll be stricter and require that they are both actually
        // the same
        // instance.
        return columns == columnSet.columns;
    }

    /**
     * This method is used to determine whether or not a dependent requires a transformer to
     * propagate dirty columns from its parent. If no transformer is required then it is acceptable
     * to reuse any column set provided by the parent. Note this is intended to be determined during
     * initialization and never during an update cycle. It is illegal to use the specialized
     * ModifiedColumnSet.EMPTY / ModifiedColumnSet.ALL as their innards do not represent any table.
     * 
     * @param columnSet the column set for the dependent table
     * @return whether or not this modified column set must use a Transformer to propagate modified
     *         columns
     */
    public boolean requiresTransformer(final ModifiedColumnSet columnSet) {
        if (this == ModifiedColumnSet.ALL || this == ModifiedColumnSet.EMPTY
            || columnSet == ModifiedColumnSet.ALL || columnSet == ModifiedColumnSet.EMPTY) {
            throw new IllegalArgumentException(
                "The ALL/EMPTY ModifiedColumnSets are not indicative of propagation compatibility.");
        }
        // They are propagation compatible iff column names and column orders are identical;
        // otherwise requires transformer.
        return columns != columnSet.columns;
    }

    /**
     * Create an exact copy of this ModifiedColumnSet.
     *
     * @return a copy with identical state including dirty bits
     */
    public ModifiedColumnSet copy() {
        final ModifiedColumnSet retVal = new ModifiedColumnSet(this);
        retVal.setAll(this);
        return retVal;
    }

    /**
     * Reset the current dirty column state.
     */
    public void clear() {
        dirtyColumns.clear();
    }

    /**
     * Sets all columns dirty.
     */
    public void setAllDirty() {
        dirtyColumns.set(0, columnNames.length);
    }

    /**
     * @return the number of dirty columns
     */
    public int size() {
        return dirtyColumns.cardinality();
    }

    /**
     * @return whether or not this set is empty
     */
    public boolean empty() {
        return dirtyColumns.isEmpty();
    }

    /**
     * @return whether or not this set is non-empty
     */
    public boolean nonempty() {
        return !empty();
    }

    /**
     * @return the number of columns in this set
     * @throws UnsupportedOperationException on {@link ModifiedColumnSet#ALL} and
     *         {@link ModifiedColumnSet#EMPTY}
     */
    public int numColumns() {
        return columns.size();
    }

    /**
     * Turns on all bits for these columns. Use this method to prepare pre-computed
     * ModifiedColumnSets.
     * 
     * @param columnNames the columns which need to be marked dirty
     */
    public void setAll(final String... columnNames) {
        for (final String column : columnNames) {
            final int bitIndex = idMap.get(column);
            if (bitIndex == idMap.getNoEntryValue()) {
                throw new IllegalArgumentException(
                    "Unknown column while constructing ModifiedColumnSet: " + column);
            }
            this.dirtyColumns.set(bitIndex);
        }
    }

    /**
     * Marks all columns in the provided column set in this set as dirty.
     * 
     * @param columnSet the set of columns to mark dirty
     */
    public void setAll(final ModifiedColumnSet columnSet) {
        if (columnSet == EMPTY) {
            return;
        }
        verifyCompatibilityWith(columnSet);
        if (columnSet == ALL) {
            dirtyColumns.set(0, numColumns());
        } else if (columnSet.nonempty()) {
            dirtyColumns.or(columnSet.dirtyColumns);
        }
    }

    /**
     * Marks specifically the column with the given index as dirty.
     * 
     * @param columnIndex column index to mark dirty
     */
    public void setColumnWithIndex(int columnIndex) {
        dirtyColumns.set(columnIndex);
    }

    /**
     * Marks specifically a range of adjacent columns. Start is inclusive, end is exclusive; like
     * the BitSet API.
     * 
     * @param columnStart start column index to mark dirty
     * @param columnEndExclusive end column index to mark dirty
     */
    public void setColumnsInIndexRange(int columnStart, int columnEndExclusive) {
        dirtyColumns.set(columnStart, columnEndExclusive);
    }

    /**
     * Turns off all bits for these columns. Use this method to prepare pre-computed
     * ModifiedColumnSets.
     * 
     * @param columnNames the columns which need to be marked clean
     */
    public void clearAll(final String... columnNames) {
        for (final String column : columnNames) {
            final int bitIndex = idMap.get(column);
            if (bitIndex == idMap.getNoEntryValue()) {
                throw new IllegalArgumentException(
                    "Unknown column while constructing ModifiedColumnSet: " + column);
            }
            this.dirtyColumns.clear(bitIndex);
        }
    }

    /**
     * Marks all columns in the provided column set in this set as clean.
     * 
     * @param columnSet the set of columns to mark clean
     */
    public void clearAll(final ModifiedColumnSet columnSet) {
        if (columnSet == EMPTY) {
            return;
        }
        verifyCompatibilityWith(columnSet);
        dirtyColumns.andNot(columnSet.dirtyColumns);
    }

    private void verifyCompatibilityWith(final ModifiedColumnSet columnSet) {
        if (!isCompatibleWith(columnSet)) {
            throw new IllegalArgumentException(
                "Provided ModifiedColumnSet is incompatible with this one! " + this.toDebugString()
                    + " vs " + columnSet.toDebugString());
        }
    }

    /**
     * Check whether or not any columns are currently marked as dirty.
     * 
     * @param columnSet the columns to check
     * @return true iff any column is dirty
     */
    public boolean containsAny(final ModifiedColumnSet columnSet) {
        verifyCompatibilityWith(columnSet);
        if (columnSet == EMPTY) {
            return false;
        }
        return dirtyColumns.intersects(columnSet.dirtyColumns);
    }

    /**
     * Check whether or not all columns are currently marked as dirty.
     * 
     * @param columnSet the columns to check
     * @return true iff all columns match dirtiness
     */
    public boolean containsAll(final ModifiedColumnSet columnSet) {
        verifyCompatibilityWith(columnSet);
        if (columnSet.empty() || this == ALL) {
            return true;
        }
        if (columnSet == ALL) {
            return dirtyColumns.cardinality() == numColumns();
        }
        final BitSet copy = (BitSet) columnSet.dirtyColumns.clone();
        copy.andNot(dirtyColumns);
        return copy.isEmpty();
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof ModifiedColumnSet)) {
            return false;
        }
        final ModifiedColumnSet columnSet = (ModifiedColumnSet) other;
        verifyCompatibilityWith(columnSet);
        if (columnSet.empty()) {
            return empty();
        }
        if (this == ALL || columnSet == ALL) {
            return (columnSet == ALL
                || columnSet.dirtyColumns.cardinality() == columnSet.numColumns())
                && (this == ALL || dirtyColumns.cardinality() == numColumns());
        }
        // note: this logic is fine for EMPTY
        return dirtyColumns.equals(columnSet.dirtyColumns);
    }

    public String toDebugString() {
        if (this == EMPTY) {
            return "ModifiedColumnSet.EMPTY";
        }
        if (this == ALL) {
            return "ModifiedColumnSet.ALL";
        }
        StringBuilder sb = new StringBuilder("ModifiedColumnSet: columns=")
            .append(Integer.toHexString(System.identityHashCode(this)))
            .append(", {");

        for (int i = 0; i < columnNames.length; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            sb.append(columnNames[i]);
            if (dirtyColumns.get(i)) {
                sb.append("**");
            }
        }

        return sb.append(" }").toString();
    }

    public String toString() {
        return toString(100);
    }

    public String toString(int maxColumns) {
        int count = 0;

        boolean isFirst = true;
        final StringBuilder result = new StringBuilder("{");

        for (int i = dirtyColumns.nextSetBit(0); i >= 0; i = dirtyColumns.nextSetBit(i + 1)) {
            result.append(isFirst ? "" : ",").append(columnNames[i]);
            isFirst = false;
            if (++count >= maxColumns) {
                result.append(",...");
                break;
            }
        }
        result.append("}");

        return result.toString();
    }
}
