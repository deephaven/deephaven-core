/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.WouldMatchPair;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.regioned.SymbolTableSource;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.*;

/**
 * Indices for memoized operations on QueryTable.
 *
 * When a null key is returned from one of the static methods; the operation will not be memoized (e.g., if we might
 * depend on the query scope; we can't memoize the operation).
 */
public abstract class MemoizedOperationKey {
    /**
     * Returns true if the attributes are compatible for this operation. If two table are identical, but for attributes
     * many of the operations can be reused. In cases where they can, it would be wasteful to reapply them.
     *
     * @param oldAttributes the attributes on the table that already is memoized
     * @param newAttributes the attributes on the table this is not yet memoized
     *
     * @return true if the attributes are compatible for this operation.
     */
    boolean attributesCompatible(Map<String, Object> oldAttributes, Map<String, Object> newAttributes) {
        // this is the safe default
        return false;
    }

    /**
     * Returns the Copy type for this operation.
     *
     * @return the attribute copy type that should be used when transferring a memoized table across a copy.
     */
    BaseTable.CopyAttributeOperation copyType() {
        throw new UnsupportedOperationException();
    }

    BaseTable.CopyAttributeOperation getParentCopyType() {
        return BaseTable.CopyAttributeOperation.None;
    }

    abstract static class AttributeAgnosticMemoizedOperationKey extends MemoizedOperationKey {
        @Override
        boolean attributesCompatible(Map<String, Object> oldAttributes, Map<String, Object> newAttributes) {
            return true;
        }

        @Override
        abstract BaseTable.CopyAttributeOperation copyType();
    }

    public interface Provider {
        MemoizedOperationKey getMemoKey();
    }

    static MemoizedOperationKey selectUpdateViewOrUpdateView(SelectColumn[] selectColumn,
            SelectUpdateViewOrUpdateView.Flavor flavor) {
        if (isMemoizable(selectColumn)) {
            return new SelectUpdateViewOrUpdateView(selectColumn, flavor);
        } else {
            return null;
        }
    }

    public static MemoizedOperationKey flatten() {
        return Flatten.FLATTEN_INSTANCE;
    }

    static MemoizedOperationKey sort(SortPair[] sortPairs) {
        return new Sort(sortPairs);
    }

    static MemoizedOperationKey dropColumns(String[] columns) {
        return new DropColumns(columns);
    }

    static MemoizedOperationKey filter(WhereFilter[] filters) {
        if (Arrays.stream(filters).allMatch(WhereFilter::canMemoize)) {
            return new Filter(filters);
        }
        return null;
    }

    public static MemoizedOperationKey reverse() {
        return Reverse.REVERSE_INSTANCE;
    }

    public static MemoizedOperationKey tree(String idColumn, String parentColumn) {
        return new Tree(idColumn, parentColumn);
    }

    public static MemoizedOperationKey aggBy(
            Collection<? extends Aggregation> aggregations,
            boolean preserveEmpty,
            Table initialGroups,
            Collection<? extends ColumnName> groupByColumns) {
        return new AggBy(new ArrayList<>(aggregations), preserveEmpty, initialGroups, new ArrayList<>(groupByColumns));
    }

    public static MemoizedOperationKey partitionBy(boolean dropKeys, Collection<? extends ColumnName> groupByColumns) {
        return new PartitionBy(dropKeys, new ArrayList<>(groupByColumns));
    }

    public static MemoizedOperationKey rollup(Collection<? extends Aggregation> aggregations,
            Collection<? extends ColumnName> groupByColumns, boolean includeConstituents) {
        return new Rollup(new AggBy(new ArrayList<>(aggregations), false, null, new ArrayList<>(groupByColumns)),
                includeConstituents);
    }

    private static boolean isMemoizable(SelectColumn[] selectColumn) {
        return Arrays.stream(selectColumn)
                .allMatch(sc -> sc instanceof SourceColumn || sc instanceof ReinterpretedColumn);
    }

    private static class Flatten extends AttributeAgnosticMemoizedOperationKey {
        static final Flatten FLATTEN_INSTANCE = new Flatten();

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Flatten;
        }

        @Override
        BaseTable.CopyAttributeOperation copyType() {
            return BaseTable.CopyAttributeOperation.Flatten;
        }
    }

    static class SelectUpdateViewOrUpdateView extends AttributeAgnosticMemoizedOperationKey {
        public enum Flavor {
            Select, Update, View, UpdateView
        }

        private final SelectColumn[] selectColumns;
        private final Flavor flavor;

        private SelectUpdateViewOrUpdateView(SelectColumn[] selectColumns, Flavor flavor) {
            this.selectColumns = selectColumns;
            this.flavor = flavor;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            final SelectUpdateViewOrUpdateView selectOrView = (SelectUpdateViewOrUpdateView) o;

            return flavor == selectOrView.flavor && Arrays.equals(selectColumns, selectOrView.selectColumns);
        }

        @Override
        public int hashCode() {
            return Objects.hash(flavor, Arrays.hashCode(selectColumns));
        }


        @Override
        BaseTable.CopyAttributeOperation copyType() {
            switch (flavor) {
                case View:
                case UpdateView:
                    return BaseTable.CopyAttributeOperation.View;

                case Select:
                case Update:
                    // turns out select doesn't copy attributes, maybe we should more accurately codify that
                    return BaseTable.CopyAttributeOperation.None;

                default:
                    throw new UnsupportedOperationException("Unexpected flavor " + flavor);
            }
        }
    }

    static class DropColumns extends AttributeAgnosticMemoizedOperationKey {
        private final String[] columns;

        private DropColumns(String[] columns) {
            this.columns = Arrays.copyOf(columns, columns.length);
            Arrays.sort(this.columns);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            final DropColumns dropColumns = (DropColumns) o;

            return Arrays.equals(columns, dropColumns.columns);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(columns);
        }

        @Override
        BaseTable.CopyAttributeOperation copyType() {
            return BaseTable.CopyAttributeOperation.DropColumns;
        }
    }

    private static class Sort extends MemoizedOperationKey {
        private final SortPair[] sortPairs;

        private Sort(SortPair[] sortPairs) {
            this.sortPairs = sortPairs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;

            final Sort sort = (Sort) o;

            return Arrays.equals(sortPairs, sort.sortPairs);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(sortPairs);
        }

        @Override
        boolean attributesCompatible(Map<String, Object> oldAttributes, Map<String, Object> newAttributes) {
            final String parentRestrictions = (String) oldAttributes.get(Table.SORTABLE_COLUMNS_ATTRIBUTE);
            final String newRestrictions = (String) newAttributes.get(Table.SORTABLE_COLUMNS_ATTRIBUTE);

            return Objects.equals(parentRestrictions, newRestrictions);
        }


        @Override
        BaseTable.CopyAttributeOperation copyType() {
            return BaseTable.CopyAttributeOperation.Sort;
        }
    }

    private static class Reverse extends AttributeAgnosticMemoizedOperationKey {
        static final Reverse REVERSE_INSTANCE = new Reverse();

        @Override
        public boolean equals(Object o) {
            return o != null && o.getClass() == getClass();
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        BaseTable.CopyAttributeOperation copyType() {
            return BaseTable.CopyAttributeOperation.Reverse;
        }
    }

    private static class Filter extends AttributeAgnosticMemoizedOperationKey {
        private final WhereFilter[] filters;

        private Filter(WhereFilter[] filters) {
            this.filters = filters;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final Filter filter = (Filter) o;
            return Arrays.equals(filters, filter.filters);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(filters);
        }


        @Override
        BaseTable.CopyAttributeOperation copyType() {
            return BaseTable.CopyAttributeOperation.Filter;
        }
    }

    private static class Tree extends MemoizedOperationKey {
        private final String idColumn;
        private final String parentColumn;


        private Tree(String idColumn, String parentColumn) {
            this.idColumn = idColumn;
            this.parentColumn = parentColumn;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final Tree tree = (Tree) o;
            return Objects.equals(idColumn, tree.idColumn) &&
                    Objects.equals(parentColumn, tree.parentColumn);
        }

        @Override
        public int hashCode() {
            return Objects.hash(idColumn, parentColumn);
        }
    }

    private static class AggBy extends AttributeAgnosticMemoizedOperationKey {

        private final List<? extends Aggregation> aggregations;
        private final boolean preserveEmpty;
        private final WeakReference<Table> initialGroups;
        private final List<? extends ColumnName> groupByColumns;

        private final int cachedHashCode;

        private AggBy(
                List<? extends Aggregation> aggregations,
                boolean preserveEmpty,
                Table initialGroups,
                List<? extends ColumnName> groupByColumns) {
            this.aggregations = aggregations;
            this.preserveEmpty = preserveEmpty;
            this.initialGroups = initialGroups == null ? null : new WeakReference<>(initialGroups);
            this.groupByColumns = groupByColumns;

            int hash = aggregations.hashCode();
            hash = 31 * hash + Boolean.hashCode(preserveEmpty);
            hash = 31 * hash + System.identityHashCode(initialGroups);
            hash = 31 * hash + groupByColumns.hashCode();
            this.cachedHashCode = hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AggBy aggBy = (AggBy) o;
            return aggregations.equals(aggBy.aggregations)
                    && preserveEmpty == aggBy.preserveEmpty
                    && equalWeakRefsByReferentIdentity(initialGroups, aggBy.initialGroups)
                    && groupByColumns.equals(aggBy.groupByColumns);
        }

        @Override
        public int hashCode() {
            return cachedHashCode;
        }

        @Override
        BaseTable.CopyAttributeOperation copyType() {
            return BaseTable.CopyAttributeOperation.None;
        }
    }

    static class PartitionBy extends MemoizedOperationKey {

        private final boolean dropKeys;
        private final List<? extends ColumnName> groupByColumns;

        private PartitionBy(boolean dropKeys, List<? extends ColumnName> groupByColumns) {
            this.dropKeys = dropKeys;
            this.groupByColumns = groupByColumns;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PartitionBy that = (PartitionBy) o;
            return dropKeys == that.dropKeys && groupByColumns.equals(that.groupByColumns);
        }

        @Override
        public int hashCode() {
            int result = (dropKeys ? 1 : 0);
            result = 31 * result + groupByColumns.hashCode();
            return result;
        }
    }

    private static class Rollup extends AttributeAgnosticMemoizedOperationKey {

        private final AggBy aggBy;
        private final boolean includeConstituents;

        Rollup(AggBy aggBy, boolean includeConstituents) {
            this.includeConstituents = includeConstituents;
            this.aggBy = aggBy;
        }

        @Override
        public int hashCode() {
            return 31 * aggBy.hashCode() + Boolean.hashCode(includeConstituents);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Rollup rollup = (Rollup) o;
            return includeConstituents == rollup.includeConstituents && Objects.equals(aggBy, rollup.aggBy);
        }

        @Override
        BaseTable.CopyAttributeOperation copyType() {
            return BaseTable.CopyAttributeOperation.Rollup;
        }

        @Override
        BaseTable.CopyAttributeOperation getParentCopyType() {
            return BaseTable.CopyAttributeOperation.RollupCopy;
        }
    }

    public static MemoizedOperationKey symbolTable(@NotNull final SymbolTableSource symbolTableSource,
            final boolean useLookupCaching) {
        return new SymbolTable(symbolTableSource, useLookupCaching);
    }

    private static final class SymbolTable extends MemoizedOperationKey {

        private final SymbolTableSource symbolTableSource;
        private final boolean useLookupCaching;

        private SymbolTable(@NotNull final SymbolTableSource symbolTableSource, final boolean useLookupCaching) {
            this.symbolTableSource = symbolTableSource;
            this.useLookupCaching = useLookupCaching;
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final SymbolTable that = (SymbolTable) other;
            // NB: We use the symbolTableSource's identity for comparison
            return symbolTableSource == that.symbolTableSource && useLookupCaching == that.useLookupCaching;
        }

        @Override
        public int hashCode() {
            return 31 * System.identityHashCode(symbolTableSource) + Boolean.hashCode(useLookupCaching);
        }
    }

    private static final class WouldMatch extends AttributeAgnosticMemoizedOperationKey {
        private final WouldMatchPair[] pairs;

        private WouldMatch(WouldMatchPair[] pairs) {
            this.pairs = pairs;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null || getClass() != obj.getClass())
                return false;
            final WouldMatch wouldMatch = (WouldMatch) obj;
            return Arrays.equals(pairs, wouldMatch.pairs);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(pairs);
        }

        @Override
        BaseTable.CopyAttributeOperation copyType() {
            return BaseTable.CopyAttributeOperation.WouldMatch;
        }
    }

    public static WouldMatch wouldMatch(WouldMatchPair... pairs) {
        return new WouldMatch(pairs);
    }

    private static class CrossJoin extends AttributeAgnosticMemoizedOperationKey {
        private final WeakReference<Table> rightTableCandidate;
        private final MatchPair[] columnsToMatch;
        private final MatchPair[] columnsToAdd;
        private final int numRightBitsToReserve;
        private final int cachedHashCode;

        CrossJoin(final Table rightTableCandidate, final MatchPair[] columnsToMatch,
                final MatchPair[] columnsToAdd, final int numRightBitsToReserve) {
            this.rightTableCandidate = new WeakReference<>(rightTableCandidate);
            this.columnsToMatch = columnsToMatch;
            this.columnsToAdd = columnsToAdd;
            this.numRightBitsToReserve = numRightBitsToReserve;

            // precompute hash as right table may disappear
            int hash = Integer.hashCode(numRightBitsToReserve);
            hash = 31 * hash + System.identityHashCode(rightTableCandidate);
            hash = 31 * hash + Arrays.hashCode(columnsToMatch);
            hash = 31 * hash + Arrays.hashCode(columnsToAdd);
            this.cachedHashCode = hash;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final CrossJoin crossJoin = (CrossJoin) o;
            return equalWeakRefsByReferentIdentity(rightTableCandidate, crossJoin.rightTableCandidate) &&
                    numRightBitsToReserve == crossJoin.numRightBitsToReserve &&
                    Arrays.equals(columnsToMatch, crossJoin.columnsToMatch) &&
                    Arrays.equals(columnsToAdd, crossJoin.columnsToAdd);
        }

        @Override
        public int hashCode() {
            return cachedHashCode;
        }

        @Override
        BaseTable.CopyAttributeOperation copyType() {
            return BaseTable.CopyAttributeOperation.None;
        }
    }

    public static CrossJoin crossJoin(final Table rightTableCandidate, final MatchPair[] columnsToMatch,
            final MatchPair[] columnsToAdd, final int numRightBitsToReserve) {
        return new CrossJoin(rightTableCandidate, columnsToMatch, columnsToAdd, numRightBitsToReserve);
    }

    private static boolean equalWeakRefsByReferentIdentity(final WeakReference<?> r1, final WeakReference<?> r2) {
        if (r1 == r2) {
            return true;
        }
        if (r1 == null || r2 == null) {
            return false;
        }
        final Object t1 = r1.get();
        final Object t2 = r2.get();
        if (t1 == null || t2 == null) {
            return false;
        }
        return t1 == t2;
    }
}
