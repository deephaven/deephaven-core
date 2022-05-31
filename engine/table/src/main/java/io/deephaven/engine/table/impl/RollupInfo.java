package io.deephaven.engine.table.impl;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.select.SelectColumn;

import java.util.*;
import java.util.stream.Collectors;

public class RollupInfo extends AbstractHierarchicalTableInfo {
    public static final String ROLLUP_COLUMN = "__RollupHierarchicalColumn";
    private static final long serialVersionUID = 5L;

    public final List<MatchPair> matchPairs;
    public final LeafType leafType;
    public final Set<String> byColumnNames;

    public final transient Collection<? extends Aggregation> aggregations;
    public final transient SelectColumn[] selectColumns;

    /**
     * The type of leaf nodes the rollup has.
     */
    public enum LeafType {
        /** The leaf tables are part of the normal rollup, column types and names will match */
        Normal,

        /**
         * The leaf tables are from the original table (they show constituent rows) and may have different column names
         * and types
         */
        Constituent
    }

    public RollupInfo(Collection<? extends Aggregation> aggregations, SelectColumn[] selectColumns, LeafType leafType) {
        this(aggregations, selectColumns, leafType, null);
    }

    public RollupInfo(Collection<? extends Aggregation> aggregations, SelectColumn[] selectColumns, LeafType leafType,
            String[] columnFormats) {
        super(columnFormats);
        this.aggregations = aggregations;
        this.selectColumns = selectColumns;
        this.matchPairs = AggregationPairs.of(aggregations).map(MatchPair::of).collect(Collectors.toList());
        this.leafType = leafType;

        final Set<String> tempSet = Arrays.stream(selectColumns).map(SelectColumn::getName)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        this.byColumnNames = Collections.unmodifiableSet(tempSet);
    }

    /**
     * Get a copy of the SelectColumns, that can be used for performing a new aggregation.
     *
     * @return a copy of selectColumns
     */
    public SelectColumn[] getSelectColumns() {
        final SelectColumn[] copiedColumns = new SelectColumn[selectColumns.length];
        for (int ii = 0; ii < selectColumns.length; ++ii) {
            copiedColumns[ii] = selectColumns[ii].copy();
        }
        return copiedColumns;
    }

    /**
     * Get the names of the SelectColumns.
     */
    public Set<String> getSelectColumnNames() {
        return byColumnNames;
    }

    @Override
    public String getHierarchicalColumnName() {
        return ROLLUP_COLUMN;
    }

    @Override
    public HierarchicalTableInfo withColumnFormats(String[] columnFormats) {
        return new RollupInfo(aggregations, selectColumns, leafType, columnFormats);
    }

    public LeafType getLeafType() {
        return leafType;
    }

    public List<MatchPair> getMatchPairs() {
        return matchPairs;
    }

    @Override
    public boolean includesConstituents() {
        return leafType == LeafType.Constituent;
    }
}

