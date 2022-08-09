/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.AggregationPairs;
import io.deephaven.engine.table.MatchPair;

import java.util.*;
import java.util.stream.Collectors;

public class RollupInfo extends AbstractHierarchicalTableInfo {
    public static final String ROLLUP_COLUMN = "__RollupHierarchicalColumn";
    private static final long serialVersionUID = 5L;

    public final List<MatchPair> matchPairs;
    public final LeafType leafType;
    public final Set<String> byColumnNames;

    public final transient Collection<? extends Aggregation> aggregations;
    public final transient Collection<? extends ColumnName> groupByColumns;

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

    public RollupInfo(Collection<? extends Aggregation> aggregations, Collection<? extends ColumnName> groupByColumns,
            LeafType leafType) {
        this(aggregations, groupByColumns, leafType, null);
    }

    public RollupInfo(Collection<? extends Aggregation> aggregations, Collection<? extends ColumnName> groupByColumns,
            LeafType leafType,
            String[] columnFormats) {
        super(columnFormats);
        this.aggregations = aggregations;
        this.groupByColumns = groupByColumns;
        this.matchPairs = AggregationPairs.of(aggregations).map(MatchPair::of).collect(Collectors.toList());
        this.leafType = leafType;
        final Set<String> tempSet =
                groupByColumns.stream().map(ColumnName::name).collect(Collectors.toCollection(LinkedHashSet::new));
        this.byColumnNames = Collections.unmodifiableSet(tempSet);
    }

    /**
     * Get a copy of the SelectColumns, that can be used for performing a new aggregation.
     *
     * @return a copy of selectColumns
     */
    public Collection<? extends ColumnName> getGroupByColumns() {
        return groupByColumns;
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
        return new RollupInfo(aggregations, groupByColumns, leafType, columnFormats);
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

