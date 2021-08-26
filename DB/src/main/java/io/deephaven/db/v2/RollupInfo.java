package io.deephaven.db.v2;

import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.by.ComboAggregateFactory;
import io.deephaven.db.v2.select.SelectColumn;

import java.util.*;
import java.util.stream.Collectors;

public class RollupInfo extends AbstractHierarchicalTableInfo {
    public static final String ROLLUP_COLUMN = "__RollupHierarchicalColumn";
    private static final long serialVersionUID = 5L;

    public final List<MatchPair> matchPairs;
    public final LeafType leafType;
    public final Set<String> byColumnNames;

    public final transient ComboAggregateFactory factory;
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

    public RollupInfo(ComboAggregateFactory factory, SelectColumn[] selectColumns, LeafType leafType) {
        this(factory, selectColumns, leafType, null);
    }

    public RollupInfo(ComboAggregateFactory factory, SelectColumn[] selectColumns, LeafType leafType,
            String[] columnFormats) {
        super(columnFormats);
        this.factory = factory;
        this.selectColumns = selectColumns;
        this.matchPairs = factory.getMatchPairs();
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
        return new RollupInfo(factory, selectColumns, leafType, columnFormats);
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

