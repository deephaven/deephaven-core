package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Pair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.QueryTable;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.UnaryOperator;

/**
 * {@link RollupTable} implementation.
 */
public class RollupTableImpl extends BaseHierarchicalTable<RollupTable, RollupTableImpl> implements RollupTable{

    protected RollupTableImpl(@NotNull final QueryTable source, @NotNull final QueryTable root) {
        super(source, root);
    }

    @Override
    public Collection<? extends Aggregation> getAggregations() {
        return null;
    }

    @Override
    public boolean includesConstituents() {
        return false;
    }

    @Override
    public Collection<? extends ColumnName> getGroupByColumns() {
        return null;
    }

    @Override
    public ColumnName getKeyWidthColumn() {
        return null;
    }

    @Override
    public ColumnName getRollupColumn() {
        return null;
    }

    @Override
    public Collection<? extends Pair> getColumnPairs() {
        return null;
    }

    @Override
    public NodeOperationsRecorder makeNodeOperationsRecorder(@NotNull NodeType nodeType) {
        return null;
    }

    @Override
    public RollupTable withNodeOperations(@NotNull NodeOperationsRecorder nodeOperations) {
        return null;
    }

    @Override
    public RollupTable reapply(@NotNull UnaryOperator<Table> sourceTransformer) {
        return null;
    }

    @Override
    protected void checkAvailableColumns(@NotNull Collection<String> columns) {

    }

    @Override
    protected RollupTableImpl copy() {
        return null;
    }
}
