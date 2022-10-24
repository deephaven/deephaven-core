/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.SortColumn;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.select.WhereFilter;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * Base result class for operations that produce hierarchical tables, for example {@link Table#rollup rollup} and
 * {@link Table#tree(String, String) tree}.
 */
public abstract class BaseHierarchicalTable<IFACE_TYPE extends HierarchicalTable<IFACE_TYPE>, IMPL_TYPE extends BaseHierarchicalTable<IFACE_TYPE, IMPL_TYPE>>
        extends BaseGridAttributes<IFACE_TYPE, IMPL_TYPE>
        implements HierarchicalTable<IFACE_TYPE> {

    /**
     * The source table that operations were applied to in order to produce this hierarchical table.
     */
    private final QueryTable sourceTable;

    /**
     * The root node of the hierarchy.
     */
    private final QueryTable rootTable;

    /**
     * The "base" BaseHierarchicalTable for this instance, which may be {@code this}. UI-driven user operations should
     * be applied to the base in order to create new results.
     */
    private final IMPL_TYPE base;

    protected BaseHierarchicalTable(
            @NotNull final QueryTable sourceTable,
            @NotNull final QueryTable rootTable,
            @Nullable final IMPL_TYPE base) {
        super(base == null ? null : base.getAttributes());
        this.sourceTable = sourceTable;
        this.rootTable = rootTable;
        if (sourceTable.isRefreshing()) {
            Assert.assertion(rootTable.isRefreshing(), "rootTable.isRefreshing()");
            manage(sourceTable);
        }
        if (rootTable.isRefreshing()) {
            manage(rootTable);
        }
        if (base == null) {
            //noinspection unchecked
            this.base = (IMPL_TYPE) this;
        } else {
            manage(base);
            this.base = base;
        }
    }

// TODO-RWC: Be sure to take format columns into account for table definitions. Prune formats appied to both for leafs?

    public final class ClientView extends LivenessArtifact {
        final Collection<SortColumn> sorts;
        final Collection<WhereFilter> filters;

        public ClientView(Collection<SortColumn> sorts, Collection<WhereFilter> filters) {
            this.sorts = sorts;
            this.filters = filters;
        }


    }

    /**
     * Create a BaseHierarchicalTable from the specified root (top level) table and {@link HierarchicalTableInfo info}
     * that describes the hierarchy type.
     *
     * @param rootTable the root table of the hierarchy
     * @param info the info that describes the hierarchy type
     *
     * @return A new Hierarchical table. The table itself is a view of the root of the hierarchy.
     */
    @NotNull
    static BaseHierarchicalTable createFrom(@NotNull QueryTable rootTable, @NotNull HierarchicalTableInfo info) {
        final Mutable<BaseHierarchicalTable> resultHolder = new MutableObject<>();

        final SwapListener swapListener =
                rootTable.createSwapListenerIfRefreshing(SwapListener::new);
        initializeWithSnapshot("-hierarchicalTable", swapListener, (usePrev, beforeClockValue) -> {
            final BaseHierarchicalTable table = new BaseHierarchicalTable(sourceTable, rootTable, info);
            rootTable.copyAttributes(table, a -> true);

            if (swapListener != null) {
                final ListenerImpl listener =
                        new ListenerImpl("hierarchicalTable()", rootTable, table);
                swapListener.setListenerAndResult(listener, table);
                table.addParentReference(swapListener);
            }

            resultHolder.setValue(table);
            return true;
        });

        return resultHolder.getValue();
    }
}
