/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import io.deephaven.engine.table.impl.select.WhereFilter;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Function;

/**
 * Base result class for operations that produce hierarchical tables, for example {@link Table#rollup rollup} and
 * {@link Table#tree(String, String) tree}.
 */
public abstract class HierarchicalTable<TYPE extends HierarchicalTable> extends BaseGridAttributes<TYPE, TYPE> {

    /**
     * The source table that operations were applied to in order to produce this hierarchical table.
     */
    private final QueryTable sourceTable;

    /**
     * The root node of the hierarchy.
     */
    private final QueryTable rootTable;

    /**
     * The "base" HierarchicalTable for this instance, which may be {@code this}. User operations should be applied to
     * the base in order to create new results.
     */
    private final TYPE base;

    private HierarchicalTable(
            @NotNull final QueryTable sourceTable,
            @NotNull final QueryTable rootTable,
            @Nullable final TYPE base) {
        this.sourceTable = sourceTable;
        this.rootTable = rootTable;
        if (sourceTable.isRefreshing()) {
            Assert.assertion(rootTable.isRefreshing(),
            manage(sourceTable);
        }
        if (rootTable.isRefreshing()) {
            manage(rootTable);
        }
        if (base == null) {
            //noinspection unchecked
            this.base = (TYPE) this;
        } else {
            manage(base);
            this.base = base;
        }
    }

    @Override
    public Table formatColumns(String... columnFormats) {
        final HierarchicalTableInfo hierarchicalTableInfo =
                (HierarchicalTableInfo) getAttribute(HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
        final String[] originalColumnFormats = hierarchicalTableInfo.getColumnFormats();

        final String[] newColumnFormats;
        if (originalColumnFormats != null && originalColumnFormats.length > 0) {
            newColumnFormats =
                    Arrays.copyOf(originalColumnFormats, originalColumnFormats.length + columnFormats.length);
            System.arraycopy(columnFormats, 0, newColumnFormats, originalColumnFormats.length, columnFormats.length);
        } else {
            newColumnFormats = columnFormats;
        }

        // Note that we are not updating the root with the 'newColumnFormats' because the original set of formats
        // are already there.
        final Table updatedRoot = rootTable.updateView(SelectColumnFactory.getFormatExpressions(columnFormats));
        final ReverseLookup maybeRll = (ReverseLookup) rootTable.getAttribute(REVERSE_LOOKUP_ATTRIBUTE);

        // Explicitly need to copy this in case we are a rollup, in which case the RLL needs to be at root level
        if (maybeRll != null) {
            updatedRoot.setAttribute(REVERSE_LOOKUP_ATTRIBUTE, maybeRll);
        }

        final HierarchicalTable result =
                createFrom((QueryTable) updatedRoot, hierarchicalTableInfo.withColumnFormats(newColumnFormats));
        copyAttributes(result, a -> !Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE.equals(a));

        return result;
    }


    public final class ClientView extends LivenessArtifact {
        final Collection<SortColumn> sorts;
        final Collection<WhereFilter> filters;

        public ClientView(Collection<SortColumn> sorts, Collection<WhereFilter> filters) {
            this.sorts = sorts;
            this.filters = filters;
        }


    }

    /**
     * Create a HierarchicalTable from the specified root (top level) table and {@link HierarchicalTableInfo info} that
     * describes the hierarchy type.
     *
     * @param rootTable the root table of the hierarchy
     * @param info the info that describes the hierarchy type
     *
     * @return A new Hierarchical table. The table itself is a view of the root of the hierarchy.
     */
    @NotNull
    static HierarchicalTable createFrom(@NotNull QueryTable rootTable, @NotNull HierarchicalTableInfo info) {
        final Mutable<HierarchicalTable> resultHolder = new MutableObject<>();

        final SwapListener swapListener =
                rootTable.createSwapListenerIfRefreshing(SwapListener::new);
        initializeWithSnapshot("-hierarchicalTable", swapListener, (usePrev, beforeClockValue) -> {
            final HierarchicalTable table = new HierarchicalTable(sourceTable, rootTable, info);
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
