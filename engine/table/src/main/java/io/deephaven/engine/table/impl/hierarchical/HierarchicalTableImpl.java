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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Base result class for operations that produce hierarchical tables, for example {@link Table#rollup rollup} and
 * {@link Table#tree(String, String) tree}.
 */
abstract class HierarchicalTableImpl<IFACE_TYPE extends HierarchicalTable<IFACE_TYPE>, IMPL_TYPE extends HierarchicalTableImpl<IFACE_TYPE, IMPL_TYPE>>
        extends BaseGridAttributes<IFACE_TYPE, IMPL_TYPE>
        implements HierarchicalTable<IFACE_TYPE> {

    /**
     * The source table that operations were applied to in order to produce this hierarchical table.
     */
    final QueryTable source;

    /**
     * The root node of the hierarchy.
     */
    final QueryTable root;

    protected HierarchicalTableImpl(
            @NotNull final Map<String, Object> initialAttributes,
            @NotNull final QueryTable source,
            @NotNull final QueryTable root) {
        super(initialAttributes);
        this.source = source;
        this.root = root;
    }

    @Override
    public Table getSource() {
        return source;
    }

    @Override
    public Table getRoot() {
        return root;
    }

    IFACE_TYPE noopResult() {
        if (getSource().isRefreshing()) {
            manageWithCurrentScope();
        }
        //noinspection unchecked
        return (IFACE_TYPE) this;
    }

    @Override
    protected void checkAvailableColumns(@NotNull final Collection<String> columns) {
        final Set<String> availableColumns = root.getDefinition().getColumnNameMap().keySet();
        final List<String> missingColumns =
                columns.stream().filter(column -> !availableColumns.contains(column)).collect(Collectors.toList());
        if (!missingColumns.isEmpty()) {
            throw new NoSuchColumnException(availableColumns, missingColumns);
        }
    }

    // TODO-RWC: Be sure to take format columns into account for table definitions. Prune formats applied to both from UI.

    public final class ClientView extends LivenessArtifact {

        /**
         * The "base" HierarchicalTableImpl for this instance, which may be {@code this}. UI-driven user operations
         * should be applied to the base in order to create new results.
         */
        private final IMPL_TYPE base;

        private final IMPL_TYPE hierarchicalTable = null; // TODO-RWC implement client views

        /**
         * The table of keys to expand.
         */
        private final Table keyTable;

        /**
         * Client-driven sorts that were applied to {@code} base
         */
        private final Collection<SortColumn> sorts;
        private final Collection<? extends WhereFilter> filters;

        public ClientView(
                @Nullable final IMPL_TYPE base,
                @NotNull final Table keyTable,
                @NotNull final Collection<SortColumn> sorts,
                @NotNull final Collection<? extends WhereFilter> filters) {
            this.keyTable = keyTable;
            this.sorts = sorts;
            this.filters = filters;
            if (source.isRefreshing()) {
                manage(HierarchicalTableImpl.this);
            }
            if (base == null) {
                // noinspection unchecked
                this.base = (IMPL_TYPE) HierarchicalTableImpl.this;
            } else {
                if (base.getSource().isRefreshing()) {
                    manage(base);
                }
                this.base = base;
            }
        }
    }
}
