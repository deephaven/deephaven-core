package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.impl.MemoizedOperationKey;
import io.deephaven.engine.table.impl.select.WhereFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.Collection;

/**
 * Immutable client "view" state for a {@link HierarchicalTableImpl}.
 */
public final class ClientView<IFACE_TYPE extends HierarchicalTable<IFACE_TYPE>, IMPL_TYPE extends HierarchicalTableImpl<IFACE_TYPE, IMPL_TYPE>>
        extends LivenessArtifact
        implements MemoizedOperationKey.Provider {

    /**
     * The "base" HierarchicalTableImpl for this instance, which may be {@code HierarchicalTableImpl.this}.
     * Client operations should be applied to the base in order to create new results.
     */
    private final IMPL_TYPE base;

    /**
     * The "effective" HierarchicalTableImpl, which may be
     */
    private final IMPL_TYPE effective;

    /**
     * The table of keys to expand.
     */
    private final Table keyTable;

    /**
     * Name for an optional column of booleans that specifiy whether descendants of an expanded node should be
     * expanded by default instead of the typical contracted by default.
     */
    private final ColumnName keyTableExpandDescendantsColumn;

    /**
     * Client-driven sorts that were applied to {@link #base} to produce {@link #effective}.
     */
    private final Collection<SortColumn> sorts;

    /**
     * Client-driven filters that were applied to {@link #base} to produce {@link #effective}.
     */
    private final Collection<? extends WhereFilter> filters;

    /**
     * Re-usable {@link MemoizedOperationKey} for use in applying node operations.
     */
    private final MemoizedOperationKey memoKey = new MemoKey(this);

    public ClientView(
            @NotNull final IMPL_TYPE base,
            @NotNull final IMPL_TYPE effective,
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableExpandDescendantsColumn,
            @NotNull final Collection<SortColumn> sorts,
            @NotNull final Collection<? extends WhereFilter> filters) {
        this.base = base;
        this.effective = effective;
        this.keyTable = keyTable;
        this.keyTableExpandDescendantsColumn = keyTableExpandDescendantsColumn;
        final Class<?> expandDescendantsDataType = keyTableExpandDescendantsColumn == null ? null :
                keyTable.getDefinition().<Boolean>getColumn(keyTableExpandDescendantsColumn.name()).getDataType();
        if (expandDescendantsDataType != null && expandDescendantsDataType != Boolean.class) {
            throw new IllegalArgumentException("Invalid data type " + expandDescendantsDataType
                    + " for expand descendants column " + keyTableExpandDescendantsColumn.name()
                    + ", expected " + Boolean.class);
        }
        this.sorts = sorts;
        this.filters = filters;

        if (base.getSource().isRefreshing()) {
            manage(base);
            if (base != effective) {
                Assert.assertion(effective.getSource().isRefreshing(), "base and effective refreshing must match");
                manage(effective);
            }
        }
    }

    @Override
    public MemoizedOperationKey getMemoKey() {
        return memoKey;
    }

    private static final class MemoKey extends MemoizedOperationKey {

        private final WeakReference<ClientView> clientView;
        private final int cachedHashCode;

        private MemoKey(@NotNull final ClientView clientView) {
            this.clientView = new WeakReference<>(clientView);
            cachedHashCode = System.identityHashCode(clientView);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            final MemoKey otherMemoKey = (MemoKey) other;
            return equalWeakRefsByReferentIdentity(clientView, otherMemoKey.clientView);
        }

        @Override
        public int hashCode() {
            return cachedHashCode;
        }
    }
}
