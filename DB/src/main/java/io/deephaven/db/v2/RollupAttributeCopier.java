package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.v2.by.ByExternalChunkedOperator;

/**
 * Copies attributes for constituent leafs or intermediate level rollups.
 */
public class RollupAttributeCopier {
    /**
     * When creating constituent leaves, we set the appropriate TableMap and reverse lookup on each leaf we are
     * creating.
     */
    public final static ByExternalChunkedOperator.AttributeCopier LEAF_WITHCONSTITUENTS_INSTANCE = (pt, st) -> {
        pt.copyAttributes(st, BaseTable.CopyAttributeOperation.ByExternal);
        st.setAttribute(Table.ROLLUP_LEAF_ATTRIBUTE, RollupInfo.LeafType.Constituent);
        st.setAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE, TableMap.emptyMap());
        st.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE, ReverseLookup.NULL);
    };

    /** For intermediate levels, we must copy the reverse lookup from the deeper level. */
    public final static ByExternalChunkedOperator.AttributeCopier DEFAULT_INSTANCE = (pt, st) -> {
        pt.copyAttributes(st, BaseTable.CopyAttributeOperation.ByExternal);
        Object reverseLookup = pt.getAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE);
        if (reverseLookup != null) {
            st.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE, reverseLookup);
        }
    };

    private RollupAttributeCopier() {}
}
