/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.by.PartitionByChunkedOperator;

/**
 * Copies attributes for constituent leafs or intermediate level rollups.
 */
public class RollupAttributeCopier {
    /**
     * When creating constituent leaves, we set the appropriate TableMap and reverse lookup on each leaf we are
     * creating.
     */
    public final static PartitionByChunkedOperator.AttributeCopier LEAF_WITHCONSTITUENTS_INSTANCE = (pt, st) -> {
        pt.copyAttributes(st, BaseTable.CopyAttributeOperation.PartitionBy);
        st.setAttribute(Table.ROLLUP_LEAF_ATTRIBUTE, RollupInfo.LeafType.Constituent);
        st.setAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_ATTRIBUTE,
                // TODO (https://github.com/deephaven/deephaven-core/issues/65):
                // Make rollups work with partitioned tables instead of table maps. Empty PartitionedTable here?
                "placeholder");
        st.setAttribute(Table.AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE, ReverseLookup.NULL);
    };

    /** For intermediate levels, we must copy the reverse lookup from the deeper level. */
    public final static PartitionByChunkedOperator.AttributeCopier DEFAULT_INSTANCE = (pt, st) -> {
        pt.copyAttributes(st, BaseTable.CopyAttributeOperation.PartitionBy);
        Object reverseLookup = pt.getAttribute(Table.AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE);
        if (reverseLookup != null) {
            st.setAttribute(Table.AGGREGATION_RESULT_ROW_LOOKUP_ATTRIBUTE, reverseLookup);
        }
    };

    private RollupAttributeCopier() {}
}
