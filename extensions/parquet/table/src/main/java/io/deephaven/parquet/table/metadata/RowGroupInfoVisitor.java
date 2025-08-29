//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import org.jetbrains.annotations.NotNull;

public class RowGroupInfoVisitor implements RowGroupInfo.Visitor<RowGroupInfo> {
    @Override
    public RowGroupInfo visit(final @NotNull RowGroupInfo.SingleRowGroup single) {
        return RowGroupInfo.singleRowGroup();
    }

    @Override
    public RowGroupInfo visit(final @NotNull RowGroupInfo.SplitEvenly splitEvenly) {
        return RowGroupInfo.splitEvenly(splitEvenly.getNumRowGroups());
    }

    @Override
    public RowGroupInfo visit(final @NotNull RowGroupInfo.SplitByMaxRows withMaxRows) {
        return RowGroupInfo.withMaxRows(withMaxRows.getMaxRows());
    }

    @Override
    public RowGroupInfo visit(final @NotNull RowGroupInfo.SplitByGroups byGroups) {
        return RowGroupInfo.byGroup(byGroups.getMaxRows(), byGroups.getGroups());
    }

    @Override
    public RowGroupInfo visit(final @NotNull RowGroupInfo generic) {
        if (generic instanceof RowGroupInfo.SingleRowGroup) {
            return visit((RowGroupInfo.SingleRowGroup) generic);
        } else if (generic instanceof RowGroupInfo.SplitEvenly) {
            return visit((RowGroupInfo.SplitEvenly) generic);
        } else if (generic instanceof RowGroupInfo.SplitByMaxRows) {
            return visit((RowGroupInfo.SplitByMaxRows) generic);
        } else if (generic instanceof RowGroupInfo.SplitByGroups) {
            return visit((RowGroupInfo.SplitByGroups) generic);
        } else {
            throw new UnsupportedOperationException(
                    String.format("Unknown %s type", RowGroupInfo.class.getCanonicalName()));
        }

    }
}

