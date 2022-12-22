/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.shared.data.treetable;

import io.deephaven.web.shared.data.FilterDescriptor;
import io.deephaven.web.shared.data.SortDescriptor;
import io.deephaven.web.shared.data.TableHandle;

import java.io.Serializable;
import java.util.Arrays;
import java.util.BitSet;

public class TreeTableRequest implements Serializable {
    private long viewportStart;
    private long viewportEnd;
    private BitSet columns;

    public long getViewportStart() {
        return viewportStart;
    }

    public void setViewportStart(long viewportStart) {
        this.viewportStart = viewportStart;
    }

    public long getViewportEnd() {
        return viewportEnd;
    }

    public void setViewportEnd(long viewportEnd) {
        this.viewportEnd = viewportEnd;
    }

    public BitSet getColumns() {
        return columns;
    }

    public void setColumns(BitSet columns) {
        this.columns = columns;
    }

    @Override
    public String toString() {
        return "TreeTableRequest{" +
                "viewportStart=" + viewportStart +
                ", viewportEnd=" + viewportEnd +
                ", columns=" + columns +
                '}';
    }
}
