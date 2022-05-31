/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.join;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.ListenerRecorder;

/**
 * This is a listener that writes down the indices that were updated on a given clock cycle, and then notifies another
 * listener. The intention is that you would have two of these, one for the left side and another for the right side of
 * the join. The ListenerRecorders are created before the MergedJoinListener, so that they are always fired first in the
 * priority queue. Once the MergedJoinListener is fired, it can examine the indices that were recorded into added and
 * removed, and thus know what has changed on the left, and also what has changed on the right at the same time to
 * produce a consistent output table.
 */
public class JoinListenerRecorder extends ListenerRecorder {

    public JoinListenerRecorder(boolean isLeft, final String description, Table parent, BaseTable dependent) {
        super(isLeft ? "leftTable: " : "rightTable: " + description, parent, dependent);
    }
}
