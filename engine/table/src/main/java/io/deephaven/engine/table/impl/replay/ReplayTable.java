/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.replay;

import io.deephaven.base.verify.Require;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;

import java.util.Map;

public class ReplayTable extends QueryTable implements Runnable {


    private final RowSet.Iterator indexIterator;
    private long curr;
    private final ColumnSource<DateTime> timeSource;
    private boolean done;
    private final Replayer replayer;

    public ReplayTable(RowSet rowSet, Map<String, ? extends ColumnSource<?>> result, String timeColumn,
            Replayer replayer) {
        super(RowSetFactory.empty().toTracking(), result);
        Require.requirement(replayer != null, "replayer != null");
        // noinspection unchecked
        replayer.registerTimeSource(rowSet, (ColumnSource<DateTime>) result.get(timeColumn));
        setRefreshing(true);
        indexIterator = rowSet.iterator();
        if (indexIterator.hasNext()) {
            curr = indexIterator.nextLong();
        } else {
            done = true;
        }
        timeSource = getColumnSource(timeColumn);
        this.replayer = replayer;
        run();
    }

    long nextTime = -1;

    @Override
    public void run() {
        if (done) {
            return;
        }
        if (nextTime < 0) {
            nextTime = timeSource.get(curr).getNanos();
        }
        if (done || nextTime >= replayer.currentTimeNanos()) {
            return;
        }
        RowSetBuilderRandom indexBuilder = RowSetFactory.builderRandom();
        while (!done && nextTime < replayer.currentTimeNanos()) {
            indexBuilder.addKey(curr);
            if (indexIterator.hasNext()) {
                curr = indexIterator.nextLong();
                nextTime = timeSource.get(curr).getNanos();
            } else {
                done = true;
            }
        }
        final RowSet added = indexBuilder.build();
        if (added.size() > 0) {
            getRowSet().writableCast().insert(added);
            notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        }
    }
}
