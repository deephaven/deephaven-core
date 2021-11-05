/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.replay;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.tables.live.LiveTable;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.v2.QueryTable;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.RowSet;
import io.deephaven.engine.v2.utils.RowSetBuilderRandom;
import io.deephaven.engine.v2.utils.RowSetFactory;

import java.util.Map;

public class ReplayTable extends QueryTable implements LiveTable {


    private final RowSet.Iterator indexIterator;
    private long curr;
    private final ColumnSource<DBDateTime> timeSource;
    private boolean done;
    private final Replayer replayer;

    public ReplayTable(RowSet rowSet, Map<String, ? extends ColumnSource<?>> result, String timeColumn,
            Replayer replayer) {
        super(RowSetFactory.empty().convertToTracking(), result);
        Require.requirement(replayer != null, "replayer != null");
        // noinspection unchecked
        replayer.registerTimeSource(rowSet, (ColumnSource<DBDateTime>) result.get(timeColumn));
        setRefreshing(true);
        indexIterator = rowSet.iterator();
        if (indexIterator.hasNext()) {
            curr = indexIterator.nextLong();
        } else {
            done = true;
        }
        timeSource = getColumnSource(timeColumn);
        this.replayer = replayer;
        refresh();
    }

    long nextTime = -1;

    @Override
    public void refresh() {
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
            getRowSet().mutableCast().insert(added);
            notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        }
    }
}
