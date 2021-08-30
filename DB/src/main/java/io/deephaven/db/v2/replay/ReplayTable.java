/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.replay;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;

import java.util.Map;

public class ReplayTable extends QueryTable implements LiveTable {


    private final Index.Iterator indexIterator;
    private long curr;
    private final ColumnSource<DBDateTime> timeSource;
    private boolean done;
    private final Replayer replayer;

    public ReplayTable(Index index, Map<String, ? extends ColumnSource<?>> result, String timeColumn,
            Replayer replayer) {
        super(Index.FACTORY.getIndexByValues(), result);
        Require.requirement(replayer != null, "replayer != null");
        // noinspection unchecked
        replayer.registerTimeSource(index, (ColumnSource<DBDateTime>) result.get(timeColumn));
        setRefreshing(true);
        indexIterator = index.iterator();
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
        Index.RandomBuilder indexBuilder = Index.FACTORY.getRandomBuilder();
        while (!done && nextTime < replayer.currentTimeNanos()) {
            indexBuilder.addKey(curr);
            if (indexIterator.hasNext()) {
                curr = indexIterator.nextLong();
                nextTime = timeSource.get(curr).getNanos();
            } else {
                done = true;
            }
        }
        final Index added = indexBuilder.getIndex();
        if (added.size() > 0) {
            getIndex().insert(added);
            notifyListeners(added, Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex());
        }
    }
}
