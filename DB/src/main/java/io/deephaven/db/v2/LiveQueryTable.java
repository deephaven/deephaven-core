/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.tables.live.LiveTable;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;

import java.util.Map;

public class LiveQueryTable extends QueryTable implements LiveTable {
    Index.RandomBuilder additionsBuilder = Index.FACTORY.getRandomBuilder();

    public LiveQueryTable(Index index, Map<String, ? extends ColumnSource<?>> result) {
        super(index, result);
    }

    @Override
    public void refresh() {
        final Index added = additionsBuilder.getIndex();
        getIndex().insert(added);
        additionsBuilder = Index.FACTORY.getRandomBuilder();
        if (added.size() > 0) {
            notifyListeners(added, Index.FACTORY.getEmptyIndex(), Index.FACTORY.getEmptyIndex());
        }
    }

    public void addIndex(long key) {
        additionsBuilder.addKey(key);
    }

    public void addRange(long firstKey, long lastKey) {
        additionsBuilder.addRange(firstKey, lastKey);
    }

    @Override
    public void destroy() {
        super.destroy();
        LiveTableMonitor.DEFAULT.removeTable(this);
    }
}
