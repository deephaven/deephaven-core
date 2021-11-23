package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;

/**
 * A listener for use in unit tests that writes down the update it receives and counts how many it received.
 */
public class SimpleListener extends InstrumentedTableUpdateListenerAdapter {
    public SimpleListener(Table source) {
        super(source, false);
        reset();
    }

    public int getCount() {
        return count;
    }

    public TableUpdate getUpdate() {
        return update;
    }

    int count;
    TableUpdate update;

    public void reset() {
        close();
        count = 0;
        update = null;
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        close();
        this.update = upstream.acquire();
        ++count;
    }

    @Override
    public String toString() {
        return "SimpleListener{" +
                "count=" + count +
                (update == null ? ""
                        : (", added=" + update.added() +
                                ", removed=" + update.removed() +
                                ", modified=" + update.modified() +
                                ", shifted=" + update.shifted() +
                                ", modifiedColumnSet=" + update.modifiedColumnSet()))
                +
                '}';
    }

    public void close() {
        if (update != null) {
            update.release();
        }
    }
}
