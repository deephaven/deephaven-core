package io.deephaven.db.v2;

/**
 * A listener for use in unit tests that writes down the update it receives and counts how many it received.
 */
public class SimpleShiftAwareListener extends InstrumentedShiftAwareListenerAdapter {
    public SimpleShiftAwareListener(DynamicTable source) {
        super(source, false);
        reset();
    }

    public int getCount() {
        return count;
    }

    public Update getUpdate() {
        return update;
    }

    int count;
    Update update;

    public void reset() {
        close();
        count = 0;
        update = null;
    }

    @Override
    public void onUpdate(final Update upstream) {
        close();
        this.update = upstream.acquire();
        ++count;
    }

    @Override
    public String toString() {
        return "SimpleShiftAwareListener{" +
                "count=" + count +
                (update == null ? "" : (
                ", added=" + update.added +
                ", removed=" + update.removed +
                ", modified=" + update.modified +
                ", shifted=" + update.shifted +
                ", modifiedColumnSet=" + update.modifiedColumnSet)) +
                '}';
    }

    public void close() {
        if (update != null) {
            update.release();
        }
    }
}
