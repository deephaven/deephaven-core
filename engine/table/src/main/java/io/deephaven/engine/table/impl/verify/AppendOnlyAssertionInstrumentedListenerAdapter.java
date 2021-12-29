package io.deephaven.engine.table.impl.verify;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.BaseTable;

public class AppendOnlyAssertionInstrumentedListenerAdapter extends BaseTable.ListenerImpl {

    private final String description;

    public AppendOnlyAssertionInstrumentedListenerAdapter(String description, Table parent,
            BaseTable dependent) {
        super(
                "assertAppendOnly(" + (description == null ? "" : description) + ')',
                parent, dependent);
        this.description = description;
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        if (upstream.removed().isNonempty() || upstream.modified().isNonempty() || upstream.shifted().nonempty()) {
            if (description == null) {
                throw new AppendOnlyAssertionFailure();
            } else {
                throw new AppendOnlyAssertionFailure(description);
            }
        }
        super.onUpdate(upstream);
    }
}
