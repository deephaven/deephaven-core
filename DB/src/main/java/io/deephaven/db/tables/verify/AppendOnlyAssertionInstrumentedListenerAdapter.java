package io.deephaven.db.tables.verify;

import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.DynamicTable;

public class AppendOnlyAssertionInstrumentedListenerAdapter extends BaseTable.ShiftAwareListenerImpl {

    private final String description;

    public AppendOnlyAssertionInstrumentedListenerAdapter(String description, DynamicTable parent,
            DynamicTable dependent) {
        super(
                "assertAppendOnly(" + (description == null ? "" : description) + ')',
                parent, dependent);
        this.description = description;
    }

    @Override
    public void onUpdate(final Update upstream) {
        if (upstream.removed.nonempty() || upstream.modified.nonempty() || upstream.shifted.nonempty()) {
            if (description == null) {
                throw new AppendOnlyAssertionFailure();
            } else {
                throw new AppendOnlyAssertionFailure(description);
            }
        }
        super.onUpdate(upstream);
    }
}
