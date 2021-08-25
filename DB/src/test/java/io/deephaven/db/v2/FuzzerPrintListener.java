package io.deephaven.db.v2;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.utils.TableTools;

import static io.deephaven.db.v2.TstUtils.prevTable;

public class FuzzerPrintListener extends InstrumentedShiftAwareListener {
    private final String description;
    private final Table table;
    private final int rowCount;

    FuzzerPrintListener(final String description, final Table table) {
        this(description, table, 100);
    }

    FuzzerPrintListener(final String description, final Table table, final int rowCount) {
        super("Fuzzer Failure Listener");
        this.description = description;
        this.table = table;
        this.rowCount = rowCount;
    }

    @Override
    public void onUpdate(final Update upstream) {
        System.out.println("Description: " + description + ": " + table.size());
        System.out.println(upstream);
        if (rowCount > 0) {
            TableTools.showWithIndex(table, rowCount);
            System.out.println("Previous: " + table.getIndex().getPrevIndex().size());
            TableTools.showWithIndex(prevTable(table), rowCount);
        }
    }

    @Override
    public void onFailureInternal(Throwable originalException,
            io.deephaven.db.v2.utils.UpdatePerformanceTracker.Entry sourceEntry) {
        System.out.println("Error for: " + description);
        originalException.printStackTrace();
    }
}
