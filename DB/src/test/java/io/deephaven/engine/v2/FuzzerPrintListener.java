package io.deephaven.engine.v2;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.tables.utils.TableTools;

import static io.deephaven.engine.v2.TstUtils.prevTable;

public class FuzzerPrintListener extends InstrumentedTableUpdateListener {
    private final String description;
    private final Table table;
    private final int rowCount;

    FuzzerPrintListener(final String description, final Table table) {
        this(description, table, 100);
    }

    FuzzerPrintListener(final String description, final Table table, final int rowCount) {
        super("Fuzzer Failure ShiftObliviousListener");
        this.description = description;
        this.table = table;
        this.rowCount = rowCount;
    }

    @Override
    public void onUpdate(final TableUpdate upstream) {
        System.out.println("Description: " + description + ": " + table.size());
        System.out.println(upstream);
        if (rowCount > 0) {
            TableTools.showWithIndex(table, rowCount);
            System.out.println("Previous: " + table.getRowSet().getPrevRowSet().size());
            TableTools.showWithIndex(prevTable(table), rowCount);
        }
    }

    @Override
    public void onFailureInternal(Throwable originalException,
            io.deephaven.engine.v2.utils.UpdatePerformanceTracker.Entry sourceEntry) {
        System.out.println("Error for: " + description);
        originalException.printStackTrace();
    }
}
