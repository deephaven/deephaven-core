package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.util.TableTools;

import static io.deephaven.engine.table.impl.TstUtils.prevTable;

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
            TableTools.showWithRowSet(table, rowCount);
            System.out.println("Previous: " + table.getRowSet().sizePrev());
            TableTools.showWithRowSet(prevTable(table), rowCount);
        }
    }

    @Override
    public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
        System.out.println("Error for: " + description);
        originalException.printStackTrace();
    }
}
