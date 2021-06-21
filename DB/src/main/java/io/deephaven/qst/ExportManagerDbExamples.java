package io.deephaven.qst;

import io.deephaven.db.v2.QueryTable;
import io.deephaven.qst.ExportManagerDb.State.Export;
import io.deephaven.qst.table.Table;

public class ExportManagerDbExamples {

    public static void test() {
        final ExportManagerDb manager = new ExportManagerDb();
        final Table i = Table.empty(42).updateView("I=i");
        final Table ijk = i
            .join(i, "", "J=I")
            .join(i, "", "K=I");
        try (final Export ref = manager.export(ijk)) {
            final QueryTable queryTable = ref.getQueryTable();
            System.out.println(queryTable.size());
        }
    }

}
