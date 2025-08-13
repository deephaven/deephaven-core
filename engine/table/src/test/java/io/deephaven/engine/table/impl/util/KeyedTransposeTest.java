package io.deephaven.engine.table.impl.util;

import java.util.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.util.TableTools.*;

public class KeyedTransposeTest extends RefreshingTableTestCase {
    Table getStaticTable() {
        return TableTools.newTable(
                stringCol("Date", "2025-08-05", "2025-08-05", "2025-08-06", "2025-08-07", "2025-08-07",
                        "2025-08-08", "2025-08-08"),
                stringCol("Host", "h1", "h1", "h2", "h1", "h2", "h2", "h2"),
                stringCol("Level", "INFO", "INFO", "WARN", "ERROR", "INFO", "WARN", "WARN"),
                intCol("Cat", 1, 1, 2, 3, 3, 2, 1),
                intCol("Id", 10, 20, 30, 40, 50, 60, 70),
                stringCol("Note", "note1", "note2", "note3", "note4", "note5", "note6", "note7")
        );
    }

    public void testKeyTranspose1() {
        Table source = getStaticTable();
        Table t = KeyedTranspose.keyedTranspose(source, List.of(AggCount("Count")), new String[]{"Date","Host"},
                new String[]{"Level"});
        TableTools.show(t);
    }

    public void testKeyTranspose2() {
        Table source = getStaticTable();
        Table t = KeyedTranspose.keyedTranspose(source, List.of(AggCount("Count"), AggSum("Sum=Cat")),
                new String[]{"Date","Host"}, new String[]{"Level"});
        TableTools.show(t);
    }

    public void testKeyTranspose3() {
        Table source = getStaticTable();
        Table t = KeyedTranspose.keyedTranspose(source, List.of(AggCount("Count")),
                new String[]{"Date","Host"}, new String[]{"Level", "Cat"});
        TableTools.show(t);
    }

    public void testKeyTranspose4() {
        Table source = getStaticTable();
        Table t = KeyedTranspose.keyedTranspose(source, List.of(AggCount("Count"), AggSum("Sum=Cat")),
                new String[]{"Date","Host"}, new String[]{"Level", "Cat"});
        TableTools.show(t);
    }

    public void testKeyTransposeIncremental() {
        Table source = getStaticTable();
        Table t = KeyedTranspose.keyedTranspose(source, List.of(AggCount("Count"), AggSum("Sum=Cat")),
                new String[]{"Date","Host"}, new String[]{});
        TableTools.show(t);
    }

}