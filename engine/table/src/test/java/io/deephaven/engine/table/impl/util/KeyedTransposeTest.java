package io.deephaven.engine.table.impl.util;

import java.util.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;

import static io.deephaven.util.QueryConstants.*;
import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

public class KeyedTransposeTest extends RefreshingTableTestCase {
    final Table staticSource = TableTools.newTable(
            stringCol("Date", "2025-08-05", "2025-08-05", "2025-08-06", "2025-08-07", "2025-08-07",
                    "2025-08-08", "2025-08-08"),
            stringCol("Host", "h1", "h1", "h2", "h1", "h2", "h2", "h2"),
            stringCol("Level", "INFO", "INFO", "WARN", "ERROR", "INFO", "WARN", "WARN"),
            intCol("Cat", 1, 1, 2, 3, 3, 2, 1),
            intCol("Id", 10, 20, 30, 40, 50, 60, 70),
            stringCol("Note", "note1", "note2", "note3", "note4", "note5", "note6", "note7")
        );

    public void testOneAggOneByColWithInitialGroups() {
        Table initialGroups = TableTools.newTable(stringCol("Level", "ERROR", "WARN", "INFO"))
                .join(staticSource.selectDistinct("Date", "Host"));
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count")), new String[]{"Date","Host"},
                new String[]{"Level"}, initialGroups);
        Table ex = TableTools.newTable(stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-07", "2025-08-07", "2025-08-08"),
                stringCol("Host", "h1", "h2", "h1", "h2", "h2"),
                longCol("ERROR", 0, 0, 1, 0, 0),
                longCol("WARN", 0, 1, 0, 0, 2),
                longCol("INFO", 2, 0, 0, 1, 0));
        assertTableEquals(ex, t);
    }

    public void testOneAggOneByColNoInitialGroups() {
        Table initialGroups = TableTools.emptyTable(0);
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count")), new String[]{"Date","Host"},
                new String[]{"Level"}, initialGroups);
        Table ex = TableTools.newTable(stringCol("Date", "2025-08-05", "2025-08-07", "2025-08-06", "2025-08-08", "2025-08-07"),
                stringCol("Host", "h1", "h2", "h2", "h2", "h1"),
                longCol("INFO", 2, 1, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("WARN", NULL_LONG, NULL_LONG, 1, 2, NULL_LONG),
                longCol("ERROR", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1));
        assertTableEquals(ex, t);
    }

    public void testTwoAggOneByCol() {
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count"), AggSum("Sum=Cat")),
                new String[]{"Date","Host"}, new String[]{"Level"});
        Table ex = TableTools.newTable(stringCol("Date", "2025-08-05", "2025-08-07", "2025-08-06", "2025-08-08", "2025-08-07"),
                stringCol("Host", "h1", "h2", "h2", "h2", "h1"),
                longCol("Count_INFO", 2, 1, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Sum_INFO", 2, 3, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_WARN", NULL_LONG, NULL_LONG, 1, 2, NULL_LONG),
                longCol("Sum_WARN", NULL_LONG, NULL_LONG, 2, 3, NULL_LONG),
                longCol("Count_ERROR", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1),
                longCol("Sum_ERROR", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 3));
        assertTableEquals(ex, t);
    }

    public void testOneAggTwoByCol() {
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count")),
                new String[]{"Date","Host"}, new String[]{"Level", "Cat"});
        Table ex = TableTools.newTable(stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-08", "2025-08-07", "2025-08-07"),
                stringCol("Host", "h1", "h2", "h2", "h1", "h2"),
                longCol("INFO_1", 2, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("WARN_2", NULL_LONG, 1, 1, NULL_LONG, NULL_LONG),
                longCol("ERROR_3", NULL_LONG, NULL_LONG, NULL_LONG, 1, NULL_LONG),
                longCol("INFO_3", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1),
                longCol("WARN_1", NULL_LONG, NULL_LONG, 1, NULL_LONG, NULL_LONG));
        assertTableEquals(ex, t);
    }

    public void testTwoAggTwoByColWithInitialGroups() {
        Table initialGroups = TableTools.newTable(stringCol("Level", "INFO", "WARN", "ERROR"),
                intCol("Cat", 3, 2, 1)).join(staticSource.selectDistinct("Date", "Host"));
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count"), AggSum("Sum=Cat")),
                new String[]{"Date","Host"}, new String[]{"Level", "Cat"}, initialGroups);
        Table ex = TableTools.newTable(stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-07", "2025-08-07", "2025-08-08"),
                stringCol("Host", "h1", "h2", "h1", "h2", "h2"),
                longCol("Count_INFO_3", 0, 0, 0, 1, 0),
                longCol("Sum_INFO_3", NULL_LONG, NULL_LONG, NULL_LONG, 3, NULL_LONG),
                longCol("Count_WARN_2", 0, 1, 0, 0, 1),
                longCol("Sum_WARN_2", NULL_LONG, 2, NULL_LONG, NULL_LONG, 2),
                longCol("Count_ERROR_1", 0, 0, 0, 0, 0),
                longCol("Sum_ERROR_1", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_INFO_1", 2, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Sum_INFO_1", 2, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_ERROR_3", NULL_LONG, NULL_LONG, 1, NULL_LONG, NULL_LONG),
                longCol("Sum_ERROR_3", NULL_LONG, NULL_LONG, 3, NULL_LONG, NULL_LONG),
                longCol("Count_WARN_1", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1),
                longCol("Sum_WARN_1", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1));
        assertTableEquals(ex, t);
    }

    public void testTwoAggTwoByColNoInitialGroups() {
        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count"), AggSum("Sum=Cat")),
                new String[]{"Date","Host"}, new String[]{"Level", "Cat"});
        Table ex = TableTools.newTable(stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-08", "2025-08-07", "2025-08-07"),
                stringCol("Host", "h1", "h2", "h2", "h1", "h2"),
                longCol("Count_INFO_1", 2, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Sum_INFO_1", 2, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_WARN_2", NULL_LONG, 1, 1, NULL_LONG, NULL_LONG),
                longCol("Sum_WARN_2", NULL_LONG, 2, 2, NULL_LONG, NULL_LONG),
                longCol("Count_ERROR_3", NULL_LONG, NULL_LONG, NULL_LONG, 1, NULL_LONG),
                longCol("Sum_ERROR_3", NULL_LONG, NULL_LONG, NULL_LONG, 3, NULL_LONG),
                longCol("Count_INFO_3", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 1),
                longCol("Sum_INFO_3", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, 3),
                longCol("Count_WARN_1", NULL_LONG, NULL_LONG, 1, NULL_LONG, NULL_LONG),
                longCol("Sum_WARN_1", NULL_LONG, NULL_LONG, 1, NULL_LONG, NULL_LONG));
        assertTableEquals(ex, t);
    }

    public void testTwoAggTwoByColEmptySource() {
        Table initialGroups = TableTools.newTable(stringCol("Level", "INFO", "WARN", "ERROR"),
                intCol("Cat", 3, 2, 1)).join(staticSource.selectDistinct("Date", "Host"));
        Table emptySource = staticSource.where("Date == `No Match`");
        Table t = KeyedTranspose.keyedTranspose(emptySource, List.of(AggCount("Count"), AggSum("Sum=Cat")),
                new String[]{"Date","Host"}, new String[]{"Level", "Cat"}, initialGroups);
        Table ex = TableTools.newTable(stringCol("Date", "2025-08-05", "2025-08-06", "2025-08-07", "2025-08-07", "2025-08-08"),
                stringCol("Host", "h1", "h2", "h1", "h2", "h2"),
                longCol("Count_INFO_3", 0, 0, 0, 0, 0),
                longCol("Sum_INFO_3", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_WARN_2", 0, 0, 0, 0, 0),
                longCol("Sum_WARN_2", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG),
                longCol("Count_ERROR_1", 0, 0, 0, 0, 0),
                longCol("Sum_ERROR_1", NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG, NULL_LONG));
        TableTools.show(t);
        assertTableEquals(ex, t);
    }

//    public void testOneAggOneByColWithIllegalValues() {
//        Table staticSource = TableTools.newTable(
//                stringCol("Date", "2025-08-05", "2025-08-05", "2025-08-06", "2025-08-07", "2025-08-07",
//                        "2025-08-08", "2025-08-08"),
//                stringCol("Bad", "E.F", "G/H", "1", "C D", "A-B", "1", "E.F"));
//        Table initialGroups = TableTools.newTable(stringCol("Bad", "1", "A-B", "C D", "E.F"))
//                .join(staticSource.selectDistinct("Date"));
//        Table t = KeyedTranspose.keyedTranspose(staticSource, List.of(AggCount("Count")), new String[]{"Date"},
//                new String[]{"Bad"}, initialGroups);
//        TableTools.show(t);
//        assertTableEquals(ex, t);
//    }

//    public void testKeyTransposeIncremental() {
//        Table source = getStaticTable();
//        Table t = KeyedTranspose.keyedTranspose(source, List.of(AggCount("Count"), AggSum("Sum=Cat")),
//                new String[]{"Date","Host"}, new String[]{});
//        TableTools.show(t);
//    }

}