/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.QueryTableTestBase;
import io.deephaven.engine.testutil.TstUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import static io.deephaven.engine.testutil.TstUtils.c;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.*;

public class TestColumnDescriptionInheritance extends QueryTableTestBase {

    private Table genTestTable() {
        return TstUtils.testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                c("Sym", "aa", "bb", "cc", "dd"),
                c("intCol", 10, 20, 40, 60),
                c("doubleCol", 0.1, 0.2, 0.4, 0.6));
    }

    public void testMaybeCopyColumnDescriptions() {
        final Table sourceTable = genTestTable();
        final Table withDescriptions = sourceTable
                .withColumnDescription("Sym", "Symbol Column")
                .withColumnDescription("doubleCol", "Double Column");


        System.out.println("Running basic \"maybeCopyColumnDescriptions\" tests...");
        final QueryTable destTable =
                new QueryTable(sourceTable.getDefinition(), sourceTable.getRowSet(), sourceTable.getColumnSourceMap());
        final Map<String, String> descriptionMap =
                (Map<String, String>) withDescriptions.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);
        assertNotNull(descriptionMap);
        assertEquals(2, descriptionMap.size());

        ((BaseTable) sourceTable).maybeCopyColumnDescriptions(destTable);
        assertNull(destTable.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));

        ((BaseTable) withDescriptions).maybeCopyColumnDescriptions(destTable);
        assertEquals(descriptionMap, destTable.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));


        System.out.println("Running table-operation level column-description tests...");
        final Map<String, String> droppedColumnMap = new HashMap<>(descriptionMap);
        droppedColumnMap.remove("doubleCol");
        assertEquals(1, droppedColumnMap.size());

        assertEquals(descriptionMap, withDescriptions
                .flatten()
                .sort("doubleCol")
                .where("Sym in `aa`, `bb`, `cc`")
                .reverse()
                .firstBy("intCol")
                .lastBy("doubleCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));

        assertEquals(descriptionMap, withDescriptions
                .select("Sym", "doubleCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(droppedColumnMap, withDescriptions
                .select("Sym", "intCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(droppedColumnMap, withDescriptions
                .view("Sym", "New=doubleCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(droppedColumnMap, withDescriptions
                .dropColumns("doubleCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));

        assertNull(withDescriptions
                .select("intCol")
                .withColumnDescription("intCol", "This will be dropped")
                .updateView("Sym=`abc`", "doubleCol=0.3")
                .dropColumns("intCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));


        System.out.println("Running update-operation level column-description tests...");
        assertEquals(descriptionMap, withDescriptions
                .update("New=Sym", "New2=intCol + ` @ ` + doubleCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(droppedColumnMap, withDescriptions
                .update("New=Sym", "New2=intCol + ` @ ` + doubleCol", "doubleCol=intCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(descriptionMap, withDescriptions
                .updateView("New=Sym", "New2=intCol + ` @ ` + doubleCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(droppedColumnMap, withDescriptions
                .updateView("New=Sym", "New2=intCol + ` @ ` + doubleCol", "doubleCol=intCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(descriptionMap, withDescriptions
                .lazyUpdate("New=Sym", "New2=intCol + ` @ ` + doubleCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(droppedColumnMap, withDescriptions
                .lazyUpdate("New=Sym", "New2=intCol + ` @ ` + doubleCol", "doubleCol=intCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));

        assertNull(sourceTable
                .updateView("Temp=Sym", "Sym=intCol", "intCol=doubleCol", "doubleCol=Temp")
                .dropColumns("Temp")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));

        assertNull(withDescriptions
                .updateView("Temp=Sym", "Sym=intCol", "intCol=doubleCol", "doubleCol=Temp")
                .dropColumns("Temp")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));


        System.out.println("Running rename-operation level column-description tests...");
        final Map<String, String> renamedColumnMap = new HashMap<>(descriptionMap);
        renamedColumnMap.put("RenamedSym", renamedColumnMap.remove("Sym"));
        assertEquals(2, renamedColumnMap.size());

        assertEquals(renamedColumnMap, withDescriptions
                .renameColumns("RenamedSym=Sym")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(descriptionMap, withDescriptions
                .renameColumns("RenamedSym=Sym")
                .renameColumns("Sym=RenamedSym")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(droppedColumnMap, withDescriptions
                .renameColumns("RenamedSym=Sym")
                .renameColumns("Sym=RenamedSym")
                .select("Sym", "intCol")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));

        assertNull(sourceTable
                .renameColumns("RenamedSym=Sym")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));


        System.out.println("Running join-operation level column-description tests...");
        final Table rightTable = withDescriptions
                .renameColumns("rightInt=intCol", "rightDouble=doubleCol")
                .withColumnDescription("Sym", "Ignored Sym");
        final Map<String, String> rightMap =
                (Map<String, String>) rightTable.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE);
        assertNotNull(rightMap);
        assertEquals(2, rightMap.size());

        final Map<String, String> joinedColumnMap = new HashMap<>(descriptionMap);
        rightMap.forEach(joinedColumnMap::putIfAbsent);
        assertEquals(3, joinedColumnMap.size());

        assertEquals(joinedColumnMap, withDescriptions
                .join(rightTable, "Sym", "rightInt,rightDouble")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(joinedColumnMap, withDescriptions
                .naturalJoin(rightTable, "Sym", "rightInt,rightDouble")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(joinedColumnMap, withDescriptions
                .exactJoin(rightTable, "Sym", "rightInt,rightDouble")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));

        assertNull(sourceTable
                .naturalJoin(sourceTable.renameColumns("rightInt=intCol", "rightDouble=doubleCol"), "Sym",
                        "rightInt,rightDouble")
                .getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));


        System.out.println("Success");
    }

    public void testColumnDescriptionCopy() {
        final Table t = newTable(stringCol("Str", "Apple", "Banana", "Carot", "Date"), intCol("Fib", 1, 1, 2, 3));
        final Table sdesc = t.withColumnDescription("Str", "Fruit");
        final Table fdesc = sdesc.withColumnDescription("Fib", "Fibonnaci");
        final Table fonlydesc = t.withColumnDescription("Fib", "Fibonnaci2");
        assertNull(t.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(Collections.singletonMap("Str", "Fruit"), sdesc.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        assertEquals(Collections.singletonMap("Fib", "Fibonnaci2"), fonlydesc.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));
        final Map<String, String> ex  = new TreeMap<>();
        ex.put("Fib", "Fibonnaci");
        ex.put("Str", "Fruit");
        assertEquals(ex, fdesc.getAttribute(Table.COLUMN_DESCRIPTIONS_ATTRIBUTE));

        final Table addDuplicate = fdesc.withColumnDescription("Str", "Fruit");
        assertSame(addDuplicate, fdesc);
    }
}
