//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.util.SafeCloseable;
import io.deephaven.vector.Vector;
import io.deephaven.util.type.ArrayTypeUtils;
import io.deephaven.engine.util.TableTools;
import io.deephaven.parquet.table.ParquetTools;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;

import java.io.*;
import java.nio.file.Files;

import static io.deephaven.engine.util.TableTools.*;

public class TestAggregatedSelect extends TestCase {

    public TestAggregatedSelect() {
        super("TestAggregatedSelect()");
    }

    private static File tableDirectory;
    private SafeCloseable executionContext;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        try {
            tableDirectory = Files.createTempDirectory("TestAggregatedSelect").toFile();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @After
    public void tearDown() {
        executionContext.close();
        FileUtils.deleteRecursivelyOnNFS(tableDirectory);
    }

    public Table createTestTable() {
        FileUtils.deleteRecursively(tableDirectory);

        TableDefinition tableDefinition = TableDefinition.of(
                ColumnDefinition.ofString("USym"),
                ColumnDefinition.ofDouble("Bid"),
                ColumnDefinition.ofDouble("BidSize"));

        final int size = 40;

        String[] symbol = new String[size];
        double[] bid = new double[size];
        double[] bidSize = new double[size];

        for (int ii = 0; ii < size; ++ii) {
            symbol[ii] = (ii < 8) ? "ABC" : "XYZ";
            bid[ii] = (ii < 15) ? 98 : 99;
            bidSize[ii] = ii;
        }

        tableDirectory.mkdirs();
        final File dest = new File(tableDirectory, "Table.parquet");
        ParquetTools.writeTable(
                newTable(stringCol("USym", symbol), doubleCol("Bid", bid), doubleCol("BidSize", bidSize)),
                dest.getPath(),
                ParquetInstructions.EMPTY.withTableDefinition(tableDefinition));
        return ParquetTools.readTable(dest.getPath());
    }

    Table doAggregatedQuery() {
        Table t = createTestTable();
        Table t2 = t.groupBy("USym", "Bid").groupBy("USym");
        return t2;
    }

    double avgConsecutive(int start, int end) {
        double count = (end - start) + 1;
        double sumEnd = (end * (end + 1)) / 2;
        double sumStart = (start * (start + 1)) / 2;
        return (sumEnd - sumStart) / count;
    }

    public void testSelectType() {
        Table table = createTestTable();
        Table selectedTable = table.select();

        String[] colNames = {"USym", "Bid", "BidSize"};
        for (String colName : colNames) {
            ColumnDefinition<?> cdFresh = table.getDefinition().getColumn(colName);
            ColumnDefinition<?> cdSelected = selectedTable.getDefinition().getColumn(colName);
            TestCase.assertEquals(cdFresh.getDataType(), cdSelected.getDataType());
            TestCase.assertEquals(cdFresh.getComponentType(), cdSelected.getComponentType());
        }
    }

    public void testUngroup() {
        Table freshTable = doAggregatedQuery();

        Table t1 = freshTable.dropColumns("BidSize");
        TableTools.show(t1);

        System.out.println("Avg Check 1");
        Table t3 = t1.update("Bid=avg(Bid)");
        TableTools.show(t3);

        System.out.println("Group Check");
        Table t2 = t1.ungroup();
        TableTools.show(t2);

        System.out.println("After Select");
        t2 = t1.select();

        String[] colNames = {"Bid", "USym"};
        for (String colName : colNames) {
            ColumnDefinition<?> cdFresh = t1.getDefinition().getColumn(colName);
            ColumnDefinition<?> cdSelected = t2.getDefinition().getColumn(colName);
            TestCase.assertEquals(cdFresh.getDataType(), cdSelected.getDataType());
            TestCase.assertEquals(cdFresh.getComponentType(), cdSelected.getComponentType());
        }

        t2 = t2.ungroup();
        TableTools.show(t2);

        System.out.println("With nested array:");
        Table s1 = freshTable.dropColumns("Bid");

        Table s1s = s1.select();
        colNames[0] = "BidSize";
        for (String colName : colNames) {
            ColumnDefinition<?> cdFresh = s1.getDefinition().getColumn(colName);
            ColumnDefinition<?> cdSelected = s1s.getDefinition().getColumn(colName);
            TestCase.assertEquals(cdFresh.getDataType(), cdSelected.getDataType());
            TestCase.assertEquals(cdFresh.getComponentType(), cdSelected.getComponentType());
        }

        TableTools.show(s1);
        System.out.println("ungrouped:");
        s1 = s1.ungroup();
        TableTools.show(s1);

        Table s2 = s1.update("BidSize=avg(BidSize)");
        TableTools.show(s2);

        Table s3 = s1.select();
        Table s4 = s3.update("BidSize=avg(BidSize)");
        TableTools.show(s4);
    }

    private void dumpArray(String prefix, Vector<?> vector) {
        System.out.println(prefix + ": Array of " + vector.getComponentType().toString());
        String prefixsp = new String(new char[prefix.length()]).replace('\0', ' ');
        final boolean containsArrays = Vector.class.isAssignableFrom(vector.getComponentType());
        final ArrayTypeUtils.ArrayAccessor<?> arrayAccessor = ArrayTypeUtils.getArrayAccessor(vector.toArray());
        for (int jj = 0; jj < vector.size(); ++jj) {
            if (containsArrays) {
                dumpArray(prefix + "[" + jj + "] ", (Vector<?>) arrayAccessor.get(jj));
            } else {
                System.out.println(prefixsp + "[" + jj + "]: " + arrayAccessor.get(jj).toString());
            }
        }
    }
}
