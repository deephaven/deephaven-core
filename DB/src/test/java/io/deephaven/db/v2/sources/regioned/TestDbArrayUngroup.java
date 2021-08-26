package io.deephaven.db.v2.sources.regioned;

import io.deephaven.base.FileUtils;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.db.tables.utils.TableTools;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class TestDbArrayUngroup {

    private static File dataDirectory;

    @Before
    public void setUp() throws IOException {
        dataDirectory = Files.createTempDirectory(Paths.get(""), "TestDbArrayUngroup-").toFile();
    }

    @After
    public void tearDown() {
        FileUtils.deleteRecursively(dataDirectory);
    }

    @Test
    public void testUngroup() {
        final Table theTable = TableTools.emptyTable(20).update("A=`a`+i%10", "B=`b`+i%5", "C=`i`+i");
        assertEquals(String.class, theTable.getDefinition().getColumn("C").getDataType());

        final Table groupedTable = theTable.by("A", "B");
        assertTrue(DbArray.class.isAssignableFrom(groupedTable.getDefinition().getColumn("C").getDataType()));
        assertEquals(String.class, groupedTable.getDefinition().getColumn("C").getComponentType());

        final Table ungroupedTable = groupedTable.ungroup();
        assertEquals(String.class, ungroupedTable.getDefinition().getColumn("C").getDataType());

        File dest = new File(dataDirectory, "testUngroup.parquet");
        ParquetTools.writeTable(groupedTable, dest);
        final Table actual = ParquetTools.readTable(dest);

        assertTrue(DbArray.class.isAssignableFrom(actual.getDefinition().getColumn("C").getDataType()));
        assertEquals(String.class, actual.getDefinition().getColumn("C").getComponentType());

        Table ungroupedActual = actual.ungroup();
        assertFalse(DbArray.class.isAssignableFrom(ungroupedActual.getDefinition().getColumn("C").getDataType()));
        assertEquals(String.class, ungroupedActual.getDefinition().getColumn("C").getDataType());
    }
}
