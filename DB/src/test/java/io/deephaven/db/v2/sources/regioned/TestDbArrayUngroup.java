package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.dbarrays.DbArray;
import io.deephaven.db.tables.utils.TableManagementTools;
import io.deephaven.db.tables.utils.TableTools;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class TestDbArrayUngroup {

    @Test
    public void testUngroup() throws IOException {
        final String namespace = getClass().getSimpleName();
        final String tableName = "testUngroup";

        final Table theTable = TableTools.emptyTable(20).update("A=`a`+i%10", "B=`b`+i%5", "C=`i`+i");
        assertEquals(String.class, theTable.getDefinition().getColumn("C").getDataType());

        final Table groupedTable = theTable.by("A", "B");
        assertTrue(DbArray.class.isAssignableFrom(groupedTable.getDefinition().getColumn("C").getDataType()));
        assertEquals(String.class, groupedTable.getDefinition().getColumn("C").getComponentType());

        final Table ungroupedTable = groupedTable.ungroup();
        assertEquals(String.class, ungroupedTable.getDefinition().getColumn("C").getDataType());

        final File dataDirectory = Files.createTempDirectory(Paths.get(""), "TestDbArrayUngroup-").toFile();
        dataDirectory.deleteOnExit();

        TableManagementTools.writeTable(groupedTable, dataDirectory);
        final Table actual = TableManagementTools.readTable(dataDirectory, groupedTable.getDefinition());

        assertTrue(DbArray.class.isAssignableFrom(actual.getDefinition().getColumn("C").getDataType()));
        assertEquals(String.class, actual.getDefinition().getColumn("C").getComponentType());

        Table ungroupedActual = actual.ungroup();
        assertFalse(DbArray.class.isAssignableFrom(ungroupedActual.getDefinition().getColumn("C").getDataType()));
        assertEquals(String.class, ungroupedActual.getDefinition().getColumn("C").getDataType());
    }
}
