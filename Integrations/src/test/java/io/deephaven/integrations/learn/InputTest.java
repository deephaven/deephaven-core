package io.deephaven.integrations.learn;

import io.deephaven.db.v2.InMemoryTable;
import org.jpy.PyObject;

import junit.framework.TestCase;
import org.junit.Test;

public class InputTest {

    @Test
    public void input_constructorTest() {
         InMemoryTable table = new InMemoryTable(
            new String[]{"Symbols", "GroupedInts"},
            new Object[]{
                    new String[]{"A", "A", "AAPL", "AAPL", "AAPL", "B", "B", "B", "B"},
                    new int[]{1, 2, 1, 2, 3, 1, 2, 3, 4}
            });

         String[] colNames = { "Symbols" };

         //PyObject pyFunc =
    }

}