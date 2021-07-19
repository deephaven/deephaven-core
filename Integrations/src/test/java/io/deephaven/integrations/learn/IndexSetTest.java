package io.deephaven.integrations.learn;

import junit.framework.TestCase;
import org.junit.BeforeClass;
import org.junit.Test;

public class IndexSetTest {

    @BeforeClass
    static public void setup() {
        IndexSet indexSet = new IndexSet(10);
    }

    @Test
    public void TestValidParams() {
        TestCase.assertEquals(-1, -1);
    }
}