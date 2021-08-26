package io.deephaven.internals;

import junit.framework.TestCase;
import org.junit.Test;

public class TestJdkInternalsLoader extends TestCase {
    @Test
    public void testGetMemoryUsed() {
        long memoryUsed = JdkInternalsLoader.getInstance().getDirectMemoryStats().getMemoryUsed();
        System.out.println(memoryUsed);
    }

    @Test
    public void testMaxDirectMemory() {
        long maxDirectMemory = JdkInternalsLoader.getInstance().getDirectMemoryStats().maxDirectMemory();
        System.out.println(maxDirectMemory);
    }
}
