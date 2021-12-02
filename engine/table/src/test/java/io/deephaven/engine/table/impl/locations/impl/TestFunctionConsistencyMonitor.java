package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class TestFunctionConsistencyMonitor {
    @Test
    public void testCurrentDateNy() {
        DateTimeUtils.currentDateNyOverride = "Aardvark";
        TestCase.assertEquals("Aardvark", CompositeTableDataServiceConsistencyMonitor.currentDateNy());

        try (final SafeCloseable ignored = CompositeTableDataServiceConsistencyMonitor.INSTANCE.start()) {
            TestCase.assertEquals("Aardvark", CompositeTableDataServiceConsistencyMonitor.currentDateNy());
            DateTimeUtils.currentDateNyOverride = "Armadillo";
            TestCase.assertEquals("Aardvark", CompositeTableDataServiceConsistencyMonitor.currentDateNy());
        }

        TestCase.assertEquals("Armadillo", CompositeTableDataServiceConsistencyMonitor.currentDateNy());

        DateTimeUtils.currentDateNyOverride = null;
    }


    @Test
    public void testMidStreamRegistration() {
        DateTimeUtils.currentDateNyOverride = "Aardvark";
        TestCase.assertEquals("Aardvark", CompositeTableDataServiceConsistencyMonitor.currentDateNy());

        final AtomicInteger atomicInteger = new AtomicInteger(7);
        final FunctionConsistencyMonitor.ConsistentSupplier<Integer> consistentInteger;

        try (final SafeCloseable ignored = CompositeTableDataServiceConsistencyMonitor.INSTANCE.start()) {
            TestCase.assertEquals("Aardvark", CompositeTableDataServiceConsistencyMonitor.currentDateNy());
            DateTimeUtils.currentDateNyOverride = "Armadillo";
            TestCase.assertEquals("Aardvark", CompositeTableDataServiceConsistencyMonitor.currentDateNy());

            consistentInteger = new CompositeTableDataServiceConsistencyMonitor.ConsistentSupplier<>(
                    atomicInteger::getAndIncrement);
            TestCase.assertEquals((Integer) 7, consistentInteger.get());
            TestCase.assertEquals((Integer) 7, consistentInteger.get());
            TestCase.assertEquals((Integer) 7, consistentInteger.get());
        }

        TestCase.assertEquals((Integer) 8, consistentInteger.get());

        TestCase.assertEquals("Armadillo", CompositeTableDataServiceConsistencyMonitor.currentDateNy());

        DateTimeUtils.currentDateNyOverride = null;
    }

    @Test
    public void testCurrentDateNyWithThreads() throws InterruptedException {
        DateTimeUtils.currentDateNyOverride = "Bobcat";
        TestCase.assertEquals("Bobcat", CompositeTableDataServiceConsistencyMonitor.currentDateNy());


        final MutableObject<String> mutableString = new MutableObject<>();

        Thread t;
        try (final SafeCloseable ignored = CompositeTableDataServiceConsistencyMonitor.INSTANCE.start()) {
            TestCase.assertEquals("Bobcat", CompositeTableDataServiceConsistencyMonitor.currentDateNy());

            t = new Thread(() -> {
                synchronized (mutableString) {
                    // do nothing
                }
                try (final SafeCloseable ignored2 = CompositeTableDataServiceConsistencyMonitor.INSTANCE.start()) {
                    mutableString.setValue(CompositeTableDataServiceConsistencyMonitor.currentDateNy());
                }
            });
            synchronized (mutableString) {
                t.start();
                DateTimeUtils.currentDateNyOverride = "Bear";
            }

            TestCase.assertEquals("Bobcat", CompositeTableDataServiceConsistencyMonitor.currentDateNy());
        }

        t.join(1000);
        TestCase.assertEquals("Bear", mutableString.getValue());

        mutableString.setValue(null);

        final MutableObject<String> mutableString2 = new MutableObject<>();
        final MutableObject<String> mutableString3 = new MutableObject<>();
        final MutableBoolean mutableBoolean = new MutableBoolean(false);
        final MutableBoolean gotValueOnce = new MutableBoolean(false);

        try (final SafeCloseable ignored = CompositeTableDataServiceConsistencyMonitor.INSTANCE.start()) {
            TestCase.assertEquals("Bear", CompositeTableDataServiceConsistencyMonitor.currentDateNy());

            t = new Thread(() -> {
                try (final SafeCloseable ignored2 = CompositeTableDataServiceConsistencyMonitor.INSTANCE.start()) {
                    synchronized (mutableString) {
                        // do nothing
                    }
                    mutableString.setValue(CompositeTableDataServiceConsistencyMonitor.currentDateNy());
                    synchronized (gotValueOnce) {
                        gotValueOnce.setTrue();
                        gotValueOnce.notifyAll();
                    }
                    while (true) {
                        synchronized (mutableBoolean) {
                            if (mutableBoolean.booleanValue())
                                break;
                            try {
                                mutableBoolean.wait();
                            } catch (InterruptedException ignored3) {
                            }
                        }
                    }
                    mutableString3.setValue(DateTimeUtils.currentDateNy());
                    mutableString2.setValue(CompositeTableDataServiceConsistencyMonitor.currentDateNy());
                }
            });
            synchronized (mutableString) {
                t.start();
                DateTimeUtils.currentDateNyOverride = "Butterfly";
            }

            TestCase.assertEquals("Bear", CompositeTableDataServiceConsistencyMonitor.currentDateNy());
        }

        try (final SafeCloseable ignored = CompositeTableDataServiceConsistencyMonitor.INSTANCE.start()) {
            while (true) {
                synchronized (gotValueOnce) {
                    if (gotValueOnce.booleanValue()) {
                        break;
                    }
                    gotValueOnce.wait();
                }
            }
            synchronized (mutableBoolean) {
                DateTimeUtils.currentDateNyOverride = "Buffalo";
                mutableBoolean.setTrue();
                mutableBoolean.notifyAll();
            }
            TestCase.assertEquals("Buffalo", CompositeTableDataServiceConsistencyMonitor.consistentDateNy());
        }

        t.join(1000);
        TestCase.assertEquals("Butterfly", mutableString.getValue());
        TestCase.assertEquals("Butterfly", mutableString2.getValue());
        TestCase.assertEquals("Buffalo", mutableString3.getValue());

        DateTimeUtils.currentDateNyOverride = null;
    }
}
