//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.file;

import io.deephaven.base.testing.JMockRule.Expectations;
import io.deephaven.base.testing.JMockRule;
import io.deephaven.base.verify.RequirementFailure;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.deephaven.base.testing.Asserts.assertEquals;
import static org.junit.Assert.*;

public class TestTrackedFileHandleFactory {

    @Rule
    public final JMockRule jmock = new JMockRule();

    private File FILE;
    private static final int CAPACITY = 100;
    private static final double TARGET_USAGE_RATIO = 0.9;
    private static final int TARGET_USAGE_THRESHOLD = 90;

    private ScheduledExecutorService scheduler;

    private TrackedFileHandleFactory FHCUT;

    @Before
    public void setUp() throws Exception {

        FILE = Files.createTempFile(TestTrackedFileHandleFactory.class.getName(), ".dat").toFile();

        scheduler = jmock.mock(ScheduledExecutorService.class);

        jmock.checking(new Expectations() {
            {
                one(scheduler).scheduleAtFixedRate(
                        with(any(Runnable.class)),
                        with(equal(60000L)),
                        with(equal(60000L)),
                        with(equal(TimeUnit.MILLISECONDS)));
            }
        });

        FHCUT = new TrackedFileHandleFactory(scheduler, CAPACITY, TARGET_USAGE_RATIO, 60000);
        assertEquals(scheduler, FHCUT.getScheduler());
        assertEquals(CAPACITY, FHCUT.getCapacity());
        assertEquals(TARGET_USAGE_RATIO, FHCUT.getTargetUsageRatio(), 0.0);
        assertEquals(TARGET_USAGE_THRESHOLD, FHCUT.getTargetUsageThreshold());
        assertEquals(0, FHCUT.getSize());
    }

    @After
    public void tearDown() throws Exception {
        TestFileHandle.tryToDelete(FILE);
    }

    @Test
    public void testConstructors() {
        try {
            new TrackedFileHandleFactory(scheduler, 0);
            fail();
        } catch (RequirementFailure expected) {
        }
        try {
            new TrackedFileHandleFactory(scheduler, 10, -0.01, 60000L);
            fail();
        } catch (RequirementFailure expected) {
        }
        try {
            new TrackedFileHandleFactory(scheduler, 10, 1.01, 60000L);
            fail();
        } catch (RequirementFailure expected) {
        }
        try {
            new TrackedFileHandleFactory(scheduler, 10, 0.09, 60000L);
            fail();
        } catch (RequirementFailure expected) {
        }
    }

    @Test
    public void testCreate() throws IOException {
        assertEquals(0, FHCUT.getSize());
        FileHandle handle = FHCUT.readOnlyHandleCreator.invoke(FILE);
        assertEquals(1, FHCUT.getSize());

        handle.close();
        assertFalse(handle.isOpen());
        assertEquals(0, FHCUT.getSize());
    }

    @Test
    public void testFull() throws IOException {
        FileHandle handles[] = new FileHandle[CAPACITY + 1];
        for (int fhi = 0; fhi < CAPACITY + 1; ++fhi) {
            assertEquals(fhi, FHCUT.getSize());
            handles[fhi] = FHCUT.readOnlyHandleCreator.invoke(FILE);
            jmock.assertIsSatisfied();
        }
        // Synchronous cleanup brings us down to threshold, but the handle that triggered the cleanup is recorded
        // afterwards.
        assertEquals(TARGET_USAGE_THRESHOLD + 1, FHCUT.getSize());

        for (int fhi = 0; fhi < handles.length; ++fhi) {
            FileHandle fh = handles[fhi];
            if (fhi < handles.length - TARGET_USAGE_THRESHOLD - 1) {
                assertFalse(fh.isOpen());
            } else {
                assertTrue(fh.isOpen());
            }
        }
    }
}
