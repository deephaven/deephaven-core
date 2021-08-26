/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.util.file;

import io.deephaven.base.testing.BaseCachedJMockTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.sched.Scheduler;
import io.deephaven.io.sched.TimedJob;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestTrackedFileHandleFactory extends BaseCachedJMockTestCase {

    private static final File FILE =
            new File(Configuration.getInstance().getWorkspacePath(), "TestTrackedFileHandleFactory.dat");
    private static final int CAPACITY = 100;
    private static final double TARGET_USAGE_RATIO = 0.9;
    private static final int TARGET_USAGE_THRESHOLD = 90;

    private Scheduler scheduler;

    private TrackedFileHandleFactory FHCUT;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // noinspection ResultOfMethodCallIgnored
        FILE.createNewFile();

        scheduler = mock(Scheduler.class);

        checking(new Expectations() {
            {
                one(scheduler).currentTimeMillis();
                will(returnValue(0L));
                one(scheduler).installJob(with(any(TimedJob.class)), with(equal(60000L)));
            }
        });

        FHCUT = new TrackedFileHandleFactory(scheduler, CAPACITY, TARGET_USAGE_RATIO, 60000);
        TestCase.assertEquals(scheduler, FHCUT.getScheduler());
        TestCase.assertEquals(CAPACITY, FHCUT.getCapacity());
        TestCase.assertEquals(TARGET_USAGE_RATIO, FHCUT.getTargetUsageRatio());
        TestCase.assertEquals(TARGET_USAGE_THRESHOLD, FHCUT.getTargetUsageThreshold());
        TestCase.assertEquals(0, FHCUT.getSize());
    }

    @After
    public void tearDown() throws Exception {
        TestFileHandle.tryToDelete(FILE);
        super.tearDown();
    }

    @Test
    public void testConstructors() {
        try {
            new TrackedFileHandleFactory(scheduler, 0);
            TestCase.fail();
        } catch (RequirementFailure expected) {
        }
        try {
            new TrackedFileHandleFactory(scheduler, 10, -0.01, 60000L);
            TestCase.fail();
        } catch (RequirementFailure expected) {
        }
        try {
            new TrackedFileHandleFactory(scheduler, 10, 1.01, 60000L);
            TestCase.fail();
        } catch (RequirementFailure expected) {
        }
        try {
            new TrackedFileHandleFactory(scheduler, 10, 0.09, 60000L);
            TestCase.fail();
        } catch (RequirementFailure expected) {
        }
    }

    @Test
    public void testCreate() throws IOException {
        TestCase.assertEquals(0, FHCUT.getSize());
        FileHandle handle = FHCUT.readOnlyHandleCreator.invoke(FILE);
        TestCase.assertEquals(1, FHCUT.getSize());

        handle.close();
        TestCase.assertFalse(handle.isOpen());
        TestCase.assertEquals(0, FHCUT.getSize());
    }

    @Test
    public void testFull() throws IOException {
        FileHandle handles[] = new FileHandle[CAPACITY + 1];
        for (int fhi = 0; fhi < CAPACITY + 1; ++fhi) {
            TestCase.assertEquals(fhi, FHCUT.getSize());
            handles[fhi] = FHCUT.readOnlyHandleCreator.invoke(FILE);
            assertIsSatisfied();
        }
        // Synchronous cleanup brings us down to threshold, but the handle that triggered the cleanup is recorded
        // afterwards.
        TestCase.assertEquals(TARGET_USAGE_THRESHOLD + 1, FHCUT.getSize());

        for (int fhi = 0; fhi < handles.length; ++fhi) {
            FileHandle fh = handles[fhi];
            if (fhi < handles.length - TARGET_USAGE_THRESHOLD - 1) {
                TestCase.assertFalse(fh.isOpen());
            } else {
                TestCase.assertTrue(fh.isOpen());
            }
        }
    }
}
