/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.sched;

import io.deephaven.io.logger.Logger;
import junit.framework.TestCase;

import java.nio.channels.SelectableChannel;
import java.io.IOException;

import io.deephaven.base.Procedure;

public class TestJobStateTimeoutQueue extends TestCase {

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * A null Job implementation
     */
    private static class NullJob extends Job {
        public int invoke(SelectableChannel channel, int readyOps, Procedure.Nullary handoff)
            throws IOException {
            return 0;
        }

        public void timedOut() {}

        public void cancelled() {}
    }

    /**
     * Macro test
     */
    public void testTimeoutQueue() {
        JobState[] ja = new JobState[10];
        for (int i = 0; i < ja.length; ++i) {
            ja[i] = new JobState(new NullJob());
        }
        JobStateTimeoutQueue q = new JobStateTimeoutQueue(Logger.NULL, 10);

        q.enter(ja[0], 1);
        assertTrue(q.testInvariant("insert 1"));
        q.enter(ja[1], 9);
        assertTrue(q.testInvariant("insert 9"));
        q.enter(ja[2], 8);
        assertTrue(q.testInvariant("insert 8"));
        q.enter(ja[3], 5);
        assertTrue(q.testInvariant("insert 5"));
        q.enter(ja[4], 2);
        assertTrue(q.testInvariant("insert 2"));
        q.enter(ja[5], 3);
        assertTrue(q.testInvariant("insert 3"));
        q.enter(ja[6], 6);
        assertTrue(q.testInvariant("insert 6"));
        q.enter(ja[7], 4);
        assertTrue(q.testInvariant("insert 4"));
        q.enter(ja[8], 7);
        assertTrue(q.testInvariant("insert 7"));
        q.enter(ja[9], 10);
        assertTrue(q.testInvariant("insert 10"));

        assertEquals(ja[0], q.top());
        q.removeTop();
        q.testInvariant("remove 1");
        assertEquals(ja[4], q.top());
        q.removeTop();
        q.testInvariant("remove 2");
        assertEquals(ja[5], q.top());
        q.removeTop();
        q.testInvariant("remove 3");
        assertEquals(ja[7], q.top());
        q.removeTop();
        q.testInvariant("remove 4");
        assertEquals(ja[3], q.top());
        q.removeTop();
        q.testInvariant("remove 5");
        assertEquals(ja[6], q.top());
        q.removeTop();
        q.testInvariant("remove 6");
        assertEquals(ja[8], q.top());
        q.removeTop();
        q.testInvariant("remove 7");
        assertEquals(ja[2], q.top());
        q.removeTop();
        q.testInvariant("remove 8");
        assertEquals(ja[1], q.top());
        q.removeTop();
        q.testInvariant("remove 9");
        assertEquals(ja[9], q.top());
        q.removeTop();
        q.testInvariant("remove 10");

        assertTrue(q.testInvariant("after clone"));
    }

    /**
     * Test change of deadline within queue
     */
    public void testDeadlineChange() {
        JobState j1 = new JobState(new NullJob());
        JobState j2 = new JobState(new NullJob());
        JobState j3 = new JobState(new NullJob());
        JobStateTimeoutQueue q = new JobStateTimeoutQueue(Logger.NULL, 10);

        q.enter(j1, 1000);
        q.enter(j2, 2000);
        q.enter(j3, 3000);

        assertEquals(j1, q.top());

        q.enter(j2, 200);
        assertEquals(j2, q.top());

        q.enter(j2, 20000);
        assertEquals(j1, q.top());

        q.enter(j1, 100000);
        assertEquals(j3, q.top());
    }
}
