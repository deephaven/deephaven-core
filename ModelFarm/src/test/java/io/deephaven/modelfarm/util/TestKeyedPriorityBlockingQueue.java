/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.modelfarm.util;

import io.deephaven.base.testing.BaseArrayTestCase;

public class TestKeyedPriorityBlockingQueue extends BaseArrayTestCase {

    public void testQueue() throws InterruptedException {
        final KeyedPriorityBlockingQueue<String> queue = new KeyedPriorityBlockingQueue<>();
        assertTrue(queue.isEmpty());

        queue.enqueue("A", 1);
        assertFalse(queue.isEmpty());
        assertEquals("A", queue.take());
        assertTrue(queue.isEmpty());

        queue.enqueue("A", 1);
        assertFalse(queue.isEmpty());
        queue.enqueue("B", 1);
        assertFalse(queue.isEmpty());
        assertEquals("A", queue.take());
        assertFalse(queue.isEmpty());
        assertEquals("B", queue.take());
        assertTrue(queue.isEmpty());

        queue.enqueue("B", 1);
        assertFalse(queue.isEmpty());
        queue.enqueue("A", 1);
        assertFalse(queue.isEmpty());
        assertEquals("B", queue.take());
        assertFalse(queue.isEmpty());
        assertEquals("A", queue.take());
        assertTrue(queue.isEmpty());

        queue.enqueue("C", 0);
        assertFalse(queue.isEmpty());
        queue.enqueue("A", 1);
        assertFalse(queue.isEmpty());
        queue.enqueue("B", 1);
        assertFalse(queue.isEmpty());
        queue.enqueue("A", 2);
        assertFalse(queue.isEmpty());
        assertEquals("A", queue.take());
        assertFalse(queue.isEmpty());
        assertEquals("B", queue.take());
        assertFalse(queue.isEmpty());
        assertEquals("C", queue.take());
        assertTrue(queue.isEmpty());
    }
}
