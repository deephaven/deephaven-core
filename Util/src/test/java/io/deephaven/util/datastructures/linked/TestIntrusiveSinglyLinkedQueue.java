//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.linked;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class TestIntrusiveSinglyLinkedQueue {
    private static class IntNode {

        private TestIntrusiveSinglyLinkedQueue.IntNode next = this;

        private final int value;

        private IntNode(final int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return Integer.toString(value);
        }
    }

    private static class IntNodeAdapter
            implements IntrusiveSinglyLinkedQueue.Adapter<TestIntrusiveSinglyLinkedQueue.IntNode> {

        @Override
        public TestIntrusiveSinglyLinkedQueue.IntNode getNext(
                @NotNull final TestIntrusiveSinglyLinkedQueue.IntNode node) {
            return node.next;
        }

        @Override
        public void setNext(@NotNull final TestIntrusiveSinglyLinkedQueue.IntNode node,
                final TestIntrusiveSinglyLinkedQueue.IntNode other) {
            node.next = other;
        }
    }

    @Test
    public void testEmpty() {
        final IntrusiveSinglyLinkedQueue<TestIntrusiveSinglyLinkedQueue.IntNode> queue =
                new IntrusiveSinglyLinkedQueue<>(new TestIntrusiveSinglyLinkedQueue.IntNodeAdapter());
        assertTrue(queue.isEmpty());
        assertNull(queue.peek());
        assertNull(queue.poll());

        final IntNode node = new IntNode(42);
        assertTrue(queue.offer(node));
        assertEquals(node, queue.peek());
        assertEquals(node, queue.poll());

        assertTrue(queue.isEmpty());
        assertNull(queue.peek());
        assertNull(queue.poll());
    }

    /**
     * Test straightforward usage as a queue, with only adds at the end and removes from the beginning.
     */
    @Test
    public void testSimple() {
        IntStream.rangeClosed(0, 20).forEach(this::doSimpleTest);
    }

    private void doSimpleTest(final int nodeCount) {
        final IntrusiveSinglyLinkedQueue<TestIntrusiveSinglyLinkedQueue.IntNode> queue =
                new IntrusiveSinglyLinkedQueue<>(new TestIntrusiveSinglyLinkedQueue.IntNodeAdapter());
        for (int ni = 0; ni < nodeCount; ++ni) {
            queue.offer(new TestIntrusiveSinglyLinkedQueue.IntNode(ni));
        }

        int ti = 0;
        while (!queue.isEmpty()) {
            assertEquals(ti++, queue.poll().value);
        }
        assertEquals(nodeCount, ti);
    }
}
