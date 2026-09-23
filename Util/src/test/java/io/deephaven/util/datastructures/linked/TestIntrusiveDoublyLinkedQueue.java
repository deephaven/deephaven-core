//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.linked;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

/**
 * Unit tests for {@link IntrusiveDoublyLinkedQueue}.
 */
public class TestIntrusiveDoublyLinkedQueue {

    private static class IntNode {

        private IntNode next = this;
        private IntNode prev = this;

        private final int value;

        private IntNode(final int value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return Integer.toString(value);
        }
    }

    private static class IntNodeAdapter implements IntrusiveDoublyLinkedStructureBase.Adapter<IntNode> {

        @NotNull
        @Override
        public IntNode getNext(@NotNull final IntNode node) {
            return node.next;
        }

        @Override
        public void setNext(@NotNull final IntNode node, @NotNull final IntNode other) {
            node.next = other;
        }

        @NotNull
        @Override
        public IntNode getPrev(@NotNull final IntNode node) {
            return node.prev;
        }

        @Override
        public void setPrev(@NotNull final IntNode node, @NotNull final IntNode other) {
            node.prev = other;
        }
    }

    @Test
    public void testEmpty() {
        final IntrusiveDoublyLinkedQueue<IntNode> queue = new IntrusiveDoublyLinkedQueue<>(new IntNodeAdapter());
        assertTrue(queue.isEmpty());
        assertNull(queue.peek());
        assertNull(queue.poll());
        try {
            queue.remove();
            fail("Expected exception");
        } catch (NoSuchElementException ignored) {
        }
    }

    /**
     * Test straightforward usage as a queue, with only adds at the end and removes from the beginning.
     */
    @Test
    public void testSimple() {
        IntStream.rangeClosed(0, 20).forEach(this::doSimpleTest);
    }

    private void doSimpleTest(final int nodeCount) {
        final IntrusiveDoublyLinkedQueue<IntNode> queue = new IntrusiveDoublyLinkedQueue<>(new IntNodeAdapter());
        for (int ni = 0; ni < nodeCount; ++ni) {
            queue.offer(new IntNode(ni));
        }

        int ti = 0;
        for (final IntNode node : queue) {
            assertEquals(ti++, node.value);
        }

        ti = 0;
        while (!queue.isEmpty()) {
            assertEquals(ti++, queue.remove().value);
        }
        assertEquals(nodeCount, ti);
    }

    /**
     * Test "fancy" usage as a queue, with adds at the end, removes from the beginning, and O(1) internal removes.
     */
    @Test
    public void testIntrusiveRemoves() {
        IntStream.rangeClosed(0, 20).forEach(this::doIntrusiveRemoveTest);
    }

    @SuppressWarnings("AutoBoxing")
    private void doIntrusiveRemoveTest(final int nodeCount) {
        final IntrusiveDoublyLinkedQueue<IntNode> queue = new IntrusiveDoublyLinkedQueue<>(new IntNodeAdapter());
        final List<IntNode> nodes = IntStream.range(0, nodeCount).mapToObj(IntNode::new).collect(Collectors.toList());

        nodes.forEach(queue::offer);
        Iterator<IntNode> qi = queue.iterator();
        for (final IntNode node : nodes) {
            assertSame(node, qi.next());
        }
        assertFalse(qi.hasNext());

        int ti = 0;
        while (!queue.isEmpty()) {
            assertEquals(ti++, queue.remove().value);
        }
        assertEquals(nodes.size(), ti);

        // noinspection unchecked
        for (final Predicate<IntNode> predicate : new Predicate[] {n -> ((IntNode) n).value % 2 == 0,
                n -> ((IntNode) n).value % 3 == 0, n -> ((IntNode) n).value % 4 == 0}) {

            final Map<Boolean, List<IntNode>> partitioned =
                    nodes.stream().collect(Collectors.partitioningBy(predicate));

            for (final boolean partitionToKeep : new boolean[] {false, true}) {
                // Put all nodes in
                nodes.forEach(queue::offer);

                // Remove half the nodes
                partitioned.get(!partitionToKeep).forEach(n -> assertTrue(queue.remove(n)));
                partitioned.get(!partitionToKeep).forEach(n -> assertFalse(queue.isLinked(n)));
                partitioned.get(!partitionToKeep).forEach(n -> assertFalse(queue.contains(n)));
                partitioned.get(!partitionToKeep).forEach(n -> assertFalse(queue.remove(n)));

                // Make sure contains only the other half
                qi = queue.iterator();
                for (final IntNode keptNode : partitioned.get(partitionToKeep)) {
                    assertSame(keptNode, qi.next());
                }
                assertFalse(qi.hasNext());

                // Remove the kept half
                int rni = 0;
                for (final IntNode keptNode : partitioned.get(partitionToKeep)) {
                    rni++;
                    assertSame(keptNode, queue.remove());
                }
                assertEquals(partitioned.get(partitionToKeep).size(), rni);
            }
        }
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testTransferFrom() {
        final IntNodeAdapter adapter = new IntNodeAdapter();
        final IntrusiveDoublyLinkedQueue<IntNode> queue1 = new IntrusiveDoublyLinkedQueue<>(adapter);
        final IntrusiveDoublyLinkedQueue<IntNode> queue2 = new IntrusiveDoublyLinkedQueue<>(adapter);

        queue1.transferBeforeHeadFrom(queue2);
        assertTrue(queue1.isEmpty());

        queue2.transferBeforeHeadFrom(queue1);
        assertTrue(queue2.isEmpty());

        queue1.transferAfterTailFrom(queue2);
        assertTrue(queue1.isEmpty());

        queue2.transferAfterTailFrom(queue1);
        assertTrue(queue2.isEmpty());

        queue2.offer(new IntNode(0));
        queue1.transferAfterTailFrom(queue2);
        assertTrue(queue2.isEmpty());
        assertFalse(queue1.isEmpty());
        assertEquals(0, queue1.peek().value);
        assertEquals(0, queue1.poll().value);
        assertTrue(queue1.isEmpty());

        queue2.offer(new IntNode(0));
        queue1.transferAfterTailFrom(queue2);
        queue2.transferAfterTailFrom(queue1);
        assertTrue(queue1.isEmpty());
        assertFalse(queue2.isEmpty());
        assertEquals(0, queue2.peek().value);
        assertEquals(0, queue2.poll().value);
        assertTrue(queue2.isEmpty());

        IntStream.range(0, 3).forEach(i -> queue2.offer(new IntNode(i)));
        queue1.transferAfterTailFrom(queue2);
        assertTrue(queue2.isEmpty());
        assertFalse(queue1.isEmpty());
        IntStream.range(0, 3).forEach(i -> assertEquals(i, queue1.poll().value));
        assertTrue(queue1.isEmpty());

        IntStream.range(0, 3).forEach(i -> queue2.offer(new IntNode(i)));
        queue1.transferAfterTailFrom(queue2);
        queue2.transferAfterTailFrom(queue1);
        assertTrue(queue1.isEmpty());
        assertFalse(queue2.isEmpty());
        IntStream.range(0, 3).forEach(i -> assertEquals(i, queue2.poll().value));
        assertTrue(queue2.isEmpty());

        IntStream.range(0, 2).forEach(i -> queue1.offer(new IntNode(i)));
        IntStream.range(2, 7).forEach(i -> queue2.offer(new IntNode(i)));
        queue1.transferAfterTailFrom(queue2);
        assertTrue(queue2.isEmpty());
        assertFalse(queue1.isEmpty());
        IntStream.range(0, 7).forEach(i -> assertEquals(i, queue1.poll().value));
        assertTrue(queue1.isEmpty());

        IntStream.range(2, 7).forEach(i -> queue1.offer(new IntNode(i)));
        IntStream.range(0, 2).forEach(i -> queue2.offer(new IntNode(i)));
        queue1.transferBeforeHeadFrom(queue2);
        assertTrue(queue2.isEmpty());
        assertFalse(queue1.isEmpty());
        IntStream.range(0, 7).forEach(i -> assertEquals(i, queue1.poll().value));
        assertTrue(queue1.isEmpty());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testInsert() {
        final IntrusiveDoublyLinkedQueue<IntNode> queue = new IntrusiveDoublyLinkedQueue<>(new IntNodeAdapter());
        for (int at : new int[] {-1, 1, 100}) {
            try {
                queue.insert(new IntNode(0), at);
                fail("Unexpectedly succeeded in inserting at " + at + " in an empty queue");
            } catch (IllegalArgumentException expected) {
            }
        }
        assertTrue(queue.isEmpty());

        queue.insert(new IntNode(0), 0);
        assertFalse(queue.isEmpty());
        assertEquals(1, queue.size());
        assertEquals(0, queue.peek().value);
        for (int at : new int[] {-1, 2, 100}) {
            try {
                queue.insert(new IntNode(2), at);
                fail("Unexpectedly succeeded in inserting at " + at + " in queue with size=1");
            } catch (IllegalArgumentException expected) {
            }
        }
        assertEquals(1, queue.size());

        queue.insert(new IntNode(1), 1);
        assertEquals(2, queue.size());
        for (int at : new int[] {-1, 3, 100}) {
            try {
                queue.insert(new IntNode(3), at);
                fail("Unexpectedly succeeded in inserting at " + at + " in queue with size=2");
            } catch (IllegalArgumentException expected) {
            }
        }
        assertEquals(2, queue.size());

        IntStream.range(0, 2).forEach(i -> assertEquals(i, queue.poll().value));

        queue.insert(new IntNode(1), 0);
        queue.insert(new IntNode(3), 1);
        queue.insert(new IntNode(0), 0);
        queue.insert(new IntNode(2), 2);
        queue.insert(new IntNode(4), 4);
        IntStream.range(0, 5).forEach(i -> assertEquals(i, queue.poll().value));
    }
}
