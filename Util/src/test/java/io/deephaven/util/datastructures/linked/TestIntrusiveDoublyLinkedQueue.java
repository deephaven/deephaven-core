package io.deephaven.util.datastructures.linked;

import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        TestCase.assertTrue(queue.isEmpty());
        TestCase.assertNull(queue.peek());
        TestCase.assertNull(queue.poll());
        try {
            queue.remove();
            TestCase.fail("Expected exception");
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
            TestCase.assertEquals(ti++, node.value);
        }

        ti = 0;
        while (!queue.isEmpty()) {
            TestCase.assertEquals(ti++, queue.remove().value);
        }
        TestCase.assertEquals(nodeCount, ti);
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
            TestCase.assertSame(node, qi.next());
        }
        TestCase.assertFalse(qi.hasNext());

        int ti = 0;
        while (!queue.isEmpty()) {
            TestCase.assertEquals(ti++, queue.remove().value);
        }
        TestCase.assertEquals(nodes.size(), ti);

        // noinspection unchecked
        for (final Predicate<IntNode> predicate : new Predicate[] {n -> ((IntNode) n).value % 2 == 0,
                n -> ((IntNode) n).value % 3 == 0, n -> ((IntNode) n).value % 4 == 0}) {

            final Map<Boolean, List<IntNode>> partitioned =
                    nodes.stream().collect(Collectors.partitioningBy(predicate));

            for (final boolean partitionToKeep : new boolean[] {false, true}) {
                // Put all nodes in
                nodes.forEach(queue::offer);

                // Remove half the nodes
                partitioned.get(!partitionToKeep).forEach(n -> TestCase.assertTrue(queue.remove(n)));
                partitioned.get(!partitionToKeep).forEach(n -> TestCase.assertFalse(queue.isLinked(n)));
                partitioned.get(!partitionToKeep).forEach(n -> TestCase.assertFalse(queue.contains(n)));
                partitioned.get(!partitionToKeep).forEach(n -> TestCase.assertFalse(queue.remove(n)));

                // Make sure contains only the other half
                qi = queue.iterator();
                for (final IntNode keptNode : partitioned.get(partitionToKeep)) {
                    TestCase.assertSame(keptNode, qi.next());
                }
                TestCase.assertFalse(qi.hasNext());

                // Remove the kept half
                int rni = 0;
                for (final IntNode keptNode : partitioned.get(partitionToKeep)) {
                    rni++;
                    TestCase.assertSame(keptNode, queue.remove());
                }
                TestCase.assertEquals(partitioned.get(partitionToKeep).size(), rni);
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
        TestCase.assertTrue(queue1.isEmpty());

        queue2.transferBeforeHeadFrom(queue1);
        TestCase.assertTrue(queue2.isEmpty());

        queue1.transferAfterTailFrom(queue2);
        TestCase.assertTrue(queue1.isEmpty());

        queue2.transferAfterTailFrom(queue1);
        TestCase.assertTrue(queue2.isEmpty());

        queue2.offer(new IntNode(0));
        queue1.transferAfterTailFrom(queue2);
        TestCase.assertTrue(queue2.isEmpty());
        TestCase.assertFalse(queue1.isEmpty());
        TestCase.assertEquals(0, queue1.peek().value);
        TestCase.assertEquals(0, queue1.poll().value);
        TestCase.assertTrue(queue1.isEmpty());

        queue2.offer(new IntNode(0));
        queue1.transferAfterTailFrom(queue2);
        queue2.transferAfterTailFrom(queue1);
        TestCase.assertTrue(queue1.isEmpty());
        TestCase.assertFalse(queue2.isEmpty());
        TestCase.assertEquals(0, queue2.peek().value);
        TestCase.assertEquals(0, queue2.poll().value);
        TestCase.assertTrue(queue2.isEmpty());

        IntStream.range(0, 3).forEach(i -> queue2.offer(new IntNode(i)));
        queue1.transferAfterTailFrom(queue2);
        TestCase.assertTrue(queue2.isEmpty());
        TestCase.assertFalse(queue1.isEmpty());
        IntStream.range(0, 3).forEach(i -> TestCase.assertEquals(i, queue1.poll().value));
        TestCase.assertTrue(queue1.isEmpty());

        IntStream.range(0, 3).forEach(i -> queue2.offer(new IntNode(i)));
        queue1.transferAfterTailFrom(queue2);
        queue2.transferAfterTailFrom(queue1);
        TestCase.assertTrue(queue1.isEmpty());
        TestCase.assertFalse(queue2.isEmpty());
        IntStream.range(0, 3).forEach(i -> TestCase.assertEquals(i, queue2.poll().value));
        TestCase.assertTrue(queue2.isEmpty());

        IntStream.range(0, 2).forEach(i -> queue1.offer(new IntNode(i)));
        IntStream.range(2, 7).forEach(i -> queue2.offer(new IntNode(i)));
        queue1.transferAfterTailFrom(queue2);
        TestCase.assertTrue(queue2.isEmpty());
        TestCase.assertFalse(queue1.isEmpty());
        IntStream.range(0, 7).forEach(i -> TestCase.assertEquals(i, queue1.poll().value));
        TestCase.assertTrue(queue1.isEmpty());

        IntStream.range(2, 7).forEach(i -> queue1.offer(new IntNode(i)));
        IntStream.range(0, 2).forEach(i -> queue2.offer(new IntNode(i)));
        queue1.transferBeforeHeadFrom(queue2);
        TestCase.assertTrue(queue2.isEmpty());
        TestCase.assertFalse(queue1.isEmpty());
        IntStream.range(0, 7).forEach(i -> TestCase.assertEquals(i, queue1.poll().value));
        TestCase.assertTrue(queue1.isEmpty());
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testInsert() {
        final IntrusiveDoublyLinkedQueue<IntNode> queue = new IntrusiveDoublyLinkedQueue<>(new IntNodeAdapter());
        for (int at : new int[] {-1, 1, 100}) {
            try {
                queue.insert(new IntNode(0), at);
                TestCase.fail("Unexpectedly succeeded in inserting at " + at + " in an empty queue");
            } catch (IllegalArgumentException expected) {
            }
        }
        TestCase.assertTrue(queue.isEmpty());

        queue.insert(new IntNode(0), 0);
        TestCase.assertFalse(queue.isEmpty());
        TestCase.assertEquals(1, queue.size());
        TestCase.assertEquals(0, queue.peek().value);
        for (int at : new int[] {-1, 2, 100}) {
            try {
                queue.insert(new IntNode(2), at);
                TestCase.fail("Unexpectedly succeeded in inserting at " + at + " in queue with size=1");
            } catch (IllegalArgumentException expected) {
            }
        }
        TestCase.assertEquals(1, queue.size());

        queue.insert(new IntNode(1), 1);
        TestCase.assertEquals(2, queue.size());
        for (int at : new int[] {-1, 3, 100}) {
            try {
                queue.insert(new IntNode(3), at);
                TestCase.fail("Unexpectedly succeeded in inserting at " + at + " in queue with size=2");
            } catch (IllegalArgumentException expected) {
            }
        }
        TestCase.assertEquals(2, queue.size());

        IntStream.range(0, 2).forEach(i -> TestCase.assertEquals(i, queue.poll().value));

        queue.insert(new IntNode(1), 0);
        queue.insert(new IntNode(3), 1);
        queue.insert(new IntNode(0), 0);
        queue.insert(new IntNode(2), 2);
        queue.insert(new IntNode(4), 4);
        IntStream.range(0, 5).forEach(i -> TestCase.assertEquals(i, queue.poll().value));
    }
}
