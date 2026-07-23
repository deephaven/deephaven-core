//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.timsort;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.SortHelpers;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;

/**
 * Sorts chunks with several tasks: the data is split into segments that are sorted independently, and adjacent sorted
 * runs are merged pairwise (each merge submitted as soon as both of its inputs are complete) until a single sorted run
 * remains.
 *
 * <p>
 * {@link #sortDirect} sorts a values chunk and its parallel permutation chunk in place with the direct
 * {@link LongSortKernel} kernels. {@link #sortIndirect} sorts with a {@link MultiColumnSortKernel}, permuting only a
 * positions chunk and assembling the permuted row keys in a parallel linear pass at the end.
 *
 * <p>
 * Each task creates its own kernel context, sized to the range it sorts or merges. The calling thread blocks until the
 * sort is complete.
 */
public class ParallelTimsort {
    private ParallelTimsort() {}

    /** Sorts a segment of the data on a task thread, creating (and closing) its own kernel context. */
    private interface SegmentSorter {
        void sortSegment(int start, int length);
    }

    /** Merges two adjacent sorted runs on a task thread, creating (and closing) its own kernel context. */
    private interface RunMerger {
        void mergeRuns(int start1, int length1, int length2);
    }

    /** A node of the merge tree: a leaf sorts its range; an internal node merges its children's sorted runs. */
    private static final class MergeNode {
        /** The first chunk position of the range this node sorts or merges. */
        final int start;
        /**
         * The length of the left child's sorted run; the right child's run begins at start + leftLength. For a leaf,
         * the whole range.
         */
        final int leftLength;
        /** The total length of this node's range, spanning both children's runs for an internal node. */
        final int totalLength;
        /** The number of children not yet sorted; the task that decrements it to zero submits this node's merge. */
        final AtomicInteger pendingChildren;
        /**
         * The node that merges this node's sorted run with its sibling's; null for the root. Assigned while the tree is
         * built, strictly before any task is submitted; task submission publishes the completed tree to the sorting
         * threads.
         */
        MergeNode parent;

        MergeNode(final int start, final int leftLength, final int totalLength, final int pendingChildren) {
            this.start = start;
            this.leftLength = leftLength;
            this.totalLength = totalLength;
            this.pendingChildren = new AtomicInteger(pendingChildren);
        }
    }

    /**
     * Sort the values chunk in place with the direct sort kernels, permuting the valuesToPermute chunk in the same way,
     * using the given number of parallel segments.
     *
     * @param contextFactory creates a sort kernel context for a task, given the size of the range it operates on
     * @param valuesToPermute the values (typically row keys) permuted alongside the sorted values
     * @param valuesToSort the values to sort
     * @param segments the number of segments to sort independently; must be at least two
     */
    public static <SORT_VALUES_ATTR extends Any, PERMUTE_VALUES_ATTR extends Any> void sortDirect(
            final IntFunction<LongSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR>> contextFactory,
            final WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            final WritableChunk<SORT_VALUES_ATTR> valuesToSort,
            final int segments) {
        sortTree(valuesToPermute.size(), segments,
                (start, length) -> {
                    try (final LongSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context =
                            contextFactory.apply(length)) {
                        context.sort(valuesToPermute, valuesToSort, start, length);
                    }
                },
                (start1, length1, length2) -> {
                    try (final LongSortKernel<SORT_VALUES_ATTR, PERMUTE_VALUES_ATTR> context =
                            contextFactory.apply(length1 + length2)) {
                        context.merge(valuesToPermute, valuesToSort, start1, length1, length2);
                    }
                });
    }

    /**
     * Sort the values in the valuesToSort chunks (comparing each column in turn), permuting the valuesToPermute chunk
     * in the same way, using the given number of parallel segments. Only a positions chunk moves during the sort; the
     * permuted row keys are assembled in a parallel linear pass at the end.
     *
     * @param contextFactory creates a sort kernel context for a task, given the size of the range it operates on
     * @param valuesToPermute the row keys permuted alongside the sort key columns
     * @param valuesToSort one chunk per sort key column
     * @param segments the number of segments to sort independently; must be at least two
     */
    public static <PERMUTE_VALUES_ATTR extends Any> void sortIndirect(
            final IntFunction<MultiColumnSortKernel<PERMUTE_VALUES_ATTR>> contextFactory,
            final WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            final WritableChunk<? extends Any>[] valuesToSort,
            final int segments) {
        final int size = valuesToPermute.size();
        try (final WritableIntChunk<ChunkPositions> positions = WritableIntChunk.makeWritableChunk(size)) {
            positions.setSize(size);
            sortTree(size, segments,
                    (start, length) -> {
                        try (final MultiColumnSortKernel<PERMUTE_VALUES_ATTR> context =
                                contextFactory.apply(length)) {
                            for (int ii = start; ii < start + length; ++ii) {
                                positions.set(ii, ii);
                            }
                            context.sortPositions(positions, valuesToSort, start, length);
                        }
                    },
                    (start1, length1, length2) -> {
                        try (final MultiColumnSortKernel<PERMUTE_VALUES_ATTR> context =
                                contextFactory.apply(length1 + length2)) {
                            context.mergePositions(positions, valuesToSort, start1, length1, length2);
                        }
                    });
            gatherRowKeysParallel(valuesToPermute, positions, size, segments);
        }
    }

    /**
     * Run the segment sorts and the pairwise merge tree over them, blocking until the root merge is complete.
     */
    private static void sortTree(final int size, final int segments, final SegmentSorter segmentSorter,
            final RunMerger runMerger) {
        final int segmentSize = (size + segments - 1) / segments;

        final ExecutionContext executionContext = ExecutionContext.getContext();
        // The tasks must run on the operation initializer thread pool, even when the sort is initiated from a
        // listener on an update graph thread. Because we block the initiating thread on a future, running the tasks
        // on the update graph's own threads would deadlock: concurrent sorts could block every update thread, and
        // the tasks that would complete their futures could then never be scheduled. That never happens with the
        // operation initializer pool: its workers report canParallelize() == false, so a sort initiated on a worker
        // runs serially rather than submitting and waiting, and the merge tree's tasks never block (the last child
        // to finish submits the parent merge). Restructuring the sort listener to continue from a completion routine
        // instead of blocking would let cycle-time sorts use the update graph's scheduler, and would be better in
        // the long term.
        final JobScheduler jobScheduler = new OperationInitializerJobScheduler();
        final CompletableFuture<Void> waitForSort = new CompletableFuture<>();

        // build the merge tree over the leaf segments, pairing adjacent runs level by level; an odd node is carried
        // up to the next level unmerged
        final List<MergeNode> leaves = buildMergeTree(size, segments, segmentSize);

        for (final MergeNode leaf : leaves) {
            jobScheduler.submit(
                    executionContext,
                    () -> {
                        if (waitForSort.isDone()) {
                            return; // a sibling task already failed
                        }
                        segmentSorter.sortSegment(leaf.start, leaf.totalLength);
                        completeNode(leaf, runMerger, jobScheduler, executionContext, waitForSort);
                    },
                    logOutput -> logOutput.append("ParallelTimsort.segmentSort"),
                    waitForSort::completeExceptionally);
        }

        try {
            waitForSort.get();
        } catch (final InterruptedException e) {
            throw new CancellationException("Interrupted during parallel sort");
        } catch (final ExecutionException e) {
            throw new UncheckedDeephavenException("Exception during parallel sort", e.getCause());
        } finally {
            SortHelpers.accumulateSchedulerPerformance(jobScheduler);
        }
    }

    private static List<MergeNode> buildMergeTree(final int size, final int segments, final int segmentSize) {
        final List<MergeNode> leaves = new ArrayList<>(segments);
        for (int segment = 0; segment < segments; ++segment) {
            final int start = segment * segmentSize;
            final int length = Math.min(segmentSize, size - start);
            leaves.add(new MergeNode(start, length, length, 0));
        }
        List<MergeNode> level = leaves;
        while (level.size() > 1) {
            final List<MergeNode> next = new ArrayList<>((level.size() + 1) / 2);
            for (int ii = 0; ii + 1 < level.size(); ii += 2) {
                final MergeNode left = level.get(ii);
                final MergeNode right = level.get(ii + 1);
                final MergeNode parent =
                        new MergeNode(left.start, left.totalLength, left.totalLength + right.totalLength, 2);
                left.parent = parent;
                right.parent = parent;
                next.add(parent);
            }
            if (level.size() % 2 == 1) {
                next.add(level.get(level.size() - 1));
            }
            level = next;
        }
        return leaves;
    }

    private static void completeNode(
            final MergeNode node,
            final RunMerger runMerger,
            final JobScheduler jobScheduler,
            final ExecutionContext executionContext,
            final CompletableFuture<Void> waitForSort) {
        final MergeNode parent = node.parent;
        if (parent == null) {
            waitForSort.complete(null);
            return;
        }
        if (parent.pendingChildren.decrementAndGet() > 0) {
            return;
        }
        jobScheduler.submit(
                executionContext,
                () -> {
                    if (waitForSort.isDone()) {
                        return; // a sibling task already failed
                    }
                    runMerger.mergeRuns(parent.start, parent.leftLength, parent.totalLength - parent.leftLength);
                    completeNode(parent, runMerger, jobScheduler, executionContext, waitForSort);
                },
                logOutput -> logOutput.append("ParallelTimsort.merge"),
                waitForSort::completeExceptionally);
    }

    /** A task's share of a parallel phase over the chunk, given as a [start, end) range of chunk positions. */
    private interface SegmentTask {
        void run(int start, int end);
    }

    /** Assemble the permuted row keys in a single linear pass, one segment per task. */
    private static <PERMUTE_VALUES_ATTR extends Any> void gatherRowKeysParallel(
            final WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            final WritableIntChunk<ChunkPositions> positions,
            final int size,
            final int segments) {
        // the gather must not reuse the sorting scheduler: reading its accumulated performance waits for its jobs,
        // so it is consumed once, after the sort completes. It must be an operation initializer scheduler, not the
        // update graph's; see sortTree.
        final JobScheduler jobScheduler = new OperationInitializerJobScheduler();
        final int segmentSize = (size + segments - 1) / segments;
        try (final WritableLongChunk<PERMUTE_VALUES_ATTR> originalKeys = WritableLongChunk.makeWritableChunk(size)) {
            try {
                // the gather reads the copied keys at arbitrary positions, so the copy phase must be complete
                // before any gather segment begins
                runPhase(jobScheduler, "ParallelTimsort.copyRowKeys", size, segments, segmentSize,
                        (start, end) -> originalKeys.copyFromChunk(valuesToPermute, start, start, end - start));
                runPhase(jobScheduler, "ParallelTimsort.gatherRowKeys", size, segments, segmentSize,
                        (start, end) -> {
                            for (int ii = start; ii < end; ++ii) {
                                valuesToPermute.set(ii, originalKeys.get(positions.get(ii)));
                            }
                        });
            } finally {
                SortHelpers.accumulateSchedulerPerformance(jobScheduler);
            }
        }
    }

    /** Run one phase of segment tasks over the chunk, blocking until every segment is complete. */
    private static void runPhase(final JobScheduler jobScheduler, final String description, final int size,
            final int segments, final int segmentSize, final SegmentTask task) {
        final CompletableFuture<Void> waitForPhase = new CompletableFuture<>();
        jobScheduler.iterateParallel(
                ExecutionContext.getContext(),
                logOutput -> logOutput.append(description),
                JobScheduler.DEFAULT_CONTEXT_FACTORY,
                0, segments,
                (context, segment, nestedErrorConsumer) -> {
                    final int start = segment * segmentSize;
                    final int end = Math.min(start + segmentSize, size);
                    task.run(start, end);
                },
                () -> waitForPhase.complete(null),
                () -> {
                },
                waitForPhase::completeExceptionally);
        try {
            waitForPhase.get();
        } catch (final InterruptedException e) {
            throw new CancellationException("Interrupted during " + description);
        } catch (final ExecutionException e) {
            throw new UncheckedDeephavenException("Exception during " + description, e.getCause());
        }
    }
}
