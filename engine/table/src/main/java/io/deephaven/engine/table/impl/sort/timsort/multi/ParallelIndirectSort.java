//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.timsort.multi;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.SortHelpers;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.IntFunction;

/**
 * Sorts the positions of a set of parallel value chunks using several tasks: the positions chunk is split into segments
 * that are sorted independently, and adjacent sorted runs are merged pairwise (each merge submitted as soon as both of
 * its inputs are complete) until a single sorted run remains. The permuted row keys are then assembled in a parallel
 * linear pass, one segment per task.
 *
 * <p>
 * Each task creates its own {@link MultiColumnSortKernel} context, sized to the range it sorts or merges; the contexts
 * only allocate scratch space (the whole-chunk buffers of a context are lazily allocated by the serial entry point,
 * which the driver never calls). The calling thread blocks until the sort is complete.
 */
public class ParallelIndirectSort {
    private ParallelIndirectSort() {}

    /** A node of the merge tree: a leaf sorts its range; an internal node merges its children's sorted runs. */
    private static final class MergeNode {
        final int start;
        final int leftLength;
        final int totalLength;
        final AtomicInteger pendingChildren;
        /**
         * Assigned while the tree is built, strictly before any task is submitted; task submission publishes the
         * completed tree to the sorting threads.
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
     * Sort the values in the valuesToSort chunks (comparing each column in turn), permuting the valuesToPermute chunk
     * in the same way, using the given number of parallel segments.
     *
     * @param contextFactory creates a sort kernel context for a task, given the size of the range it operates on
     * @param valuesToPermute the row keys permuted alongside the sort key columns
     * @param valuesToSort one chunk per sort key column
     * @param segments the number of segments to sort independently; must be at least two
     */
    public static <PERMUTE_VALUES_ATTR extends Any> void sort(
            final IntFunction<MultiColumnSortKernel<PERMUTE_VALUES_ATTR>> contextFactory,
            final WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            final WritableChunk<? extends Any>[] valuesToSort,
            final int segments) {
        final int size = valuesToPermute.size();
        final int segmentSize = (size + segments - 1) / segments;

        final ExecutionContext executionContext = ExecutionContext.getContext();
        final JobScheduler jobScheduler = new OperationInitializerJobScheduler();
        final CompletableFuture<Void> waitForSort = new CompletableFuture<>();
        final Consumer<Exception> onError = waitForSort::completeExceptionally;

        try (final WritableIntChunk<ChunkPositions> positions = WritableIntChunk.makeWritableChunk(size)) {
            positions.setSize(size);

            // build the merge tree over the leaf segments, pairing adjacent runs level by level; an odd node is
            // carried up to the next level unmerged
            final List<MergeNode> leaves = buildMergeTree(size, segments, segmentSize);

            for (final MergeNode leaf : leaves) {
                jobScheduler.submit(
                        executionContext,
                        () -> {
                            if (waitForSort.isDone()) {
                                return; // a sibling task already failed
                            }
                            try (final MultiColumnSortKernel<PERMUTE_VALUES_ATTR> context =
                                    contextFactory.apply(leaf.totalLength)) {
                                for (int ii = leaf.start; ii < leaf.start + leaf.totalLength; ++ii) {
                                    positions.set(ii, ii);
                                }
                                context.sortPositions(positions, valuesToSort, leaf.start, leaf.totalLength);
                            }
                            completeNode(leaf, positions, valuesToSort, contextFactory, jobScheduler,
                                    executionContext, waitForSort, onError);
                        },
                        logOutput -> logOutput.append("ParallelIndirectSort.segmentSort"),
                        onError);
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

            gatherRowKeysParallel(valuesToPermute, positions, size, segments, executionContext);
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

    private static <PERMUTE_VALUES_ATTR extends Any> void completeNode(
            final MergeNode node,
            final WritableIntChunk<ChunkPositions> positions,
            final WritableChunk<? extends Any>[] valuesToSort,
            final IntFunction<MultiColumnSortKernel<PERMUTE_VALUES_ATTR>> contextFactory,
            final JobScheduler jobScheduler,
            final ExecutionContext executionContext,
            final CompletableFuture<Void> waitForSort,
            final Consumer<Exception> onError) {
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
                    try (final MultiColumnSortKernel<PERMUTE_VALUES_ATTR> context =
                            contextFactory.apply(parent.totalLength)) {
                        context.mergePositions(positions, valuesToSort, parent.start, parent.leftLength,
                                parent.totalLength - parent.leftLength);
                    }
                    completeNode(parent, positions, valuesToSort, contextFactory, jobScheduler, executionContext,
                            waitForSort, onError);
                },
                logOutput -> logOutput.append("ParallelIndirectSort.merge"),
                onError);
    }

    /** Assemble the permuted row keys in a single linear pass, one segment per task. */
    private static <PERMUTE_VALUES_ATTR extends Any> void gatherRowKeysParallel(
            final WritableLongChunk<PERMUTE_VALUES_ATTR> valuesToPermute,
            final WritableIntChunk<ChunkPositions> positions,
            final int size,
            final int segments,
            final ExecutionContext executionContext) {
        // the gather must not reuse the sorting scheduler: reading its accumulated performance waits for its jobs,
        // so it is consumed once, after the sort completes
        final JobScheduler jobScheduler = new OperationInitializerJobScheduler();
        final int segmentSize = (size + segments - 1) / segments;
        try (final WritableLongChunk<PERMUTE_VALUES_ATTR> originalKeys = WritableLongChunk.makeWritableChunk(size)) {
            originalKeys.copyFromChunk(valuesToPermute, 0, 0, size);
            final CompletableFuture<Void> waitForGather = new CompletableFuture<>();
            jobScheduler.iterateParallel(
                    executionContext,
                    logOutput -> logOutput.append("ParallelIndirectSort.gatherRowKeys"),
                    JobScheduler.DEFAULT_CONTEXT_FACTORY,
                    0, segments,
                    (context, segment, nestedErrorConsumer) -> {
                        final int start = segment * segmentSize;
                        final int end = Math.min(start + segmentSize, size);
                        for (int ii = start; ii < end; ++ii) {
                            valuesToPermute.set(ii, originalKeys.get(positions.get(ii)));
                        }
                    },
                    () -> waitForGather.complete(null),
                    () -> {
                    },
                    waitForGather::completeExceptionally);
            try {
                waitForGather.get();
            } catch (final InterruptedException e) {
                throw new CancellationException("Interrupted during parallel sort row key gather");
            } catch (final ExecutionException e) {
                throw new UncheckedDeephavenException("Exception during parallel sort row key gather", e.getCause());
            } finally {
                SortHelpers.accumulateSchedulerPerformance(jobScheduler);
            }
        }
    }
}
