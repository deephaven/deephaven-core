//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import org.jetbrains.annotations.NotNull;

public class TestSwitchColumnSource extends RefreshingTableTestCase {

    /**
     * A constant source whose fill and get contexts are its own: it rejects a context created by any other source, as
     * real implementations with typed contexts do (by casting).
     */
    private static final class TypedContextSource extends AbstractColumnSource<Integer>
            implements MutableColumnSourceGetDefaults.ForInt {

        private final class OwnFillContext implements ChunkSource.FillContext {
            private TypedContextSource owner() {
                return TypedContextSource.this;
            }
        }

        private final class OwnGetContext implements ChunkSource.GetContext {
            private final WritableIntChunk<Values> chunk;

            private OwnGetContext(final int chunkCapacity) {
                chunk = WritableIntChunk.makeWritableChunk(chunkCapacity);
            }

            private TypedContextSource owner() {
                return TypedContextSource.this;
            }

            @Override
            public void close() {
                chunk.close();
            }
        }

        private final int value;

        private TypedContextSource(final int value) {
            super(int.class);
            this.value = value;
        }

        @Override
        public int getInt(final long rowKey) {
            return value;
        }

        @Override
        public int getPrevInt(final long rowKey) {
            return value;
        }

        @Override
        public boolean isImmutable() {
            return true;
        }

        @Override
        public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            return new OwnFillContext();
        }

        @Override
        public GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
            return new OwnGetContext(chunkCapacity);
        }

        private void fill(@NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            destination.setSize(rowSequence.intSize());
            destination.asWritableIntChunk().fillWithValue(0, rowSequence.intSize(), value);
        }

        @Override
        public void fillChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination,
                @NotNull final RowSequence rowSequence) {
            assertSame(this, ((OwnFillContext) context).owner());
            fill(destination, rowSequence);
        }

        @Override
        public void fillPrevChunk(@NotNull final FillContext context,
                @NotNull final WritableChunk<? super Values> destination, @NotNull final RowSequence rowSequence) {
            fillChunk(context, destination, rowSequence);
        }

        @Override
        public Chunk<? extends Values> getChunk(@NotNull final GetContext context,
                @NotNull final RowSequence rowSequence) {
            final OwnGetContext own = (OwnGetContext) context;
            assertSame(this, own.owner());
            fill(own.chunk, rowSequence);
            return own.chunk;
        }

        @Override
        public Chunk<? extends Values> getPrevChunk(@NotNull final GetContext context,
                @NotNull final RowSequence rowSequence) {
            return getChunk(context, rowSequence);
        }
    }

    public void testContextsFollowDelegate() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final TypedContextSource first = new TypedContextSource(1);
        final TypedContextSource second = new TypedContextSource(2);
        final SwitchColumnSource<Integer> switchSource = new SwitchColumnSource<>(first);

        // A consumer may hold one switch context across delegate changes; the inner contexts must follow the delegate
        // rather than handing a context created for one implementation to another.
        try (final ChunkSource.FillContext fillContext = switchSource.makeFillContext(4, null);
                final ChunkSource.GetContext getContext = switchSource.makeGetContext(4, null);
                final WritableIntChunk<Values> destination = WritableIntChunk.makeWritableChunk(4);
                final RowSequence rows = RowSequenceFactory.forRange(0, 3)) {
            switchSource.fillChunk(fillContext, destination, rows);
            assertEquals(1, destination.get(0));
            assertEquals(1, switchSource.getChunk(getContext, rows).asIntChunk().get(0));

            updateGraph.runWithinUnitTestCycle(() -> {
                switchSource.setNewCurrent(second);

                // Current reads go to the new delegate; previous reads go to the old one.
                switchSource.fillChunk(fillContext, destination, rows);
                assertEquals(2, destination.get(0));
                switchSource.fillPrevChunk(fillContext, destination, rows);
                assertEquals(1, destination.get(0));
                assertEquals(2, switchSource.getChunk(getContext, rows).asIntChunk().get(0));
                assertEquals(1, switchSource.getPrevChunk(getContext, rows).asIntChunk().get(0));
            });

            // Once the cycle commits, previous reads fall through to the new delegate.
            switchSource.fillPrevChunk(fillContext, destination, rows);
            assertEquals(2, destination.get(0));
            assertEquals(2, switchSource.getPrevChunk(getContext, rows).asIntChunk().get(0));

            // Switching back reuses the (now stale) contexts again, in the other direction.
            updateGraph.runWithinUnitTestCycle(() -> {
                switchSource.setNewCurrent(first);
                switchSource.fillChunk(fillContext, destination, rows);
                assertEquals(1, destination.get(0));
                switchSource.fillPrevChunk(fillContext, destination, rows);
                assertEquals(2, destination.get(0));
            });
        }
    }
}
