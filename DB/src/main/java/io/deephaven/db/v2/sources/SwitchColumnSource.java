package io.deephaven.db.v2.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.UpdateCommitter;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

public class SwitchColumnSource<T> extends AbstractColumnSource<T> {

    private final UpdateCommitter<SwitchColumnSource<T>> updateCommitter;
    private final Consumer<ColumnSource<T>> onPreviousCommitted;

    private ColumnSource<T> currentSource;
    private ColumnSource<T> prevSource;
    private long prevValidityStep = -1;

    public SwitchColumnSource(ColumnSource<T> currentSource) {
        this(currentSource, null);
    }

    public SwitchColumnSource(@NotNull final ColumnSource<T> currentSource,
            @Nullable final Consumer<ColumnSource<T>> onPreviousCommitted) {
        super(currentSource.getType(), currentSource.getComponentType());
        this.updateCommitter = new UpdateCommitter<>(this, SwitchColumnSource::clearPrevious);
        this.onPreviousCommitted = onPreviousCommitted;
        this.currentSource = currentSource;
    }

    private void clearPrevious() {
        final ColumnSource<T> captured = prevSource;
        prevValidityStep = -1;
        prevSource = null;
        if (onPreviousCommitted != null) {
            onPreviousCommitted.accept(captured);
        }
    }

    public void setNewCurrent(ColumnSource<T> newCurrent) {
        Assert.eq(newCurrent.getType(), "newCurrent.getType()", getType(), "getType()");
        Assert.eq(newCurrent.getComponentType(), "newCurrent.getComponentType()", getComponentType(),
                "getComponentType()");
        prevSource = currentSource;
        prevValidityStep = LogicalClock.DEFAULT.currentStep();
        currentSource = newCurrent;
        updateCommitter.maybeActivate();
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    private abstract class SwitchContext<CT extends Context> implements Context {

        final int chunkCapacity;
        final SharedContext sharedContext;

        private CT currentContext;
        private CT prevContext;

        private SwitchContext(final int chunkCapacity, final SharedContext sharedContext) {
            this.chunkCapacity = Require.geqZero(chunkCapacity, "chunkCapacity");
            this.sharedContext = sharedContext;
        }

        abstract CT makeContext(@NotNull final ColumnSource innerSource);

        public CT getCurrentContext() {
            return currentContext == null
                    ? currentContext = makeContext(currentSource)
                    : currentContext;
        }

        public CT getPrevContext() {
            return prevInvalid()
                    ? getCurrentContext()
                    : prevContext == null
                            ? prevContext = makeContext(prevSource)
                            : prevContext;
        }

        @Override
        public void close() {
            // noinspection EmptyTryBlock
            try (final SafeCloseable ignored1 = currentContext;
                    final SafeCloseable ignored2 = prevContext) {
            }
        }
    }

    private class SwitchFillContext extends SwitchContext<FillContext> implements FillContext {

        private SwitchFillContext(final int chunkCapacity, final SharedContext sharedContext) {
            super(chunkCapacity, sharedContext);
        }

        @Override
        FillContext makeContext(@NotNull final ColumnSource innerSource) {
            return innerSource.makeFillContext(chunkCapacity, sharedContext);
        }
    }

    @Override
    public FillContext makeFillContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new SwitchFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Attributes.Values> destination,
            @NotNull final OrderedKeys orderedKeys) {
        // noinspection unchecked
        currentSource.fillChunk(((SwitchFillContext) context).getCurrentContext(), destination, orderedKeys);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext context,
            @NotNull final WritableChunk<? super Attributes.Values> destination,
            @NotNull final OrderedKeys orderedKeys) {
        if (prevInvalid()) {
            // noinspection unchecked
            currentSource.fillPrevChunk(((SwitchFillContext) context).getCurrentContext(), destination, orderedKeys);
            return;
        }
        // noinspection unchecked
        prevSource.fillPrevChunk(((SwitchFillContext) context).getPrevContext(), destination, orderedKeys);
    }

    private class SwitchGetContext extends SwitchContext<GetContext> implements GetContext {

        private SwitchGetContext(final int chunkCapacity, final SharedContext sharedContext) {
            super(chunkCapacity, sharedContext);
        }

        @Override
        GetContext makeContext(@NotNull final ColumnSource innerSource) {
            return innerSource.makeGetContext(chunkCapacity, sharedContext);
        }
    }

    @Override
    public GetContext makeGetContext(final int chunkCapacity, final SharedContext sharedContext) {
        return new SwitchGetContext(chunkCapacity, sharedContext);
    }

    @Override
    public Chunk<? extends Attributes.Values> getChunk(@NotNull final GetContext context,
            @NotNull final OrderedKeys orderedKeys) {
        // noinspection unchecked
        return currentSource.getChunk(((SwitchGetContext) context).getCurrentContext(), orderedKeys);
    }

    @Override
    public Chunk<? extends Attributes.Values> getPrevChunk(@NotNull final GetContext context,
            @NotNull final OrderedKeys orderedKeys) {
        if (prevInvalid()) {
            // noinspection unchecked
            return currentSource.getPrevChunk(((SwitchGetContext) context).getCurrentContext(), orderedKeys);
        }
        // noinspection unchecked
        return prevSource.getPrevChunk(((SwitchGetContext) context).getPrevContext(), orderedKeys);
    }

    @Override
    public T get(final long index) {
        return currentSource.get(index);
    }

    @Override
    public Boolean getBoolean(final long index) {
        return currentSource.getBoolean(index);
    }

    @Override
    public byte getByte(final long index) {
        return currentSource.getByte(index);
    }

    @Override
    public char getChar(final long index) {
        return currentSource.getChar(index);
    }

    @Override
    public double getDouble(final long index) {
        return currentSource.getDouble(index);
    }

    @Override
    public float getFloat(final long index) {
        return currentSource.getFloat(index);
    }

    @Override
    public int getInt(final long index) {
        return currentSource.getInt(index);
    }

    @Override
    public long getLong(final long index) {
        return currentSource.getLong(index);
    }

    @Override
    public short getShort(final long index) {
        return currentSource.getShort(index);
    }

    @Override
    public T getPrev(final long index) {
        if (prevInvalid()) {
            return currentSource.getPrev(index);
        }
        return prevSource.getPrev(index);
    }

    @Override
    public Boolean getPrevBoolean(final long index) {
        if (prevInvalid()) {
            return currentSource.getPrevBoolean(index);
        }
        return prevSource.getPrevBoolean(index);
    }

    @Override
    public byte getPrevByte(final long index) {
        if (prevInvalid()) {
            return currentSource.getPrevByte(index);
        }
        return prevSource.getPrevByte(index);
    }

    @Override
    public char getPrevChar(final long index) {
        if (prevInvalid()) {
            return currentSource.getPrevChar(index);
        }
        return prevSource.getPrevChar(index);
    }

    @Override
    public double getPrevDouble(final long index) {
        if (prevInvalid()) {
            return currentSource.getPrevDouble(index);
        }
        return prevSource.getPrevDouble(index);
    }

    @Override
    public float getPrevFloat(final long index) {
        if (prevInvalid()) {
            return currentSource.getPrevFloat(index);
        }
        return prevSource.getPrevFloat(index);
    }

    @Override
    public int getPrevInt(final long index) {
        if (prevInvalid()) {
            return currentSource.getPrevInt(index);
        }
        return prevSource.getPrevInt(index);
    }

    @Override
    public long getPrevLong(final long index) {
        if (prevInvalid()) {
            return currentSource.getPrevLong(index);
        }
        return prevSource.getPrevLong(index);
    }

    @Override
    public short getPrevShort(final long index) {
        if (prevInvalid()) {
            return currentSource.getPrevShort(index);
        }
        return prevSource.getPrevShort(index);
    }


    private boolean prevInvalid() {
        return prevValidityStep == -1 || prevValidityStep != LogicalClock.DEFAULT.currentStep();
    }
}
