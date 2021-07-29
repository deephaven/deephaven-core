package io.deephaven.db.v2.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.chunk.SharedContext;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.UpdateCommitter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Consumer;

public class SwitchColumnSource<T> extends AbstractColumnSource<T> {
    private long prevCycle = -1;
    private ColumnSource<T> prevSource;
    private ColumnSource<T> currentSource;
    private final UpdateCommitter<SwitchColumnSource<T>> updateCommitter;
    private final Consumer<ColumnSource<T>> onPreviousCommitted;

    public SwitchColumnSource(ColumnSource<T> currentSource) {
        this(currentSource, null);
    }

    public SwitchColumnSource(@NotNull final ColumnSource<T> currentSource, @Nullable final Consumer<ColumnSource<T>> onPreviousCommitted) {
        super(currentSource.getType(), currentSource.getComponentType());
        this.currentSource = currentSource;
        this.updateCommitter = new UpdateCommitter<>(this, SwitchColumnSource::clearPrevious);
        this.onPreviousCommitted = onPreviousCommitted;
    }

    private void clearPrevious() {
        final ColumnSource<T> captured = prevSource;
        prevCycle = -1;
        prevSource = null;
        onPreviousCommitted.accept(captured);
    }

    @Override
    public boolean isImmutable() {
        return false;
    }

    private class SwitchFillContext implements FillContext {
        final FillContext currentContext;
        final FillContext prevContext;

        private SwitchFillContext(int chunkCapacity, SharedContext sharedContext) {
            this.currentContext = currentSource.makeFillContext(chunkCapacity, sharedContext);
            if (prevSource != null) {
                this.prevContext = prevSource.makeFillContext(chunkCapacity, sharedContext);
            } else {
                this.prevContext = null;
            }
        }

        @Override
        public void close() {
            currentContext.close();
            if (prevContext != null) {
                prevContext.close();
            }
        }
    }

    public void setNewCurrent(ColumnSource<T> newCurrent) {
        Assert.eq(newCurrent.getType(), "newCurrent.getType()", currentSource.getType(), "currentSource.getType()");
        prevSource = currentSource;
        prevCycle = LogicalClock.DEFAULT.currentStep();
        currentSource = newCurrent;
        updateCommitter.maybeActivate();
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return new SwitchFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk destination, @NotNull OrderedKeys orderedKeys) {
        //noinspection unchecked
        currentSource.fillChunk(((SwitchFillContext)context).currentContext, destination, orderedKeys);
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk destination, @NotNull OrderedKeys orderedKeys) {
        if (prevInvalid()) {
            currentSource.fillPrevChunk(((SwitchFillContext)context).currentContext, destination, orderedKeys);
            return;
        }
        //noinspection unchecked
        final SwitchFillContext switchContext = (SwitchFillContext) context;
        final FillContext useContext = switchContext.prevContext != null ? switchContext.prevContext : switchContext.currentContext;
        //noinspection unchecked
        prevSource.fillPrevChunk(useContext, destination, orderedKeys);
    }

    @Override
    public T get(long index) {
        return currentSource.get(index);
    }

    @Override
    public Boolean getBoolean(long index) {
        return currentSource.getBoolean(index);
    }

    @Override
    public byte getByte(long index) {
        return currentSource.getByte(index);
    }

    @Override
    public char getChar(long index) {
        return currentSource.getChar(index);
    }

    @Override
    public double getDouble(long index) {
        return currentSource.getDouble(index);
    }

    @Override
    public float getFloat(long index) {
        return currentSource.getFloat(index);
    }

    @Override
    public int getInt(long index) {
        return currentSource.getInt(index);
    }

    @Override
    public long getLong(long index) {
        return currentSource.getLong(index);
    }

    @Override
    public short getShort(long index) {
        return currentSource.getShort(index);
    }

    @Override
    public T getPrev(long index) {
        if (prevInvalid()) {
            return currentSource.getPrev(index);
        }
        return prevSource.getPrev(index);
    }

    @Override
    public Boolean getPrevBoolean(long index) {
        if (prevInvalid()) {
            return currentSource.getPrevBoolean(index);
        }
        return prevSource.getPrevBoolean(index);
    }

    @Override
    public byte getPrevByte(long index) {
        if (prevInvalid()) {
            return currentSource.getPrevByte(index);
        }
        return prevSource.getPrevByte(index);
    }

    @Override
    public char getPrevChar(long index) {
        if (prevInvalid()) {
            return currentSource.getPrevChar(index);
        }
        return prevSource.getPrevChar(index);
    }

    @Override
    public double getPrevDouble(long index) {
        if (prevInvalid()) {
            return currentSource.getPrevDouble(index);
        }
        return prevSource.getPrevDouble(index);
    }

    @Override
    public float getPrevFloat(long index) {
        if (prevInvalid()) {
            return currentSource.getPrevFloat(index);
        }
        return prevSource.getPrevFloat(index);
    }

    @Override
    public int getPrevInt(long index) {
        if (prevInvalid()) {
            return currentSource.getPrevInt(index);
        }
        return prevSource.getPrevInt(index);
    }

    @Override
    public long getPrevLong(long index) {
        if (prevInvalid()) {
            return currentSource.getPrevLong(index);
        }
        return prevSource.getPrevLong(index);
    }

    @Override
    public short getPrevShort(long index) {
        if (prevInvalid()) {
            return currentSource.getPrevShort(index);
        }
        return prevSource.getPrevShort(index);
    }


    private boolean prevInvalid() {
        return prevCycle == -1 || prevCycle != LogicalClock.DEFAULT.currentStep();
    }
}
