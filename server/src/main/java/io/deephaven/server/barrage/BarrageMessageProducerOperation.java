package io.deephaven.server.barrage;

import com.google.common.annotations.VisibleForTesting;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.MemoizedOperationKey;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.extensions.barrage.BarrageMessageProducer;
import io.deephaven.util.Scheduler;
import org.jetbrains.annotations.Nullable;

public class BarrageMessageProducerOperation<MessageView>
        implements QueryTable.MemoizableOperation<BarrageMessageProducer<MessageView>> {

    @AssistedFactory
    public interface Factory<MessageView> {
        BarrageMessageProducerOperation<MessageView> create(BaseTable parent, long updateIntervalMs);
    }

    private final Scheduler scheduler;
    private final BarrageMessageProducer.StreamGenerator.Factory<MessageView> streamGeneratorFactory;
    private final BaseTable parent;
    private final long updateIntervalMs;
    private final Runnable onGetSnapshot;

    @AssistedInject
    public BarrageMessageProducerOperation(
            final Scheduler scheduler,
            final BarrageMessageProducer.StreamGenerator.Factory<MessageView> streamGeneratorFactory,
            @Assisted final BaseTable parent,
            @Assisted final long updateIntervalMs) {
        this(scheduler, streamGeneratorFactory, parent, updateIntervalMs, null);
    }

    @VisibleForTesting
    public BarrageMessageProducerOperation(final Scheduler scheduler,
            final BarrageMessageProducer.StreamGenerator.Factory<MessageView> streamGeneratorFactory,
            final BaseTable parent,
            final long updateIntervalMs,
            @Nullable final Runnable onGetSnapshot) {
        this.scheduler = scheduler;
        this.streamGeneratorFactory = streamGeneratorFactory;
        this.parent = parent;
        this.updateIntervalMs = updateIntervalMs;
        this.onGetSnapshot = onGetSnapshot;
    }

    @Override
    public String getDescription() {
        return "BarrageMessageProducer(" + updateIntervalMs + ")";
    }

    @Override
    public String getLogPrefix() {
        return "BarrageMessageProducer.Operation(" + System.identityHashCode(this) + "): ";
    }

    @Override
    public MemoizedOperationKey getMemoizedOperationKey() {
        return new MyMemoKey(updateIntervalMs);
    }

    @Override
    public Result<BarrageMessageProducer<MessageView>> initialize(final boolean usePrev,
            final long beforeClock) {
        final BarrageMessageProducer<MessageView> result = new BarrageMessageProducer<MessageView>(
                scheduler, streamGeneratorFactory, parent, updateIntervalMs, onGetSnapshot);
        return new Result<>(result, result.constructListener());
    }

    private static class MyMemoKey extends MemoizedOperationKey {
        private final long interval;

        private MyMemoKey(final long interval) {
            this.interval = interval;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final MyMemoKey that = (MyMemoKey) o;
            return interval == that.interval;
        }

        @Override
        public int hashCode() {
            return Long.hashCode(interval);
        }
    }
}
