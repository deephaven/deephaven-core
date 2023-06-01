package io.deephaven.engine.updategraph;

import io.deephaven.io.log.LogEntry;
import io.deephaven.util.locks.AwareFunctionalLock;

import java.util.Arrays;
import java.util.concurrent.locks.Condition;

public interface UpdateGraph extends UpdateSourceRegistrar, NotificationQueue, NotificationQueue.Dependency {

    /**
     * Retrieve the number of update threads.
     *
     * <p>
     * The UpdateGraphProcessor has a configurable number of update processing threads. The number of threads is exposed
     * in your method to enable you to partition a query based on the number of threads.
     * </p>
     *
     * @return the number of update threads configured.
     */
    int getUpdateThreads();

    /**
     * Test if this thread is part of our run thread executor service.
     *
     * @return whether this is one of our run threads.
     */
    boolean isRefreshThread();

    /**
     * Should this thread check table operations for safety with respect to the update lock?
     *
     * @return if we should check table operations.
     */
    boolean getCheckTableOperations();

    /**
     * <p>
     * Set the target duration of an update cycle, including the updating phase and the idle phase. This is also the
     * target interval between the start of one cycle and the start of the next.
     * <p>
     * Can be reset to default via {@link #resetCycleDuration()}.
     *
     * @implNote Any target cycle duration {@code < 0} will be clamped to 0.
     *
     * @param targetCycleDurationMillis The target duration for update cycles in milliseconds
     */
    void setTargetCycleDurationMillis(final long targetCycleDurationMillis);

    /**
     * Get the target duration of an update cycle, including the updating phase and the idle phase. This is also the
     * target interval between the start of one cycle and the start of the next.
     *
     * @return The {@link #setTargetCycleDurationMillis(long) current} target cycle duration
     */
    long getTargetCycleDurationMillis();

    /**
     * Resets the run cycle time to the default target.
     */
    @SuppressWarnings("unused")
    void resetCycleDuration();

    /**
     * @return The {@link LogicalClock} to use with this update graph
     */
    LogicalClock clock();

    /**
     * @return The shared {@link AwareFunctionalLock} to use with this update graph
     */
    AwareFunctionalLock sharedLock();

    /**
     * @return The exclusive {@link AwareFunctionalLock} to use with this update graph
     */
    AwareFunctionalLock exclusiveLock();

    /**
     * @return A LogEntry that may be prefixed with UpdateGraph information
     */
    LogEntry logDependencies();

    /**
     * <p>
     * If we are establishing a new table operation, on a refreshing table without the UpdateGraphProcessor lock; then
     * we are likely committing a grievous error, but one that will only occasionally result in us getting the wrong
     * answer or if we are lucky an assertion. This method is called from various query operations that should not be
     * established without the UGP lock.
     * </p>
     *
     * <p>
     * The run thread pool threads are allowed to instantiate operations, even though that thread does not have the
     * lock; because they are protected by the main run thread and dependency tracking.
     * </p>
     *
     * <p>
     * If you are sure that you know what you are doing better than the query engine, you may call
     * {@link #setCheckTableOperations(boolean)} to set a thread local variable bypassing this check.
     * </p>
     */
    void checkInitiateTableOperation();

    /**
     * If you know that the table operations you are performing are indeed safe, then call this method with false to
     * disable table operation checking. Conversely, if you want to enforce checking even if the configuration
     * disagrees; call it with true.
     *
     * @param value the new value of check table operations
     * @return the old value of check table operations
     */
    boolean setCheckTableOperations(boolean value);

    /**
     * Request that the next update cycle begin as soon as practicable. This "hurry-up" cycle happens through normal
     * means using the refresh thread and its workers.
     */
    void requestRefresh();

    void requestSignal(Condition updateGraphProcessorCondition);

    // TODO: What does Ryan really want here?
    // hide in impl maybe behind processor:
    // <UP_TYPE extends UpdateProcessor> boolean hasProcessorType(@NotNull final Class<UP_TYPE> processorType) {
    // final UpdateProcessor processor = processor();
    // return processorType.isAssignableFrom(processor.getClass());
    // }
    //
    // <UP_TYPE extends UpdateProcessor> UP_TYPE processor();

    void takeAccumulatedCycleStats(AccumulatedCycleStats ugpAccumCycleStats);

    class AccumulatedCycleStats {
        /**
         * Number of cycles run.
         */
        public int cycles = 0;
        /**
         * Number of cycles run not exceeding their time budget.
         */
        public int cyclesOnBudget = 0;
        /**
         * Accumulated safepoints over all cycles.
         */
        public int safePoints = 0;
        /**
         * Accumulated safepoint time over all cycles.
         */
        public long safePointPauseTimeMillis = 0L;

        public int[] cycleTimesMicros = new int[32];
        public static final int MAX_DOUBLING_LEN = 1024;

        synchronized void accumulate(
                final long targetCycleDurationMillis,
                final long cycleTimeNanos,
                final long safePoints,
                final long safePointPauseTimeMillis) {
            final boolean onBudget = targetCycleDurationMillis * 1000 * 1000 >= cycleTimeNanos;
            if (onBudget) {
                ++cyclesOnBudget;
            }
            this.safePoints += safePoints;
            this.safePointPauseTimeMillis += safePointPauseTimeMillis;
            if (cycles >= cycleTimesMicros.length) {
                final int newLen;
                if (cycleTimesMicros.length < MAX_DOUBLING_LEN) {
                    newLen = cycleTimesMicros.length * 2;
                } else {
                    newLen = cycleTimesMicros.length + MAX_DOUBLING_LEN;
                }
                cycleTimesMicros = Arrays.copyOf(cycleTimesMicros, newLen);
            }
            cycleTimesMicros[cycles] = (int) ((cycleTimeNanos + 500) / 1_000);
            ++cycles;
        }

        public synchronized void take(final AccumulatedCycleStats out) {
            out.cycles = cycles;
            out.cyclesOnBudget = cyclesOnBudget;
            out.safePoints = safePoints;
            out.safePointPauseTimeMillis = safePointPauseTimeMillis;
            if (out.cycleTimesMicros.length < cycleTimesMicros.length) {
                out.cycleTimesMicros = new int[cycleTimesMicros.length];
            }
            System.arraycopy(cycleTimesMicros, 0, out.cycleTimesMicros, 0, cycles);
            cycles = 0;
            cyclesOnBudget = 0;
            safePoints = 0;
            safePointPauseTimeMillis = 0;
        }
    }

    default <UP_TYPE extends UpdateGraph> UP_TYPE cast() {
        // noinspection unchecked
        return (UP_TYPE) this;
    }
}
