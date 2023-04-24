/*
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.updategraph;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.log.impl.LogOutputStringImpl;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.locks.AwareFunctionalLock;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public final class UpdateContext implements LogOutputAppendable {

    public static Builder newBuilder(final String name) {
        return new Builder(name);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // ThreadLocal Management
    // -----------------------------------------------------------------------------------------------------------------

    private static volatile UpdateContext defaultContext = null;

    public static UpdateContext getDefaultContext() {
        UpdateContext localContext;
        if ((localContext = defaultContext) == null) {
            synchronized (UpdateContext.class) {
                if ((localContext = defaultContext) == null) {
                    final int numUpdateThreads = Configuration.getInstance().getIntegerWithDefault(
                            "UpdateGraphProcessor.updateThreads", -1);
                    localContext = defaultContext = new Builder("DEFAULT")
                            .numUpdateThreads(numUpdateThreads)
                            .build();
                }
            }
        }
        return localContext;
    }

    private static final ThreadLocal<UpdateContext> currentContext =
            ThreadLocal.withInitial(UpdateContext::getDefaultContext);

    /**
     * @return an object representing the current update context
     */
    public static UpdateContext get() {
        return currentContext.get();
    }

    /**
     * @return the {@link LogicalClock} to use in the current update context
     */
    @ScriptApi
    public static LogicalClock logicalClock() {
        return get().getLogicalClock();
    }

    /**
     * @return the {@link UpdateGraphProcessor} to use in the current update context
     */
    @ScriptApi
    public static UpdateGraphProcessor updateGraphProcessor() {
        return get().getUpdateGraphProcessor();
    }

    /**
     * @return the {@link AwareFunctionalLock exclusiveLock} to use in the current update context
     */
    @ScriptApi
    public static AwareFunctionalLock exclusiveLock() {
        return get().getExclusiveLock();
    }

    /**
     * @return the {@link AwareFunctionalLock sharedLock} to use in the current update context
     */
    @ScriptApi
    public static AwareFunctionalLock sharedLock() {
        return get().getSharedLock();
    }

    /**
     * Installs the UpdateContext to be used for the current thread.
     */
    private static void setContext(final UpdateContext context) {
        currentContext.set(context);
    }

    // -----------------------------------------------------------------------------------------------------------------
    // UpdateContext Implementation
    // -----------------------------------------------------------------------------------------------------------------
    private final UpdateGraphProcessor updateGraphProcessor;

    UpdateContext(final UpdateGraphProcessor updateGraphProcessor) {
        this.updateGraphProcessor = Objects.requireNonNull(updateGraphProcessor);
    }

    /**
     * Execute runnable within this update context.
     */
    public void apply(Runnable runnable) {
        apply(() -> {
            runnable.run();
            return null;
        });
    }

    /**
     * Executes supplier within this update context.
     */
    public <T> T apply(Supplier<T> supplier) {
        try (SafeCloseable ignored = open()) {
            // actually evaluate the script
            return supplier.get();
        }
    }

    /**
     * Installs the UpdateContext and returns a function that will restore the original UpdateContext.
     *
     * @return a closeable to cleanup the update context
     */
    public SafeCloseable open() {
        // save the current context
        final UpdateContext oldUpdateContext = currentContext.get();

        UpdateContext.setContext(this);

        return () -> {
            // restore the old context
            UpdateContext.setContext(oldUpdateContext);
        };
    }

    public LogicalClock getLogicalClock() {
        return updateGraphProcessor.getLogicalClock();
    }

    public UpdateGraphProcessor getUpdateGraphProcessor() {
        return updateGraphProcessor;
    }

    public AwareFunctionalLock getExclusiveLock() {
        return updateGraphProcessor.exclusiveLock();
    }

    public AwareFunctionalLock getSharedLock() {
        return updateGraphProcessor.sharedLock();
    }

    public void checkInitiateTableOperation() {
        if (UpdateContext.get() != this) {
            throw new IllegalStateException("Table must be operated on in the same update context as it was created");
        }
        updateGraphProcessor.checkInitiateTableOperation();
    }

    @Override
    public LogOutput append(LogOutput logOutput) {
        return logOutput.append("UpdateContext{").append(updateGraphProcessor.getName()).append("}");
    }

    @Override
    public String toString() {
        return new LogOutputStringImpl().append(this).toString();
    }

    public static final String ALLOW_UNIT_TEST_MODE_PROP = "UpdateGraphProcessor.allowUnitTestMode";
    public static final String DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP =
            "UpdateGraphProcessor.targetCycleDurationMillis";
    public static final String MINIMUM_CYCLE_DURATION_TO_LOG_MILLIS_PROP =
            "UpdateGraphProcessor.minimumCycleDurationToLogMillis";

    public static final class Builder {
        private boolean allowUnitTestMode =
                Configuration.getInstance().getBooleanWithDefault(ALLOW_UNIT_TEST_MODE_PROP, false);
        private long targetCycleDurationMillis =
                Configuration.getInstance().getIntegerWithDefault(DEFAULT_TARGET_CYCLE_DURATION_MILLIS_PROP, 1000);
        private long minimumCycleDurationToLogNanos = TimeUnit.MILLISECONDS.toNanos(
                Configuration.getInstance().getIntegerWithDefault(MINIMUM_CYCLE_DURATION_TO_LOG_MILLIS_PROP, 25));

        private String name;
        private int numUpdateThreads = 1;

        public Builder(String name) {
            this.name = name;
        }

        /**
         * This enables Unit Test Mode. Unit tests mode allows complete control over the update graph processor. This is
         * useful for testing boundary conditions to validate the behavior of table operations.
         *
         * @param allowUnitTestMode true to allow unit test mode
         * @return this builder
         */
        public Builder allowUnitTestMode(boolean allowUnitTestMode) {
            this.allowUnitTestMode = allowUnitTestMode;
            return this;
        }

        /**
         * Set the target duration of an update cycle, including the updating phase and the idle phase. This is also the
         * target interval between the start of one cycle and the start of the next.
         *
         * @implNote Any target cycle duration {@code < 0} will be clamped to 0.
         *
         * @param targetCycleDurationMillis The target duration for update cycles in milliseconds
         * @return this builder
         */
        public Builder targetCycleDurationMillis(long targetCycleDurationMillis) {
            this.targetCycleDurationMillis = targetCycleDurationMillis;
            return this;
        }

        /**
         * Set the minimum duration of an update cycle that should be logged at the INFO level.
         *
         * @param minimumCycleDurationToLogNanos threshold to log a slow cycle
         * @return this builder
         */
        public Builder getMinimumCycleDurationToLogNanos(long minimumCycleDurationToLogNanos) {
            this.minimumCycleDurationToLogNanos = minimumCycleDurationToLogNanos;
            return this;
        }

        /**
         * Sets the number of threads to use in the update graph processor. Values < 0 indicate to use one thread per
         * available processor.
         *
         * @param numUpdateThreads number of threads to use in update processing
         * @return this builder
         */
        public Builder numUpdateThreads(int numUpdateThreads) {
            this.numUpdateThreads = numUpdateThreads;
            return this;
        }

        /**
         * Creates an UpdateGraphProcessor, starts it, and returns the UpdateContext associated with it.
         *
         * @return an update context wrapping the newly constructed update graph processor
         */
        public UpdateContext build() {
            return new UpdateGraphProcessor(
                    name,
                    allowUnitTestMode,
                    targetCycleDurationMillis,
                    minimumCycleDurationToLogNanos,
                    numUpdateThreads).getUpdateContext();
        }
    }
}
