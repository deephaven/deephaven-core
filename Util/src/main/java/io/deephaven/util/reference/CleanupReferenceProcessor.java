package io.deephaven.util.reference;

import io.deephaven.base.reference.CleanupReference;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.FunctionalInterfaces;
import io.deephaven.util.Utils;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.internal.log.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;

/**
 * Utility for draining a reference queue of {@link CleanupReference}s and invoking their cleanup
 * methods.
 */
public class CleanupReferenceProcessor {

    private static final Logger log = LoggerFactory.getLogger(CleanupReferenceProcessor.class);

    private static final boolean LOG_CLEANED_REFERENCES = Configuration.getInstance()
        .getBooleanWithDefault("CleanupReferenceProcessor.logCleanedReferences", false);

    private final String name;
    private final long shutdownCheckDelayMillis;
    private final FunctionalInterfaces.ThrowingTriConsumer<Logger, CleanupReference, Exception, ? extends RuntimeException> exceptionHandler;

    /**
     * The reference queue from the most recent initialization.
     */
    private volatile ReferenceQueue<?> referenceQueue;

    /**
     * Construct a new {@link CleanupReferenceProcessor}.
     *
     * @param name The name of the processor, used for naming threads
     * @param shutdownCheckDelayMillis The frequency with which to check for shutdown
     * @param exceptionHandler Callback for exception handling
     */
    public CleanupReferenceProcessor(@NotNull final String name,
        final long shutdownCheckDelayMillis,
        @NotNull final FunctionalInterfaces.ThrowingTriConsumer<Logger, CleanupReference, Exception, ? extends RuntimeException> exceptionHandler) {
        this.name = Require.neqNull(name, "name");
        this.shutdownCheckDelayMillis =
            Require.geqZero(shutdownCheckDelayMillis, "shutdownDelayCheckMillis");
        this.exceptionHandler = Require.neqNull(exceptionHandler, "exceptionHandler");
    }

    /**
     * <p>
     * Get the reference queue for this cleaner.
     * <p>
     * On the first call after construction or {@link #resetForUnitTests()}, this method initializes
     * the instance as a side effect. Initialization entails:
     * <ol>
     * <li>Constructing a {@link ReferenceQueue}.</li>
     * <li>Starting a daemon thread that will drain the reference queue and invoke
     * {@link CleanupReference#cleanup()} on any {@link CleanupReference} dequeued.</li>
     * </ol>
     *
     * @return The {@link ReferenceQueue} constructed in the most recent initialization of this
     *         {@link CleanupReferenceProcessor} instance
     */
    public <RT> ReferenceQueue<RT> getReferenceQueue() {
        ReferenceQueue localQueue;
        if ((localQueue = referenceQueue) == null) {
            synchronized (this) {
                if ((localQueue = referenceQueue) == null) {
                    referenceQueue = localQueue = new ReferenceQueue<>();
                    final Thread cleanerThread = new Thread(new DrainQueue(localQueue),
                        "CleanupReferenceProcessor-" + name + "-drainingThread");
                    cleanerThread.setDaemon(true);
                    cleanerThread.start();
                }
            }
        }
        // noinspection unchecked
        return localQueue;
    }

    /**
     * Reset this instance so that the next call to {@link #getReferenceQueue()} will re-initialize
     * it and provide a new queue. Results in eventual termination of the daemon thread that may
     * have been draining the existing queue.
     */
    @TestUseOnly
    public final synchronized void resetForUnitTests() {
        referenceQueue = null;
    }

    /**
     * Drain the reference queue and call cleanup on any {@link CleanupReference}s dequeued.
     */
    private class DrainQueue implements Runnable {
        private final ReferenceQueue localQueue;

        private DrainQueue(ReferenceQueue localQueue) {
            this.localQueue = localQueue;
        }

        @Override
        public void run() {
            while (localQueue == referenceQueue) {
                final Reference<?> reference;
                try {
                    reference = localQueue.remove(shutdownCheckDelayMillis);
                } catch (InterruptedException ignored) {
                    continue;
                }
                if (reference instanceof CleanupReference) {
                    try {
                        if (LOG_CLEANED_REFERENCES) {
                            log.info().append("CleanupReferenceProcessor-").append(name)
                                .append(", cleaning ").append(Utils.REFERENT_FORMATTER, reference)
                                .endl();
                        }
                        ((CleanupReference) reference).cleanup();
                    } catch (Exception e) {
                        exceptionHandler.accept(log, (CleanupReference) reference, e);
                    }
                }
            }
        }
    }
}
