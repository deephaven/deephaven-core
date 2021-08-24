package io.deephaven.db.util.config;

import io.deephaven.io.logger.Logger;
import io.deephaven.util.process.ProcessEnvironment;

import java.util.concurrent.CompletableFuture;

/**
 * A listener to be used with asynchronous input table methods to receive status notifications.
 */
public interface InputTableStatusListener {
    /**
     * A Simple implementation that does nothing on success, and logs an error on failure
     */
    InputTableStatusListener DEFAULT = new InputTableStatusListener() {
        final Logger log = ProcessEnvironment.getDefaultLog(InputTableStatusListener.class);

        @Override
        public void onError(Throwable t) {
            log.error().append("Error writing to Input Table: ").append(t).endl();
        }
    };

    class Future extends CompletableFuture<Void> implements InputTableStatusListener {
        @Override
        public void onError(Throwable t) {
            completeExceptionally(t);
        }

        @Override
        public void onSuccess() {
            complete(null);
        }
    }

    /**
     * Handle an error that occured during an input table write.
     * 
     * @param t the error.
     */
    void onError(Throwable t);

    /**
     * Handle successful completion of an input table write.
     */
    default void onSuccess() {}
}
