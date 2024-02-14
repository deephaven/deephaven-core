package io.deephaven.util.thread;

import java.util.Collection;

/**
 * Extension point to allow threads that will run user code from within the platform to be controlled by configuration.
 */
public interface ThreadInitializationFactory {
    ThreadInitializationFactory NO_OP = r -> r;

    static ThreadInitializationFactory of(Collection<ThreadInitializationFactory> factories) {
        return runnable -> {
            Runnable acc = runnable;
            for (ThreadInitializationFactory factory : factories) {
                acc = factory.createInitializer(acc);
            }
            return acc;
        };
    }

    Runnable createInitializer(Runnable runnable);
}
