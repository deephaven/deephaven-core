package io.deephaven.util.locks;

import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link ReentrantLock} that adds the {@link FunctionalLock} default features.
 */
public class FunctionalReentrantLock extends ReentrantLock implements FunctionalLock {

    public FunctionalReentrantLock() {}

    public FunctionalReentrantLock(final boolean fair) {
        super(fair);
    }
}
