package io.deephaven.util.locks;

/**
 * {@link java.util.concurrent.locks.Lock} that implements {@link AwareLock} and
 * {@link FunctionalLock}.
 */
public interface AwareFunctionalLock extends AwareLock, FunctionalLock {
}
