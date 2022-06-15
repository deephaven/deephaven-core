/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.locks;

/**
 * {@link java.util.concurrent.locks.Lock} that implements {@link AwareLock} and {@link FunctionalLock}.
 */
public interface AwareFunctionalLock extends AwareLock, FunctionalLock {
}
