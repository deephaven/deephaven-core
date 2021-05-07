package org.jpy;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

/**
 * An auxiliary structure that allows for proper cleanup of {@link PyObject} after they have been
 * GC'd.
 */
final class PyObjectState implements AutoCloseable {
    private static final AtomicLongFieldUpdater<PyObjectState> updater =
        AtomicLongFieldUpdater.newUpdater(PyObjectState.class, "pointer");

    /**
     * The value of the Python/C API {@code PyObject*} which this class represents.
     */
    private volatile long pointer;

    PyObjectState(long pointer) {
        if (pointer == 0) {
            throw new IllegalArgumentException("pointer == 0");
        }
        this.pointer = pointer;
    }

    public final long borrowPointer() {
        final long localPointer = updater.get(this); // TODO: how does this differ from just a volatile read against pointer?
        if (localPointer == 0) {
            throw new IllegalStateException("PyObjectState has already been taken");
        }
        return localPointer;
    }

    /**
     * Only a single caller can ever take the pointer. Once taken, they are responsible for closing
     * the pointer.
     *
     * @return the pointer, or 0 if it's already been taken
     */
    final long takePointer() {
        final long localPointer = updater.get(this); // TODO: how does this differ from just a volatile read against pointer?
        if (localPointer == 0) {
            // It's already been taken
            return 0;
        }
        if (!updater.compareAndSet(this, localPointer, 0)) {
            // Another thread concurrently took the pointer
            return 0;
        }
        // the caller is responsible for closing it now!
        return localPointer;
    }

    @Override
    public void close() {
        final long pointerForClosure = takePointer();
        if (pointerForClosure == 0) {
            // It's already been taken
            return;
        }
        // We are closing it
        PyLib.decRef(pointerForClosure);
    }
}
