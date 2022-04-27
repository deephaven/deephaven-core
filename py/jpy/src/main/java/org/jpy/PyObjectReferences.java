package org.jpy;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Allows for proper cleanup of PyObjects outside of the {@link Object#finalize()} method.
 *
 * <p>Note: this setup could likely be better structured as a factory, but then the existing JNI
 * code would need to be aware of it. Instead, we'll ensure that new PyObjects register.
 */
class PyObjectReferences {
    private static final String WEAK = "weak";
    private static final String REF_TYPE = System.getProperty("PyObject.reference_type", WEAK);

    /**
     * At the end of the day, this is the most important number in determining the relative value
     * that is being placed in cleaning up vs performing more python work first. The larger this
     * number is, the more emphasis that is placed on cleaning up. The smaller this number is, the
     * more emphasis that is placed on other python work. It's possible that out of memory issues
     * occur if too small a number is provided here - thus the guidance is to err on the larger
     * side.
     */
    private static final int DEFAULT_BATCH_CLOSE_SIZE = Integer.parseInt(System.getProperty("PyObject.batch_close_size", "4096"));

    private static final long CLEANUP_THREAD_ACTIVE_SLEEP_MILLIS = Long.parseLong(System.getProperty("PyObject.active_sleep_millis", "0"));
    private static final long CLEANUP_THREAD_PASSIVE_SLEEP_MILLIS = Long.parseLong(System.getProperty("PyObject.passive_sleep_millis", "1000"));

    private final ReferenceQueue<PyObject> referenceQueue;
    private final Map<Reference<PyObject>, PyObjectState> references;
    private final long[] buffer;

    PyObjectReferences() {
        this(DEFAULT_BATCH_CLOSE_SIZE);
    }

    PyObjectReferences(int batchCloseSize) {
        if (batchCloseSize <= 0) {
            throw new IllegalArgumentException("batchCloseSize must be positive");
        }
        referenceQueue = new ReferenceQueue<>();
        references = new ConcurrentHashMap<>();
        buffer = new long[batchCloseSize];
    }

    void register(PyObject pyObject) {
        if (references.put(asRef(pyObject), pyObject.getState()) != null) {
            throw new IllegalStateException("Existing reference overwritten - this should not happen.");
        }
    }

    private Reference<PyObject> asRef(PyObject pyObject) {
        // The implementation details regarding best-practices of phantom vs weak references
        // notifications is a bit murky to me. While the most technically correct replacement for
        // finalization logic is a phantom reference, does a weak reference serve a similar purpose
        // if we can guarantee the object won't ever be re-animated after being marked as weak? If
        // so, does using weak references get us the notification a bit sooner? And - does using
        // weak references actually allow us to reclaim memory a bit faster, since phantom
        // references don't allow GC to actually proceed on the object until the phantom references
        // themselves are collected (see the phantom references javadoc)?
        //
        // I *think* the answer to the above questions are "yes".
        //
        // As such, using a weak reference here, while a bit unsafer, is the more performant choice.
        // We must guarantee that no other users will keep weak references to PyObjects and
        // re-animate them.
        return WEAK.equals(REF_TYPE) ?
            new WeakReference<>(pyObject, referenceQueue) :
            new PhantomReference<>(pyObject, referenceQueue);
    }

    /**
     * This should *only* be invoked through the proxy, or when we *know* we have the GIL.
     */
    public int cleanupOnlyUseFromGIL() {
        return cleanupOnlyUseFromGIL(buffer);
    }

    private int cleanupOnlyUseFromGIL(long[] buffer) {
        return PyLib.ensureGil(() -> {
            int index = 0;
            while (index < buffer.length) {
                final Reference<? extends PyObject> reference = referenceQueue.poll();
                if (reference == null) {
                    break;
                }
                index = appendIfNotClosed(buffer, index, reference);
            }
            if (index == 0) {
                return 0;
            }

            // We really really really want to make sure we *already* have the GIL lock at this point in
            // time. Otherwise, we block here until the GIL is available for us, and stall all cleanup
            // related to our PyObjects.

            if (index == 1) {
                PyLib.decRef(buffer[0]);
                return 1;
            }
            PyLib.decRefs(buffer, index);
            return index;
        });
    }

    private int appendIfNotClosed(long[] buffer, int index, Reference<? extends PyObject> reference) {
        reference.clear(); // helps GC proceed a bit faster for PhantomReference - guava Finalizer does this too

        final PyObjectState state = references.remove(reference);
        if (state == null) {
            throw new IllegalStateException("Reference from queue not in map - this should not happen.");
        }
        final long pointerForClosure = state.takePointer();
        if (pointerForClosure == 0) {
            // it's already been closed
            return index;
        }
        buffer[index] = pointerForClosure;
        return index + 1;
    }

    PyObjectCleanup asProxy() {
        try (
            final CreateModule createModule = CreateModule.create();
            final IdentityModule identityModule = IdentityModule.create(createModule)) {
            return identityModule
                .identity(this)
                .createProxy(PyObjectCleanup.class);
        }
    }

    Thread createCleanupThread(String name) {
        return new Thread(this::cleanupThreadLogic, name);
    }

    private void cleanupThreadLogic() {

        // Note: this loop logic *could* be written completely in python (as seen below), as python
        // should release the gil during a `time.sleep(seconds)` operation. It would save us on the
        // number of java -> python JNI crossings (but not on the python -> java JNI crossings) -
        // but that isn't a big concern wrt cleanup logic. More important might be java thread
        // lifecycle interruption, which we get when we call Thread.sleep.

        /*
            def cleanup(references):
              import time
              while True:
                size = references.cleanup()
                sleep_time = 0.1 if size == 1024 else 1.0
                time.sleep(sleep_time)
         */

        final PyObjectCleanup proxy = asProxy();

        while (!Thread.currentThread().isInterrupted()) {
            // This blocks on the GIL, acquires the GIL, and then releases the GIL.
            // For linux, acquiring the GIL involves a pthread_mutex_lock, which does not provide
            // any fairness guarantees. As such, we need to be mindful of other python users/code,
            // and ensure we don't overly acquire the GIL causing starvation issues, especially when
            // there is no cleanup work to do.
            final int size = proxy.cleanupOnlyUseFromGIL();


            // Although, it *does* make sense to potentially take the GIL in a tight loop when there
            // is a lot of real cleanup work to do. Sleeping for any amount of time may be
            // detrimental to the cleanup of resources. There is a balance that we want to try to
            // achieve between producers of PyObjects, and the cleanup of PyObjects (us).

            // It would be much nicer if ReferenceQueue exposed a method that blocked until the
            // queue was non-empty and *doesn't* remove any items. We can potentially implement this
            // by using reflection to access the internal lock of the ReferenceQueue in the future.

            if (size == buffer.length) {
                if (CLEANUP_THREAD_ACTIVE_SLEEP_MILLIS == 0) {
                    Thread.yield();
                } else {
                    try {
                        Thread.sleep(CLEANUP_THREAD_ACTIVE_SLEEP_MILLIS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            } else {
                try {
                    Thread.sleep(CLEANUP_THREAD_PASSIVE_SLEEP_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
    }
}
