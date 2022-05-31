package io.deephaven.util.datastructures;

/**
 * Note this is similar to java.util.function.LongPredicate; we don't use that since it could create confusion as for
 * intended semantics for use.
 */
@FunctionalInterface
public interface LongAbortableConsumer {

    /**
     * Provides a value to this consumer. A false return value indicates that the application providing values to this
     * consumer should not invoke it again.
     *
     * @param v the value
     * @return false if don't want any more values after this one, true otherwise.
     */
    boolean accept(long v);
}
