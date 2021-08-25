package io.deephaven.util.type;

public interface NamedImplementation {

    /**
     * <p>
     * Get a name for the implementing class. Useful for abstract classes that implement
     * {@link io.deephaven.base.log.LogOutputAppendable LogOutputAppendable} or override
     * {@link Object#toString() toString}.
     * <p>
     * The default implementation is correct, but not suitable for high-frequency usage.
     * 
     * @return A name for the implementing class
     */
    default String getImplementationName() {
        return getClass().getSimpleName();
    }
}
