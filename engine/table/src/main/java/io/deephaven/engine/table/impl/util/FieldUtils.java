package io.deephaven.engine.table.impl.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Supplier;

/**
 * Utilities for manipulating object fields.
 */
public class FieldUtils {

    /**
     * Ensure a field is initialized exactly once, and get the current value after possibly initializing it.
     *
     * @param instance The instance that owns the field
     * @param updater An {@link AtomicReferenceFieldUpdater} associated with the field
     * @param defaultValue The reference value that signifies that the field has not been initialized
     * @param valueFactory A factory for new values; may be called concurrently by multiple threads as they race to
     *        initialize the field
     * @return The initialized value of the field
     */
    public static <FIELD_TYPE, INSTANCE_TYPE> FIELD_TYPE ensureField(
            INSTANCE_TYPE instance,
            @NotNull final AtomicReferenceFieldUpdater<INSTANCE_TYPE, FIELD_TYPE> updater,
            @Nullable final FIELD_TYPE defaultValue,
            @NotNull final Supplier<FIELD_TYPE> valueFactory) {
        final FIELD_TYPE currentValue = updater.get(instance);
        if (currentValue != defaultValue) {
            // The field has previously been initialized, return the current value we already retrieved
            return currentValue;
        }
        final FIELD_TYPE candidateValue = valueFactory.get();
        if (updater.compareAndSet(instance, defaultValue, candidateValue)) {
            // This thread won the initialization race, return the candidate value we set
            return candidateValue;
        }
        // This thread lost the initialization race, re-read and return the current value
        return updater.get(instance);
    }
}
