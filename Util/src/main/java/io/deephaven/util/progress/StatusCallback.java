package io.deephaven.util.progress;

import java.util.function.Supplier;

/**
 * TODO docs
 */
public interface StatusCallback {
    /**
     * Update the progress % and status message.
     *
     * @param progress percent complete (0-100)
     * @param status optional message text
     */
    default void update(int progress, String status) {
        update(progress, () -> status);
    }

    /**
     * Update the progress % and status message.
     *
     * @param progress percent complete (0-100)
     * @param statusSupplier optional message supplier
     */
    void update(int progress, Supplier<String> statusSupplier);


    /**
     * Indicates that no further updates are expected.
     *
     * @param status optional message
     */
    default void finish(String status) {
        finish(() -> status);
    }

    /**
     * Indicates that no further updates are expected.
     *
     * @param statusSupplier optional message supplier
     */
    default void finish(Supplier<String> statusSupplier) {
        update(100, statusSupplier);
    }

    /**
     * get the current value of the progress object.
     *
     * @return the % complete of the topmost ProcessStatus - a number between 1 and 100
     */
    int getValue();

    /**
     * Get the current value of the the current step/subrange.
     * 
     * @return the % complete of the current step - a number between 1 and 100
     */
    default int getStepValue() {
        return getValue();
    };

    /**
     * Get a new status callback representing a sub range of this one.
     *
     * TODO: it's iffy whether this belongs in the interface, but is pretty handy. Could easily be a
     * factory.
     * 
     * @param min 0% in the subrange corresponds to min in the parent
     * @param max 100% in the subrange corresponds to max in the parent
     * @return a new status callback representing the specified subrange of this callback
     */
    default StatusCallback subrange(int min, int max) {
        return new ProcessStatusSubrange(this, min, max);
    };

    /**
     * Syntactic sugar for converting the remaining capacity to a subrange.
     * 
     * @return a new ProcessStatusSubrange for the remaining part of this one.
     */
    default StatusCallback remaining() {
        return subrange(getStepValue(), 100);
    }

    /**
     * Syntactic sugar for converting part of remaining capacity to a subrange.
     * 
     * @return a new ProcessStatusSubrange for pct percent of the remaining part of this one.
     */
    default StatusCallback remaining(int pct) {
        int min = getStepValue();
        int max = (int) ((100.0 - min) * pct / 100.0) + min;
        return subrange(min, max);
    }
}
