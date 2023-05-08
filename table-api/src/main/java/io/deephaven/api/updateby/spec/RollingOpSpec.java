package io.deephaven.api.updateby.spec;

import org.immutables.value.Value;

/**
 * An {@link UpdateBySpec} base class for performing a windowed rolling operation
 */

public abstract class RollingOpSpec extends UpdateBySpecBase {
    // We would like to use jdk.internal.util.ArraysSupport.MAX_ARRAY_LENGTH, but it is not exported
    final static int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    // Provide a default reverse-looking timescale
    @Value.Default
    public WindowScale revWindowScale() {
        return WindowScale.ofTicks(0);
    }

    // Provide a default forward-looking timescale
    @Value.Default
    public WindowScale fwdWindowScale() {
        return WindowScale.ofTicks(0);
    }

    @Value.Check
    final void checkWindowSizes() {
        // assert some rational constraints on window sizes (leq MAX_SIZE and geq 0)
        final double size =
                revWindowScale().getFractionalTimeScaleUnits() + fwdWindowScale().getFractionalTimeScaleUnits();
        if (size < 0) {
            throw new IllegalArgumentException("UpdateBy rolling window size must be non-negative");
        } else if (!revWindowScale().isTimeBased() && size > MAX_ARRAY_SIZE) {
            throw new IllegalArgumentException(
                    "UpdateBy rolling window size may not exceed MAX_ARRAY_SIZE (" + MAX_ARRAY_SIZE + ")");
        }
    }
}
