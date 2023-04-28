package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.updateby.DeltaControl;
import org.immutables.value.Value;
import org.immutables.value.Value.Immutable;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Sum of the specified columns.
 */
@Immutable
@BuildableStyle
public class DeltaSpec extends UpdateBySpecBase {
    // provide a default delta control
    @Value.Default
    public DeltaControl deltaControl() {
        return DeltaControl.DEFAULT;
    }

    public static DeltaSpec of() {
        return ImmutableDeltaSpec.builder().build();
    }

    public static DeltaSpec of(DeltaControl control) {
        return ImmutableDeltaSpec.builder().deltaControl(control).build();
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return
        // is primitive or boxed numeric?
        applicableToNumeric(inputType)
                || inputType == char.class || inputType == Character.class;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
