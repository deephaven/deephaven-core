//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.updateby.DeltaControl;
import org.immutables.value.Value.Immutable;

import java.util.Optional;

/**
 * A {@link UpdateBySpec} for performing a Cumulative Sum of the specified columns.
 */
@Immutable
@BuildableStyle
public abstract class DeltaSpec extends UpdateBySpecBase {
    // provide a default delta control
    public abstract Optional<DeltaControl> deltaControl();

    public static DeltaSpec of() {
        return ImmutableDeltaSpec.builder().build();
    }

    public static DeltaSpec of(DeltaControl control) {
        return ImmutableDeltaSpec.builder().deltaControl(control).build();
    }

    public final DeltaControl deltaControlOrDefault() {
        return deltaControl().orElse(DeltaControl.DEFAULT);
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
