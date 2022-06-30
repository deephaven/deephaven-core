package io.deephaven.api.updateby.spec;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;

/**
 * A {@link UpdateBySpec} for performing a forward fill of the specified columns.
 */

@Immutable
@SimpleStyle
public abstract class FillBySpec extends UpdateBySpecBase {
    public static FillBySpec of() {
        return ImmutableFillBySpec.of();
    }

    @Override
    public final boolean applicableTo(Class<?> inputType) {
        return true;
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
