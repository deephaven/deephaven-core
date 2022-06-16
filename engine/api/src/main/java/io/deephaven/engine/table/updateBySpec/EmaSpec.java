package io.deephaven.engine.table.updateBySpec;

import io.deephaven.annotations.SimpleStyle;
import io.deephaven.engine.table.EmaControl;
import io.deephaven.util.type.TypeUtils;
import org.immutables.value.Value.Parameter;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link UpdateBySpec} for performing an Exponential Moving Average across the specified columns
 */
@Immutable
@SimpleStyle
public abstract class EmaSpec implements UpdateBySpec {
    public static EmaSpec ofTime(@NotNull final EmaControl control,
            @NotNull final String timestampCol,
            long timeScaleNanos) {
        return ImmutableEmaSpec.of(control, timestampCol, timeScaleNanos);
    }

    public static EmaSpec ofTicks(@NotNull EmaControl control, long tickWindow) {
        return ImmutableEmaSpec.of(control, null, tickWindow);
    }

    @Parameter
    public abstract EmaControl control();

    @Parameter
    @Nullable
    public abstract String timestampCol();

    @Parameter
    public abstract long timeScaleUnits();

    @Override
    public boolean applicableTo(@NotNull Class<?> inputType) {
        return TypeUtils.isNumeric(inputType);
    }

    @Override
    public final <T> T walk(Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
