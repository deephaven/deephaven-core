package io.deephaven.engine.table.updateBySpec;

import io.deephaven.engine.table.EmaControl;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A {@link UpdateBySpec} for performing an Exponential Moving Average across the specified columns
 */
public class EmaSpec implements UpdateBySpec {
    private final String timestampCol;
    private final long timeScaleUnits;
    private final EmaControl control;

    public static EmaSpec ofTime(@NotNull final EmaControl control,
            @NotNull final String timestampCol,
            long timeScaleNanos) {
        return new EmaSpec(control, timestampCol, timeScaleNanos);
    }

    public static EmaSpec ofTicks(@NotNull EmaControl control, long tickWindow) {
        return new EmaSpec(control, null, tickWindow);
    }

    EmaSpec(@NotNull final EmaControl control, @Nullable final String timestampCol, final long timeScaleUnits) {
        this.timestampCol = timestampCol;
        this.control = control;
        this.timeScaleUnits = timeScaleUnits;
    }

    public String getTimestampCol() {
        return timestampCol;
    }

    public long getTimeScaleUnits() {
        return timeScaleUnits;
    }

    public EmaControl getControl() {
        return control;
    }

    @NotNull
    @Override
    public String describe() {
        return "Ema(" + (timestampCol == null ? "TickBased" : timestampCol) + ", " + timeScaleUnits + ", " + control
                + ")";
    }

    @Override
    public boolean applicableTo(@NotNull Class<?> inputType) {
        return TypeUtils.isNumeric(inputType);
    }

    @Override
    public <V extends Visitor> V walk(@NotNull V v) {
        v.visit(this);
        return v;
    }
}
