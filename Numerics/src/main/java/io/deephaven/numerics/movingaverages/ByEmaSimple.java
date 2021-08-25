/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.numerics.movingaverages;

import java.util.concurrent.TimeUnit;

/**
 * A DB aware EMA which can compute "by" emas without grouping and then ungrouping.
 */
public class ByEmaSimple extends ByEma {
    private static final long serialVersionUID = -2162403525928154570L;
    private final AbstractMa.Type type;
    private final AbstractMa.Mode mode;
    private final double timescaleNanos;

    public ByEmaSimple(ByEma.BadDataBehavior nullBehavior, ByEma.BadDataBehavior nanBehavior, AbstractMa.Mode mode,
            double timescale, TimeUnit timeUnit) {
        this(nullBehavior, nanBehavior, AbstractMa.Type.LEVEL, mode, timescale, timeUnit);
    }

    public ByEmaSimple(ByEma.BadDataBehavior nullBehavior, ByEma.BadDataBehavior nanBehavior, AbstractMa.Type type,
            AbstractMa.Mode mode, double timescale, TimeUnit timeUnit) {
        super(nullBehavior, nanBehavior);
        this.type = type;
        this.mode = mode;
        this.timescaleNanos = timeUnit == null ? timescale : timescale * timeUnit.toNanos(1);
    }

    @Override
    protected AbstractMa createEma(Key key) {
        return new Ema(type, mode, timescaleNanos);
    }
}
