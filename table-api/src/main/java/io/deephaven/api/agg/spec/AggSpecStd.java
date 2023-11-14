/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SingletonStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the sample standard deviation of the input column values for each group. Only
 * works for numeric input types.
 *
 * Sample standard deviation is computed using Bessel's correction
 * (https://en.wikipedia.org/wiki/Bessel%27s_correction), which ensures that the sample variance will be an unbiased
 * estimator of population variance.
 *
 * @see TableOperations#stdBy
 */
@Immutable
@SingletonStyle
public abstract class AggSpecStd extends AggSpecEmptyBase {

    public static AggSpecStd of() {
        return ImmutableAggSpecStd.of();
    }

    @Override
    public final String description() {
        return "standard deviation";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
