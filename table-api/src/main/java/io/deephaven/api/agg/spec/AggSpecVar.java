//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg.spec;

import io.deephaven.annotations.SingletonStyle;
import io.deephaven.api.TableOperations;
import org.immutables.value.Value.Immutable;

/**
 * Specifies an aggregation that outputs the sample variance of the input column values for each group. Only works for
 * numeric input types.
 *
 * Sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction), which
 * ensures that the sample variance will be an unbiased estimator of population variance.
 *
 * @see TableOperations#varBy
 */
@Immutable
@SingletonStyle
public abstract class AggSpecVar extends AggSpecEmptyBase {

    public static AggSpecVar of() {
        return ImmutableAggSpecVar.of();
    }

    @Override
    public final String description() {
        return "variance";
    }

    @Override
    public final <V extends Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
