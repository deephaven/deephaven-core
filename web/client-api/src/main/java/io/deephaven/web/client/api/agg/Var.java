//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Computes the sample variance within each aggregation group.
 *
 * <p>
 * Uses <a href="https://en.wikipedia.org/wiki/Bessel%27s_correction">Bessel's correction</a>, which ensures that the
 * sample variance is an unbiased estimator of population variance.
 */
@JsType(namespace = "dh.agg")
public final class Var extends ColumnAggregation {
    @TsLiteral
    public final String type = "Var";
}
