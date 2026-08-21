//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.agg;

import com.vertispan.tsdefs.annotations.TsInterface;
import jsinterop.annotations.JsType;

/**
 * Computes the sample variance within each aggregation group.
 *
 * <p>
 * Sample variance is computed using <a href="https://en.wikipedia.org/wiki/Bessel%27s_correction">Bessel's
 * correction</a>, which ensures that the sample variance will be an unbiased estimator of population variance.
 */
@JsType
@TsInterface
public final class Var extends ColumnAggregation {
    public final String type = "Var";
}
