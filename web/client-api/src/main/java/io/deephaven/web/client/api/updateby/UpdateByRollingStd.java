//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import com.vertispan.tsdefs.annotations.TsLiteral;
import jsinterop.annotations.JsType;

/**
 * Computes the sample standard deviation of each input column over each row's window. Uses
 * <a href="https://en.wikipedia.org/wiki/Bessel%27s_correction">Bessel's correction</a>, which divides by
 * {@code n - 1}. Skips null values.
 */
@JsType(namespace = "dh.updateby")
public final class UpdateByRollingStd extends UpdateByRollingSpec {
    @TsLiteral
    public final String type = "UpdateByRollingStd";
}
