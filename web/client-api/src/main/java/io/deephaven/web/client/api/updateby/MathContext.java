//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public class MathContext {
    public int precision;
    public RoundingMode roundingMode;
}
