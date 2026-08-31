//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.updateby;

import jsinterop.annotations.JsType;

@JsType(namespace = "dh.updateby")
public class UpdateByEmOptions {
    public BadDataBehavior onNullValue;
    public BadDataBehavior onNaNValue;
    public BadDataBehavior onNullTime;
    public BadDataBehavior onNegativeDeltaTime;
    public BadDataBehavior onZeroDeltaTime;

    public MathContext bigValueContext;
}
