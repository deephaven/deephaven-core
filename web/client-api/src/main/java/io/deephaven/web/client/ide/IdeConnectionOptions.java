/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.ide;

import com.vertispan.tsdefs.annotations.TsIgnore;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsType;

@JsType(namespace = "dh")
@TsIgnore
public class IdeConnectionOptions {
    public String authToken;
    public String serviceId;

    @JsConstructor
    public IdeConnectionOptions() {}
}
