//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.ide;

import elemental2.promise.Promise;
import io.deephaven.web.client.api.console.JsCommandResult;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

/**
 * Event fired when a command is issued from the client.
 */
@JsType(namespace = "dh")
public class CommandInfo {

    private final String code;
    private final Promise<JsCommandResult> result;

    public CommandInfo(String code, Promise<JsCommandResult> result) {
        this.code = code;
        this.result = result;
    }

    @JsProperty
    public String getCode() {
        return code;
    }

    @JsProperty
    public Promise<JsCommandResult> getResult() {
        return result;
    }
}
