//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.autocompleteresponse;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.AutoCompleteResponse.ResponseCase",
        namespace = JsPackage.GLOBAL)
public class ResponseCase {
    public static int COMPLETION_ITEMS,
            DIAGNOSTIC,
            DIAGNOSTIC_PUBLISH,
            HOVER,
            RESPONSE_NOT_SET,
            SIGNATURES;
}
