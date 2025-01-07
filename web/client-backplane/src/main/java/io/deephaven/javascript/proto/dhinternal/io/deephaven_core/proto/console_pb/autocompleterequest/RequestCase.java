//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.console_pb.autocompleterequest;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.console_pb.AutoCompleteRequest.RequestCase",
        namespace = JsPackage.GLOBAL)
public class RequestCase {
    public static int CHANGE_DOCUMENT,
            CLOSE_DOCUMENT,
            GET_COMPLETION_ITEMS,
            GET_DIAGNOSTIC,
            GET_HOVER,
            GET_SIGNATURE_HELP,
            OPEN_DOCUMENT,
            REQUEST_NOT_SET;
}
