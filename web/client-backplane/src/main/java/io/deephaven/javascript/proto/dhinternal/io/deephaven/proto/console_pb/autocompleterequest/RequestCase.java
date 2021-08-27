package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.console_pb.autocompleterequest;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.console_pb.AutoCompleteRequest.RequestCase",
        namespace = JsPackage.GLOBAL)
public class RequestCase {
    public static int CHANGE_DOCUMENT,
            CLOSE_DOCUMENT,
            GET_COMPLETION_ITEMS,
            OPEN_DOCUMENT,
            REQUEST_NOT_SET;
}
