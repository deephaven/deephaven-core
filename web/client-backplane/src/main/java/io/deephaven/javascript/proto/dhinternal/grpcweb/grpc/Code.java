package io.deephaven.javascript.proto.dhinternal.grpcweb.grpc;

import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(isNative = true, name = "dhinternal.grpcWeb.grpc.Code", namespace = JsPackage.GLOBAL)
public class Code {
    public static int Aborted,
            AlreadyExists,
            Canceled,
            DataLoss,
            DeadlineExceeded,
            FailedPrecondition,
            Internal,
            InvalidArgument,
            NotFound,
            OK,
            OutOfRange,
            PermissionDenied,
            ResourceExhausted,
            Unauthenticated,
            Unavailable,
            Unimplemented,
            Unknown;
}
