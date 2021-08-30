package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb.FieldsChangeUpdate;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.application_pb.ListFieldsRequest;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.application_pb_service.ApplicationServiceClient",
        namespace = JsPackage.GLOBAL)
public class ApplicationServiceClient {
    public String serviceHost;

    public ApplicationServiceClient(String serviceHost, Object options) {}

    public ApplicationServiceClient(String serviceHost) {}

    public native ResponseStream<FieldsChangeUpdate> listFields(
            ListFieldsRequest requestMessage, BrowserHeaders metadata);

    public native ResponseStream<FieldsChangeUpdate> listFields(ListFieldsRequest requestMessage);
}
