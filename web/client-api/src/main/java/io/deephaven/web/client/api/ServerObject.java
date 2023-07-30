package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsInterface;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.TypedTicket;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;

/**
 * Indicates that this object is a local representation of an object that exists on the server. Known implementations
 * include JsTable, JsWidget, JsWidgetExportedObject, etc.
 */
@JsType(namespace = "dh")
@TsInterface
public interface ServerObject {
    @JsIgnore
    TypedTicket typedTicket();
}
