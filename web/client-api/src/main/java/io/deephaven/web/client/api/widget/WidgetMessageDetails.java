package io.deephaven.web.client.api.widget;

import com.vertispan.tsdefs.annotations.TsInterface;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;

/**
 * Represents the contents of a single widget data message from the server, with a binary data paylod and exported
 * objects. Implemented both by Widget itself and by the {@code event.details} when data is received by the client.
 *
 * Terminology note: the name of this type should probably use "Data" instead of "Message", and the methods should use
 * "payload" rather than "data" to match other platforms and the protobuf itself. These names are instead used for
 * backwards compatibility and to better follow JS expectations.
 */
@JsType(namespace = "dh", name = "WidgetMessageDetails")
@TsInterface
public interface WidgetMessageDetails {
    /**
     * Returns the data from this message as a base64-encoded string.
     */
    String getDataAsBase64();

    /**
     * Returns the data from this message as a Uint8Array.
     */
    Uint8Array getDataAsU8();

    /**
     * Returns the data from this message as a utf-8 string.
     */
    String getDataAsString();

    /**
     * Returns an array of exported objects sent from the server. The plugin implementation is now responsible for these
     * objects, and should close them when no longer needed.
     */
    @JsProperty
    JsWidgetExportedObject[] getExportedObjects();
}
