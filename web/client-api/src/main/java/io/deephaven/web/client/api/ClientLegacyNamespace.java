package io.deephaven.web.client.api;

import jsinterop.annotations.JsType;

/**
 * Deprecated for use in Deephaven Core.
 */
@Deprecated
@JsType(namespace = "dh", name = "Client")
public class ClientLegacyNamespace {
    public static final String EVENT_REQUEST_FAILED = "requestfailed";
    public static final String EVENT_REQUEST_STARTED = "requeststarted";
    public static final String EVENT_REQUEST_SUCCEEDED = "requestsucceeded";
}
