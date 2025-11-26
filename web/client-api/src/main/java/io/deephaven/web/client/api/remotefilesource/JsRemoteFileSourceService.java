package io.deephaven.web.client.api.remotefilesource;

import io.deephaven.web.client.api.event.HasEventHandling;
import jsinterop.annotations.JsType;

/**
 * JavaScript client for the RemoteFileSource service.
 */
@JsType(namespace = "dh.remotefilesource", name = "RemoteFileSourceService")
public class JsRemoteFileSourceService extends HasEventHandling {
    // TODO: This needs to send a RemoteFileSourcePluginFetchRequest flight message to get a plugin service instance
}
