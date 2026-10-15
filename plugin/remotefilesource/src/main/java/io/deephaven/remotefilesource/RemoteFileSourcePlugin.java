//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.remotefilesource;

import com.google.auto.service.AutoService;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeBase;
import io.deephaven.plugin.type.ObjectCommunicationException;
import io.deephaven.plugin.type.PluginMarker;

/**
 * ObjectType plugin for remote file sources. This plugin is registered via @AutoService and handles creation of
 * RemoteFileSourceMessageStream connections for bidirectional communication with clients.
 * <p>
 * This plugin recognizes PluginMarker objects whose pluginName matches this plugin's name. When a connection is
 * established, a RemoteFileSourceMessageStream is created to handle bidirectional message passing between client and
 * server.
 * <p>
 * A RemoteFileSourceMessageStream serves resources for a script evaluation only once its client has declared them; see
 * {@link io.deephaven.engine.util.remotefilesource.RemoteFileSourceClassLoader} for how a declaration is claimed by the
 * run it was made for.
 * <p>
 * There is no dedicated RPC for obtaining the marker. A client gets one by evaluating a script that calls
 * {@link #getMarker()} in a console session and exporting the resulting variable, so only a caller permitted to run
 * code on the worker can open a connection.
 */
@AutoService(ObjectType.class)
public class RemoteFileSourcePlugin extends ObjectTypeBase {
    public static final String NAME = "DeephavenRemoteFileSourcePlugin";

    /**
     * The single marker instance for this plugin. Markers are matched by plugin name, but exports for a given plugin
     * name are expected to share one instance rather than creating one per request.
     */
    private static final PluginMarker MARKER = new PluginMarker(NAME);

    /**
     * Returns the marker object that connects to this plugin. Clients evaluate a script calling this method to obtain
     * an exportable reference to the plugin, so its name and signature are part of the client contract.
     *
     * @return the marker for this plugin
     */
    public static PluginMarker getMarker() {
        return MARKER;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean isType(Object object) {
        if (object instanceof PluginMarker) {
            PluginMarker marker = (PluginMarker) object;
            return NAME.equals(marker.getPluginName());
        }
        return false;
    }

    @Override
    public MessageStream compatibleClientConnection(Object object, MessageStream connection)
            throws ObjectCommunicationException {
        if (!isType(object)) {
            throw new ObjectCommunicationException("Expected RemoteFileSource marker object, got " + object.getClass());
        }

        // Return a new bidirectional message stream for this connection
        return new RemoteFileSourceMessageStream(connection);
    }
}

