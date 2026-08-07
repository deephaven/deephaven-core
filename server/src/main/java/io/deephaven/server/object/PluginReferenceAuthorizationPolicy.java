//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.object;

import io.deephaven.plugin.type.ObjectType.AuthorizationExportBehavior;

/**
 * The server-wide policy that determines whether a plugin's exported references are run through the authorization
 * transform when the plugin itself makes no {@link AuthorizationExportBehavior declaration}.
 */
public enum PluginReferenceAuthorizationPolicy {
    /**
     * Plugins that do not declare an {@link AuthorizationExportBehavior} are served without transforming their exported
     * references. This preserves historical behavior.
     */
    PERMISSIVE,
    /**
     * Plugins that do not declare an {@link AuthorizationExportBehavior} have their exported references transformed
     * (and a warning is logged prompting the plugin to declare its behavior). A plugin may still explicitly
     * {@link AuthorizationExportBehavior#MANUAL opt out} by taking responsibility for authorization itself.
     */
    FAIL_CLOSED
}
