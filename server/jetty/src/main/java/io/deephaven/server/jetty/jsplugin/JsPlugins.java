/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import org.eclipse.jetty.servlet.ServletHolder;

/**
 * The jetty interface for creating {@link io.deephaven.plugin.type.JsPlugin} servlets.
 */
public interface JsPlugins {

    ServletHolder servletHolder(String name);
}
