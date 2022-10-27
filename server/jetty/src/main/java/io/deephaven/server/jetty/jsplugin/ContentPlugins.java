/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import io.deephaven.plugin.type.ContentPlugin;
import org.eclipse.jetty.servlet.ServletHolder;

/**
 * The jetty interface for creating {@link ContentPlugin} servlets.
 */
public interface ContentPlugins {

    ServletHolder servletHolder(String name);
}
