/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.ContentPlugin;
import io.deephaven.plugin.type.ContentPluginRegistration;
import io.deephaven.plugin.type.JsPlugin;

/**
 * A no-op {@link ContentPluginRegistration}.
 */
public enum ContentPluginRegistrationNoOp implements ContentPluginRegistration, ContentPlugin.Visitor<Void> {
    INSTANCE;

    private static final Logger log = LoggerFactory.getLogger(ContentPluginRegistrationNoOp.class);

    @Override
    public void register(ContentPlugin contentPlugin) {
        contentPlugin.walk(this);
    }


    @Override
    public Void visit(JsPlugin jsPlugin) {
        log.info().append("No-op registration for js plugin '")
                .append(jsPlugin.info().name()).append('@').append(jsPlugin.info().version())
                .append('\'')
                .endl();
        return null;
    }
}
