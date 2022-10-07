/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.JsPlugin;
import io.deephaven.plugin.type.JsPluginRegistration;

import javax.inject.Inject;

/**
 * A no-op {@link JsPluginRegistration}.
 */
public final class JsPluginRegistrationNoOp implements JsPluginRegistration {
    private static final Logger log = LoggerFactory.getLogger(JsPluginRegistrationNoOp.class);

    @Inject
    public JsPluginRegistrationNoOp() {}

    @Override
    public void register(JsPlugin jsPlugin) {
        log.info().append("No-op registration for js plugin '")
                .append(jsPlugin.name()).append('@').append(jsPlugin.version())
                .append('\'')
                .endl();
    }
}
