/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.plugin.type.JsType;
import io.deephaven.plugin.type.JsTypeRegistration;

import javax.inject.Inject;

/**
 * A no-op {@link JsTypeRegistration}.
 */
public final class JsTypeRegistrationNoOp implements JsTypeRegistration {
    private static final Logger log = LoggerFactory.getLogger(JsTypeRegistrationNoOp.class);

    @Inject
    public JsTypeRegistrationNoOp() {}

    @Override
    public void register(JsType jsType) {
        log.info().append("No-op registration for js type '")
                .append(jsType.name()).append('@').append(jsType.version())
                .append('\'')
                .endl();
    }
}
