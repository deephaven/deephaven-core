/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import dagger.Binds;
import dagger.Module;
import io.deephaven.plugin.type.JsTypeRegistration;

/**
 * Binds {@link JsTypeRegistrationNoOp} as {@link JsTypeRegistration}.
 */
@Module
public interface JsTypesNoOpModule {
    @Binds
    JsTypeRegistration bindsRegistration(JsTypeRegistrationNoOp noop);
}
