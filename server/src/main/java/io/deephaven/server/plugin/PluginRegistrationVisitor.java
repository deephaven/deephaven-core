/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.type.JsType;
import io.deephaven.plugin.type.JsTypeRegistration;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeRegistration;

import javax.inject.Inject;
import java.util.Objects;

final class PluginRegistrationVisitor
        implements io.deephaven.plugin.Registration.Callback, Plugin.Visitor<PluginRegistrationVisitor> {

    private final ObjectTypeRegistration objectTypeRegistration;
    private final JsTypeRegistration jsTypeRegistration;

    @Inject
    public PluginRegistrationVisitor(ObjectTypeRegistration objectTypeRegistration,
            JsTypeRegistration jsTypeRegistration) {
        this.objectTypeRegistration = Objects.requireNonNull(objectTypeRegistration);
        this.jsTypeRegistration = Objects.requireNonNull(jsTypeRegistration);
    }

    @Override
    public void register(Plugin plugin) {
        plugin.walk(this);
    }

    @Override
    public PluginRegistrationVisitor visit(ObjectType objectType) {
        objectTypeRegistration.register(objectType);
        return this;
    }

    @Override
    public PluginRegistrationVisitor visit(JsType jsType) {
        jsTypeRegistration.register(jsType);
        return this;
    }
}
