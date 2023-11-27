/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.js.JsPluginRegistration;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeRegistration;

import javax.inject.Inject;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Plugin {@link io.deephaven.plugin.Registration.Callback} implementation that forwards registered plugins to a
 * {@link ObjectTypeRegistration} or {@link JsPluginRegistration}.
 */
final class PluginRegistrationVisitor
        implements io.deephaven.plugin.Registration.Callback, Plugin.Visitor<PluginRegistrationVisitor> {

    private final ObjectTypeRegistration objectTypeRegistration;
    private final JsPluginRegistration jsPluginRegistration;

    @Inject
    PluginRegistrationVisitor(
            ObjectTypeRegistration objectTypeRegistration,
            JsPluginRegistration jsPluginRegistration) {
        this.objectTypeRegistration = Objects.requireNonNull(objectTypeRegistration);
        this.jsPluginRegistration = Objects.requireNonNull(jsPluginRegistration);
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
    public PluginRegistrationVisitor visit(JsPlugin jsPlugin) {
        jsPluginRegistration.register(jsPlugin);
        return this;
    }
}
