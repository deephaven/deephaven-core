/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeRegistration;

import java.util.Objects;
import java.util.function.Consumer;

final class PluginRegistrationVisitor
        implements io.deephaven.plugin.Registration.Callback, Plugin.Visitor<PluginRegistrationVisitor> {

    private final ObjectTypeRegistration objectTypeRegistration;
    private final Consumer<JsPlugin> jsPluginConsumer;

    PluginRegistrationVisitor(
            ObjectTypeRegistration objectTypeRegistration,
            Consumer<JsPlugin> jsPluginConsumer) {
        this.objectTypeRegistration = Objects.requireNonNull(objectTypeRegistration);
        this.jsPluginConsumer = Objects.requireNonNull(jsPluginConsumer);
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
        jsPluginConsumer.accept(jsPlugin);
        return this;
    }
}
