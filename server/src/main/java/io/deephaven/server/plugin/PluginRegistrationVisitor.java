/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.type.JsPlugin;
import io.deephaven.plugin.type.JsPluginRegistration;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeRegistration;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

final class PluginRegistrationVisitor
        implements io.deephaven.plugin.Registration.Callback, Plugin.Visitor<PluginRegistrationVisitor> {

    private final ObjectTypeRegistration objectTypeRegistration;
    private final JsPluginRegistration jsPluginRegistration;

    @Inject
    public PluginRegistrationVisitor(ObjectTypeRegistration objectTypeRegistration,
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
        try {
            jsPluginRegistration.register(jsPlugin);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }
}
