package io.deephaven.server.plugin;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.type.ObjectType;
import io.deephaven.plugin.type.ObjectTypeRegistration;

import javax.inject.Inject;
import java.util.Objects;

final class PluginRegistrationVisitor
        implements io.deephaven.plugin.Registration.Callback, Plugin.Visitor<PluginRegistrationVisitor> {

    private final ObjectTypeRegistration objectTypeRegistration;

    @Inject
    public PluginRegistrationVisitor(ObjectTypeRegistration objectTypeRegistration) {
        this.objectTypeRegistration = Objects.requireNonNull(objectTypeRegistration);
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
}
