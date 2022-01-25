package io.deephaven.plugin;

// Provides a concrete class for extra typing information that can be logged during startup
final class PluginRegistrations extends Registrations {

    public PluginRegistrations(Iterable<? extends Plugin> plugins) {
        super(plugins);
    }
}
