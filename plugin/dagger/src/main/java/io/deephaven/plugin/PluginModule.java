/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.type.ObjectType;

import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides the set of {@link Registration} from {@link ServiceLoader#load(Class)} against the classes
 * {@link Registration}, {@link Plugin}, {@link ObjectType}, and {@link JsPlugin}.
 */
@Module
public interface PluginModule {

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesServiceLoaderRegistrations() {
        return ServiceLoader.load(Registration.class).stream().map(Provider::get).collect(Collectors.toSet());
    }

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesServiceLoaderPlugins() {
        return ServiceLoader.load(Plugin.class).stream().map(Provider::get).collect(Collectors.toSet());
    }

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesServiceLoaderObjectTypes() {
        return ServiceLoader.load(ObjectType.class).stream().map(Provider::get).collect(Collectors.toSet());
    }

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesServiceLoaderJsPlugin() {
        return ServiceLoader.load(JsPlugin.class).stream().map(Provider::get).collect(Collectors.toSet());
    }
}
