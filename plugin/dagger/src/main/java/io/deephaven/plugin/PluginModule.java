//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.engine.validation.ColumnExpressionValidator;
import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.type.ObjectType;

import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Provides the set of {@link Registration} from {@link ServiceLoader#load(Class)} against the classes
 * {@link Registration}, {@link Plugin}, {@link ObjectType}, and {@link JsPlugin}.
 */
@Module
public interface PluginModule {
    @Provides
    static PluginOptions pluginOptions(ColumnExpressionValidator validator) {
        return PluginOptions.builder().columnExpressionValidator(validator).build();
    }

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesServiceLoaderRegistrations(PluginOptions options) {
        return ServiceLoader.load(Registration.class).stream()
                .map(provider -> applyPluginOptions(provider.get(), options)).collect(Collectors.toSet());
    }

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesServiceLoaderPlugins(PluginOptions options) {
        return ServiceLoader.load(Plugin.class).stream().map(provider -> applyPluginOptions(provider.get(), options))
                .collect(Collectors.toSet());
    }

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesServiceLoaderObjectTypes(PluginOptions options) {
        return ServiceLoader.load(ObjectType.class).stream()
                .map(provider -> applyPluginOptions(provider.get(), options)).collect(Collectors.toSet());
    }

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesServiceLoaderJsPlugin(PluginOptions options) {
        return ServiceLoader.load(JsPlugin.class).stream().map(provider -> applyPluginOptions(provider.get(), options))
                .collect(Collectors.toSet());
    }

    static Registration applyPluginOptions(Registration registration, PluginOptions options) {
        if (registration instanceof AcceptsPluginOptions) {
            ((AcceptsPluginOptions) registration).setPluginOptions(options);
        }
        return registration;
    }
}
