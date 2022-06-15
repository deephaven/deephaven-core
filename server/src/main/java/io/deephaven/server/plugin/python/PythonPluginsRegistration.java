/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.python;

import dagger.Binds;
import dagger.multibindings.IntoSet;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.plugin.Registration;

import javax.inject.Inject;
import javax.inject.Provider;

/**
 * Registers all {@link io.deephaven.plugin.Plugin} found via python method "deephaven.plugin:register_all_into". See
 * the deephaven-plugin python package for more information, if {@link ScriptSession#scriptType()} is {@code python}.
 */
public final class PythonPluginsRegistration implements Registration {

    private final Provider<ScriptSession> scriptSessionProvider;

    @Inject
    public PythonPluginsRegistration(Provider<ScriptSession> scriptSessionProvider) {
        this.scriptSessionProvider = scriptSessionProvider;
    }

    @Override
    public void registerInto(Callback callback) {
        if (!scriptSessionProvider.get().scriptType().equalsIgnoreCase("python")) {
            return;
        }
        try (final Deephaven2ServerPluginModule module = Deephaven2ServerPluginModule.of()) {
            module.initialize_all_and_register_into(new CallbackAdapter(callback));
        }
    }

    /**
     * {@link Binds} {@link PythonPluginsRegistration} into the set of {@link Registration}.
     */
    @dagger.Module
    public interface Module {
        @Binds
        @IntoSet
        Registration bindsPythonPluginsRegistration(PythonPluginsRegistration registration);
    }
}
