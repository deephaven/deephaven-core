package io.deephaven.server.plugin.python;

import dagger.Binds;
import dagger.multibindings.IntoSet;
import io.deephaven.plugin.Registration;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;

import javax.inject.Inject;

/**
 * Registers all {@link io.deephaven.plugin.Plugin} found via python method "deephaven.plugin:register_all_into". See
 * the deephaven-plugin python package for more information, if {@link ConsoleServiceGrpcImpl#isPythonSession()} is
 * {@code true}.
 */
public final class PythonPluginsRegistration implements Registration {

    @Inject
    public PythonPluginsRegistration() {}

    @Override
    public void registerInto(Callback callback) {
        if (!ConsoleServiceGrpcImpl.isPythonSession()) {
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
