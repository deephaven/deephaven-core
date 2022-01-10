package io.deephaven.server.plugin.java;

import io.deephaven.plugin.Registration;
import io.deephaven.plugin.Registration.Callback;

import java.util.ServiceLoader;

public final class JavaServiceLoader {

    /**
     * Registers all {@link Registration plugins} found via {@link ServiceLoader#load(Class)}.
     *
     * @param callback the plugin callback
     */
    public static void allRegisterInto(Callback callback) {
        for (Registration provider : ServiceLoader.load(Registration.class)) {
            provider.registerInto(callback);
        }
    }
}
