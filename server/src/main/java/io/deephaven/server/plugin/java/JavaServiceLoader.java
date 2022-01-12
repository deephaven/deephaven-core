package io.deephaven.server.plugin.java;

import io.deephaven.plugin.Registration;
import io.deephaven.plugin.Registration.Callback;

import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;

public final class JavaServiceLoader {

    /**
     * Initializes all {@link Registration registrations} found via {@link ServiceLoader#load(Class)}, and register them
     * into {@code callback}.
     *
     * @param callback the plugin callback
     */
    public static void initializeAllAndRegisterInto(Callback callback) {
        final List<Registration> registrations = loadFromServiceLoader();
        for (Registration registration : registrations) {
            registration.init();
        }
        for (Registration registration : registrations) {
            registration.registerInto(callback);
        }
    }

    private static List<Registration> loadFromServiceLoader() {
        return ServiceLoader
                .load(Registration.class)
                .stream()
                .map(Provider::get)
                .collect(Collectors.toList());
    }
}
