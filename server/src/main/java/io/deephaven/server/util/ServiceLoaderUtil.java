package io.deephaven.server.util;

import io.deephaven.server.session.SessionService;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;
import java.util.stream.Collectors;

public class ServiceLoaderUtil {
    public static <T> Optional<T> loadOne(SessionService sessionService, Class<T> clazz) {
        final List<Provider<T>> providers = ServiceLoader.load(clazz).stream().collect(Collectors.toList());
        if (providers.isEmpty()) {
            return Optional.empty();
        }
        if (providers.size() == 1) {
            final Class<? extends T> providerClazz = providers.get(0).type();
            try {
                return Optional
                        .of(providerClazz.getDeclaredConstructor(SessionService.class).newInstance(sessionService));
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException
                    | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
        final String classes =
                providers.stream().map(Provider::type).map(Class::toString).collect(Collectors.joining(",", "[", "]"));
        throw new IllegalStateException(String.format("Found multiple '%s', expected one or none: %s", clazz, classes));
    }
}
