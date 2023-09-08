package io.deephaven.util.thread;

import io.deephaven.configuration.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Extension point to allow threads that will run user code from within the platform to be controlled by configuration.
 */
public interface ThreadInitializationFactory {
    /* private */ String[] CONFIGURED_INITIALIZATION_TYPES =
            Configuration.getInstance().getStringArrayFromProperty("thread.initialization");
    /* private */ List<ThreadInitializationFactory> INITIALIZERS = Arrays.stream(CONFIGURED_INITIALIZATION_TYPES)
            .filter(str -> !str.isBlank())
            .map(type -> {
                try {
                    // noinspection unchecked
                    Class<? extends ThreadInitializationFactory> clazz =
                            (Class<? extends ThreadInitializationFactory>) Class.forName(type);
                    return clazz.getDeclaredConstructor().newInstance();
                } catch (ClassNotFoundException | NoSuchMethodException | InvocationTargetException
                        | InstantiationException | IllegalAccessException e) {

                    // TODO (https://github.com/deephaven/deephaven-core/issues/4040):
                    // Currently the default property file is shared between both the java client and the server. This
                    // means that client-side usage will attempt to load the thread.initialization property intended for
                    // the server which is not available on the class path.
                    if (e instanceof ClassNotFoundException && type.startsWith("io.deephaven.server.")) {
                        return null;
                    }

                    throw new IllegalArgumentException(
                            "Error instantiating initializer " + type + ", please check configuration", e);
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());

    /**
     * Chains configured initializers to run before/around any given runnable, returning a runnable intended to be run
     * by a new thread.
     */
    static Runnable wrapRunnable(Runnable runnable) {
        Runnable acc = runnable;
        for (ThreadInitializationFactory INITIALIZER : INITIALIZERS) {
            acc = INITIALIZER.createInitializer(acc);
        }
        return acc;
    }

    Runnable createInitializer(Runnable runnable);
}
