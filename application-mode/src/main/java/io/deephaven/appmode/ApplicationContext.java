package io.deephaven.appmode;

import java.util.function.Consumer;

/**
 * This application context can be used to get access to the application state from within script applications.
 * <p/>
 * {@link ApplicationContext#get} is only valid during the initial invocation of a script application during start up of
 * Application Mode. Scripts may dynamically add fields after start up by capturing and using the ApplicationState after
 * the script after having returned execution control to the begin the server process.
 * <p/>
 * Each application owns and manages a unique ApplicationState.
 */
public class ApplicationContext {

    private static final ThreadLocal<ApplicationState> states = new ThreadLocal<>();

    public static ApplicationState get() {
        final ApplicationState state = states.get();
        if (state == null) {
            throw new IllegalStateException("Should not be getting application state outside runUnderContext");
        }
        return state;
    }

    public static void initialize(Consumer<ApplicationState> initializer) {
        initializer.accept(get());
    }

    public static void runUnderContext(final ApplicationState context, final Runnable runner) {
        ApplicationContext.states.set(context);
        try {
            runner.run();
        } finally {
            ApplicationContext.states.remove();
        }
    }
}
