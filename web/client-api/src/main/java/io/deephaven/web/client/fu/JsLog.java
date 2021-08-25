package io.deephaven.web.client.fu;

import io.deephaven.web.client.api.LazyString;
import io.deephaven.web.shared.fu.JsProvider;
import jsinterop.annotations.JsProperty;

/**
 * A place where we can shuffle off things we want to log for development, but not necessarily in
 * production.
 *
 */
public class JsLog {

    private JsLog() {}

    @JsProperty(name = "debug", namespace = "console")
    private static native elemental2.core.Function getDebug();

    @JsProperty(name = "info", namespace = "console")
    private static native elemental2.core.Function getInfo();

    @JsProperty(name = "warn", namespace = "console")
    private static native elemental2.core.Function getWarn();

    @JsProperty(name = "error", namespace = "console")
    private static native elemental2.core.Function getError();

    public static boolean shouldSpam() {
        return JsSettings.isDevMode();
    }

    public static void debugMaybe(JsProvider<Object[]> msgs) {
        if (shouldSpam()) {
            debug(msgs.valueOf());
        }
    }

    public static void debug(Object... msgs) {
        if (shouldSpam()) {
            getDebug().apply(null, LazyString.resolve(msgs));
        }
    }

    public static void info(Object... msgs) {
        if (shouldSpam()) {
            getInfo().apply(null, LazyString.resolve(msgs));
        }
    }

    public static void warn(Object... msgs) {
        getWarn().apply(null, LazyString.resolve(msgs));
    }

    public static void error(Object... msgs) {
        getError().apply(null, LazyString.resolve(msgs));
    }
}
