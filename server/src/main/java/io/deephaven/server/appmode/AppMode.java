package io.deephaven.server.appmode;

import io.deephaven.appmode.ApplicationConfig;
import io.deephaven.server.console.ConsoleServiceGrpcImpl;

public enum AppMode {
    APP_ONLY, HYBRID, CONSOLE_ONLY, API_ONLY;

    public static AppMode currentMode() {
        boolean appEnabled = ApplicationConfig.isApplicationModeEnabled();
        boolean consoleEnabled = !ConsoleServiceGrpcImpl.REMOTE_CONSOLE_DISABLED;
        if (appEnabled && consoleEnabled) {
            return HYBRID;
        }
        if (appEnabled) {
            return APP_ONLY;
        }
        if (consoleEnabled) {
            return CONSOLE_ONLY;
        }
        return API_ONLY;
    }

    public boolean hasVisibilityToAppExports() {
        return this == HYBRID || this == APP_ONLY;
    }

    public boolean hasVisibilityToConsoleExports() {
        return this == HYBRID || this == CONSOLE_ONLY;
    }
}
