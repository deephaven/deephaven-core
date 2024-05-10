//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.runner;

import java.lang.Runtime.Version;
import java.lang.management.ManagementFactory;
import java.util.List;

/**
 * Deephaven safety checks.
 */
public final class SafetyChecks {
    public static final String SAFETY_CHECKS_PROPERTY = "deephaven.safetyChecks";
    public static final String SAFETY_CHECKS_ENV = "DEEPHAVEN_SAFETY_CHECKS";

    /**
     * Checks whether safety checks are enabled. This parses the system property {@value SAFETY_CHECKS_PROPERTY} if it
     * exists, otherwise it parses the environment variable {@value SAFETY_CHECKS_ENV} if it exists, otherwise defaults
     * to {@code true}. Disabling safety checks is not recommended.
     *
     * @return if safety checks are enabled.
     */
    public static boolean isEnabled() {
        return parseBoolean(SAFETY_CHECKS_PROPERTY, SAFETY_CHECKS_ENV, true);
    }

    /**
     * Applies a set of safety checks if {@link #isEnabled()}.
     *
     * @see JDK_8287432
     */
    public static void check() {
        if (!isEnabled()) {
            return;
        }
        JDK_8287432.check();
    }

    private static boolean isEnabled(Class<?> clazz) {
        return parseBoolean(safetyCheckPropertyName(clazz), safetyCheckEnvironmentName(clazz), true);
    }

    private static String safetyCheckEnvironmentName(Class<?> clazz) {
        return "DEEPHAVEN_SAFETY_CHECK_" + clazz.getSimpleName();
    }

    private static String safetyCheckPropertyName(Class<?> clazz) {
        return "deephaven.safetyCheck." + clazz.getSimpleName();
    }

    private static boolean parseBoolean(String propertyName, String envName, boolean defaultValue) {
        final String propertyValue = System.getProperty(propertyName);
        if (propertyValue != null) {
            return Boolean.parseBoolean(propertyValue);
        }
        final String envValue = System.getenv(envName);
        if (envValue != null) {
            return Boolean.parseBoolean(envValue);
        }
        return defaultValue;
    }

    private static String disableSafetyCheckMessage(Class<?> clazz) {
        return String.format(
                "To disable this safety check (not recommended), you can set the system property `-D%s=false` or environment variable `%s=false`.",
                safetyCheckPropertyName(clazz), safetyCheckEnvironmentName(clazz));
    }

    private static String disableSafetyChecksMessage() {
        return String.format(
                "To disable all safety checks (not recommended), you can set the system property `-D%s=false` or environment variable `%s=false`.",
                SAFETY_CHECKS_PROPERTY, SAFETY_CHECKS_ENV);
    }

    private static IllegalStateException exception(Class<?> clazz, String baseMessage) {
        return new IllegalStateException(String.format(
                "%s %s %s",
                baseMessage,
                disableSafetyCheckMessage(clazz),
                disableSafetyChecksMessage()));
    }

    /**
     * A safety check for <a href="https://bugs.openjdk.org/browse/JDK-8287432">JDK-8287432</a>.
     */
    public static final class JDK_8287432 {

        private static void check() {
            if (!isEnabled(JDK_8287432.class)) {
                return;
            }
            if (isVulnerableVersion() && !hasWorkaround()) {
                throw exception(JDK_8287432.class, String.format(
                        "The current JDK %s (located at %s) is vulnerable to the bug https://bugs.openjdk.org/browse/JDK-8287432. We recommend updating to 11.0.17+, 17.0.5+, or 21+. If that is not possible, you can apply a workaround '%s'.",
                        Runtime.version(),
                        System.getProperty("java.home"),
                        String.join(" ", workaroundJvmArguments())));
            }
        }

        private static boolean isVulnerableVersion(Version version) {
            switch (version.feature()) {
                case 11:
                    return version.compareTo(Version.parse("11.0.17")) < 0;
                case 17:
                    return version.compareTo(Version.parse("17.0.5")) < 0;
            }
            // Assume the version does not have the bug.
            return false;
        }

        private static boolean isVulnerableVersion() {
            return isVulnerableVersion(Runtime.version());
        }

        private static boolean hasWorkaround() {
            return ManagementFactory.getRuntimeMXBean().getInputArguments()
                    .contains("-XX:DisableIntrinsic=_currentThread");
        }

        private static List<String> workaroundJvmArguments() {
            return List.of("-XX:+UnlockDiagnosticVMOptions", "-XX:DisableIntrinsic=_currentThread");
        }
    }
}
