//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.configuration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public final class ConfigDir {
    public static final String PROPERTY = "deephaven.configDir";
    public static final String ENV_VAR = "DEEPHAVEN_CONFIG_DIR";
    static final String ROOT_FILE_PROP = "Configuration.rootFile";
    private static final String DEFAULT_CONFIG_FILE_NAME = "deephaven.prop";
    private static final String DEFAULT_CONFIGURATION_FILE = "dh-defaults.prop";

    /**
     * Gets the config directory by the system property {@value #PROPERTY} or environment variable {@value #ENV_VAR} if
     * present.
     *
     * @return the config directory
     */
    public static Optional<Path> get() {
        return getOptional().map(Paths::get);
    }

    /**
     * Gets the config directory if the system property {@value #PROPERTY} or environment variable {@value #ENV_VAR} is
     * present, otherwise sets the system property {@value #PROPERTY} to {@code defaultValue} and returns
     * {@code defaultValue}.
     *
     * @param defaultValue the value to set if none is present
     * @return the config directory
     */
    public static Path getOrSet(String defaultValue) {
        final String existing = getOptional().orElse(null);
        if (existing != null) {
            return Paths.get(existing);
        }
        System.setProperty(PROPERTY, defaultValue);
        return Paths.get(defaultValue);
    }

    /**
     * Gets the configuration file, first by the system property {@value #ROOT_FILE_PROP} if set; otherwise the filename
     * {@value #DEFAULT_CONFIG_FILE_NAME} in {@link #get() the config directory} if the file exists; and otherwise
     * returns {@value #DEFAULT_CONFIGURATION_FILE}.
     *
     * @return the configuration file
     */
    public static String configurationFile() {
        Optional<String> optional = Optional.ofNullable(System.getProperty(ROOT_FILE_PROP));
        if (!optional.isPresent()) {
            optional = configDirectoryFileIfExists();
        }
        return optional.orElse(DEFAULT_CONFIGURATION_FILE);
    }

    private static Optional<String> configDirectoryFileIfExists() {
        return get()
                .map(ConfigDir::defaultFileName)
                .filter(Files::exists)
                .map(Path::toString);
    }

    private static Optional<String> getOptional() {
        Optional<String> optional = viaProperty();
        if (!optional.isPresent()) {
            optional = viaEnvVar();
        }
        return optional;
    }

    private static Path defaultFileName(Path p) {
        return p.resolve(DEFAULT_CONFIG_FILE_NAME);
    }

    private static Optional<String> viaProperty() {
        return Optional.ofNullable(System.getProperty(PROPERTY));
    }

    private static Optional<String> viaEnvVar() {
        return Optional.ofNullable(System.getenv(ENV_VAR));
    }
}
