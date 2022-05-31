package io.deephaven.jpy;

import io.deephaven.jpy.JpyConfig.Flag;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Represents the source for creating a {@link JpyConfig}.
 *
 * <p>
 * See {@link #asJpyConfig()}
 */
public interface JpyConfigSource {

    // Note: these are "suggested" property names, since they might not be valid in all configuration
    // contexts.

    /**
     * Suggested property name for use with {@link #getFlags()}
     */
    String JPY_FLAGS_PROP = "jpy.flags";

    /**
     * Suggested property name for use with {@link #getExtraPaths()}
     */
    String JPY_EXTRA_PATHS_PROP = "jpy.extraPaths";

    /**
     * Suggested property name for use with {@link #getPythonHome()}
     */
    String JPY_PY_HOME_PROP = "jpy.pythonHome";

    /**
     * Suggested property name for use with {@link #getProgramName()}
     */
    String JPY_PROGRAM_NAME_PROP = "jpy.programName";

    /**
     * Suggested property name for use with {@link #getPythonLib()}. Matches the system property key that jpy uses
     * internally for pythonLib.
     */
    String JPY_PY_LIB_PROP = "jpy.pythonLib";

    /**
     * Suggested property name for use with {@link #getJpyLib()}. Matches the system property key that jpy uses
     * internally for jpyLib.
     */
    String JPY_JPY_LIB_PROP = "jpy.jpyLib";

    /**
     * Suggested property name for use with {@link #getJdlLib()}. Matches the system property key that jpy uses
     * internally for jdlLib.
     */
    String JPY_JDL_LIB_PROP = "jpy.jdlLib";

    Optional<String> getFlags();

    Optional<String> getExtraPaths();

    Optional<String> getPythonHome();

    Optional<String> getProgramName();

    Optional<String> getPythonLib();

    Optional<String> getJpyLib();

    Optional<String> getJdlLib();

    default Map<String, String> asProperties() {
        final Map<String, String> map = new LinkedHashMap<>();
        getFlags().ifPresent(v -> map.put(JPY_FLAGS_PROP, v));
        getExtraPaths().ifPresent(v -> map.put(JPY_EXTRA_PATHS_PROP, v));
        getPythonHome().ifPresent(v -> map.put(JPY_PY_HOME_PROP, v));
        getProgramName().ifPresent(v -> map.put(JPY_PROGRAM_NAME_PROP, v));
        getPythonLib().ifPresent(v -> map.put(JPY_PY_LIB_PROP, v));
        getJpyLib().ifPresent(v -> map.put(JPY_JPY_LIB_PROP, v));
        getJdlLib().ifPresent(v -> map.put(JPY_JDL_LIB_PROP, v));
        return map;
    }

    default EnumSet<Flag> getFlagsSet() {
        final EnumSet<Flag> flags = EnumSet.noneOf(Flag.class);
        getFlags()
                .map(s -> s.split(","))
                .map(Stream::of)
                .orElseGet(Stream::empty)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Flag::valueOf)
                .forEach(flags::add);
        return flags;
    }

    default List<Path> getExtraPathsList() {
        final List<Path> extraPaths = new ArrayList<>();
        getExtraPaths()
                .map(s -> s.split(","))
                .map(Stream::of)
                .orElseGet(Stream::empty)
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Paths::get)
                .forEachOrdered(extraPaths::add);
        return extraPaths;
    }

    default JpyConfig asJpyConfig() {
        return new JpyConfig(
                sanitize(getProgramName()),
                sanitize(getPythonHome()),
                sanitize(getPythonLib()),
                sanitize(getJpyLib()),
                sanitize(getJdlLib()),
                getExtraPathsList(),
                getFlagsSet());
    }

    /* private */ static Path sanitize(Optional<String> value) {
        return value.map(String::trim).filter(s -> !s.isEmpty()).map(Paths::get).orElse(null);
    }

    /**
     * A system property based implementation of {@link JpyConfigSource}, using the suggested property names.
     */
    static JpyConfigSource sysProps() {
        return new FromProperties(System.getProperties());
    }

    final class FromProperties implements JpyConfigSource {
        private final Properties properties;

        public FromProperties(Properties properties) {
            this.properties = Objects.requireNonNull(properties);
        }

        @Override
        public Optional<String> getFlags() {
            return Optional.ofNullable(properties.getProperty(JPY_FLAGS_PROP));
        }

        @Override
        public Optional<String> getExtraPaths() {
            return Optional.ofNullable(properties.getProperty(JPY_EXTRA_PATHS_PROP));
        }

        @Override
        public Optional<String> getPythonHome() {
            return Optional.ofNullable(properties.getProperty(JPY_PY_HOME_PROP));
        }

        @Override
        public Optional<String> getProgramName() {
            return Optional.ofNullable(properties.getProperty(JPY_PROGRAM_NAME_PROP));
        }

        @Override
        public Optional<String> getPythonLib() {
            return Optional.ofNullable(properties.getProperty(JPY_PY_LIB_PROP));
        }

        @Override
        public Optional<String> getJpyLib() {
            return Optional.ofNullable(properties.getProperty(JPY_JPY_LIB_PROP));
        }

        @Override
        public Optional<String> getJdlLib() {
            return Optional.ofNullable(properties.getProperty(JPY_JDL_LIB_PROP));
        }
    }
}
