package io.deephaven.engine.util.jpy;

import io.deephaven.configuration.Configuration;
import io.deephaven.jpy.JpyConfig;
import io.deephaven.jpy.JpyConfigSource;
import java.util.Objects;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;

/**
 * Encapsulates our loading of {@link JpyConfig} from a {@link Configuration}
 */
class JpyConfigLoader implements JpyConfigSource {

    private final Configuration conf;

    JpyConfigLoader(@NotNull Configuration conf) {
        this.conf = Objects.requireNonNull(conf);
    }

    @Override
    public Optional<String> getFlags() {
        return Optional.ofNullable(conf.getStringWithDefault(JPY_FLAGS_PROP, null));
    }

    @Override
    public Optional<String> getExtraPaths() {
        return Optional.ofNullable(conf.getStringWithDefault(JPY_EXTRA_PATHS_PROP, null));
    }

    @Override
    public Optional<String> getPythonHome() {
        return Optional.ofNullable(conf.getStringWithDefault(JPY_PY_HOME_PROP, null));
    }

    @Override
    public Optional<String> getProgramName() {
        return Optional.ofNullable(conf.getStringWithDefault(JPY_PROGRAM_NAME_PROP, null));
    }

    @Override
    public Optional<String> getPythonLib() {
        return Optional.ofNullable(conf.getStringWithDefault(JPY_PY_LIB_PROP, null));
    }

    @Override
    public Optional<String> getJpyLib() {
        return Optional.ofNullable(conf.getStringWithDefault(JPY_JPY_LIB_PROP, null));
    }

    @Override
    public Optional<String> getJdlLib() {
        return Optional.ofNullable(conf.getStringWithDefault(JPY_JDL_LIB_PROP, null));
    }
}
