package io.deephaven.server.jetty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.nio.file.Path;

/**
 * @see <a href="https://github.com/deephaven/js-plugin-template">js-plugin-template</a>
 */
@Immutable
@SimpleStyle
abstract class JsPlugin {

    public static final String NAME = "name";
    public static final String VERSION = "version";
    public static final String MAIN = "main";

    @JsonCreator
    public static JsPlugin of(
            @JsonProperty(value = NAME, required = true) String name,
            @JsonProperty(value = VERSION, required = true) String version,
            @JsonProperty(value = MAIN, required = true) String main) {
        return ImmutableJsPlugin.of(name, version, main);
    }

    /**
     * The name of the plugin.
     */
    @Parameter
    @JsonProperty(NAME)
    public abstract String name();

    /**
     * The version of the plugin.
     */
    @Parameter
    @JsonProperty(VERSION)
    public abstract String version();

    /**
     * The main js file.
     */
    @Parameter
    @JsonProperty(MAIN)
    public abstract String main();

    /**
     * Equivalent to {@code manifestBase.resolve(name()).resolve(main())}.
     *
     * @param manifestBase the manifestBase path
     * @return the main path
     */
    public final Path mainFilePathFromManifestBase(Path manifestBase) {
        return manifestBase.resolve(name()).resolve(main());
    }

    /**
     * Equivalent to {@code manifestBase.resolve(name()).resolve(main()).getParent()}.
     *
     * @param manifestBase the manifestBase path
     * @return the distribution path
     */
    public final Path distributionDirFromManifestBase(Path manifestBase) {
        return mainFilePathFromManifestBase(manifestBase).getParent();
    }

    public final Path distributionDirFromPackageBase(Path packageBase) {
        return packageBase.resolve(main()).getParent();
    }
}
