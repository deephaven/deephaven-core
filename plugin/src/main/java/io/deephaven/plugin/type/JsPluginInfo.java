package io.deephaven.plugin.type;

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
public abstract class JsPluginInfo {

    public static final String NAME = "name";
    public static final String VERSION = "version";
    public static final String MAIN = "main";

    @JsonCreator
    public static JsPluginInfo of(
            @JsonProperty(value = NAME, required = true) String name,
            @JsonProperty(value = VERSION, required = true) String version,
            @JsonProperty(value = MAIN, required = true) String main) {
        return ImmutableJsPluginInfo.of(name, version, main);
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
     * The main js file; the relative path with respect to {@link JsPlugin#copyTo(Path)} destination.
     */
    @Parameter
    @JsonProperty(MAIN)
    public abstract String main();
}
