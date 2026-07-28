//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.js;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.plugin.js.JsPlugin;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * @see <a href="https://github.com/deephaven/js-plugin-template">js-plugin-template</a>
 */
@Immutable
@SimpleStyle
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class JsPluginManifestEntry {

    public static final String NAME = "name";
    public static final String VERSION = "version";
    public static final String MAIN = "main";
    public static final String LOADER = "loader";

    @JsonCreator
    public static JsPluginManifestEntry of(
            @JsonProperty(value = NAME, required = true) String name,
            @JsonProperty(value = VERSION, required = true) String version,
            @JsonProperty(value = MAIN, required = true) String main,
            @JsonProperty(value = LOADER) Map<String, Object> loader) {
        return ImmutableJsPluginManifestEntry.of(name, version, main, loader);
    }

    public static JsPluginManifestEntry from(JsPlugin plugin) {
        return ImmutableJsPluginManifestEntry.of(plugin.name(), plugin.version(), plugin.main().toString(),
                plugin.loader().orElse(null));
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
     * The optional loader configuration, sourced from the plugin's "loader" package.json field (if present).
     */
    @Nullable
    @Parameter
    @JsonProperty(LOADER)
    public abstract Map<String, Object> loader();
}
