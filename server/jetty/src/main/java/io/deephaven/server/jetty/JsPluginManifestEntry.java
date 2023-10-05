/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * @see <a href="https://github.com/deephaven/js-plugin-template">js-plugin-template</a>
 */
@Immutable
@SimpleStyle
abstract class JsPluginManifestEntry {

    public static final String NAME = "name";
    public static final String VERSION = "version";
    public static final String MAIN = "main";

    @JsonCreator
    public static JsPluginManifestEntry of(
            @JsonProperty(value = NAME, required = true) String name,
            @JsonProperty(value = VERSION, required = true) String version,
            @JsonProperty(value = MAIN, required = true) String main) {
        return ImmutableJsPluginManifestEntry.of(name, version, main);
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
}
