//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.js;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.plugin.js.JsPlugin;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.Nullable;

/**
 * @see <a href="https://github.com/deephaven/js-plugin-template">js-plugin-template</a>
 */
@Immutable
@BuildableStyle
@JsonInclude(JsonInclude.Include.NON_NULL)
public abstract class JsPluginManifestEntry {

    public static final String NAME = "name";
    public static final String VERSION = "version";
    public static final String MAIN = "main";
    public static final String LOADER = "loader";

    public static Builder builder() {
        return ImmutableJsPluginManifestEntry.builder();
    }

    @JsonCreator
    public static JsPluginManifestEntry of(
            @JsonProperty(value = NAME, required = true) String name,
            @JsonProperty(value = VERSION, required = true) String version,
            @JsonProperty(value = MAIN, required = true) String main) {
        return JsPluginManifestEntry.of(name, version, main, null);
    }

    @JsonCreator
    public static JsPluginManifestEntry of(
            @JsonProperty(value = NAME, required = true) String name,
            @JsonProperty(value = VERSION, required = true) String version,
            @JsonProperty(value = MAIN, required = true) String main,
            @JsonProperty(value = LOADER) Object loader) {
        final Builder builder = builder()
                .name(name)
                .version(version)
                .main(main);
        if (loader != null) {
            builder.loader(loader);
        }
        return builder.build();
    }

    public static JsPluginManifestEntry from(JsPlugin plugin) {
        final Builder builder = builder()
                .name(plugin.name())
                .version(plugin.version())
                .main(plugin.main().toString());
        plugin.loader().ifPresent(builder::loader);
        return builder.build();
    }

    /**
     * The name of the plugin.
     */
    @JsonProperty(NAME)
    public abstract String name();

    /**
     * The version of the plugin.
     */
    @JsonProperty(VERSION)
    public abstract String version();

    /**
     * The main js file.
     */
    @JsonProperty(MAIN)
    public abstract String main();

    /**
     * The optional loader configuration, sourced from the plugin's "loader" package.json field (if present).
     */
    @Nullable
    @JsonProperty(LOADER)
    public abstract Object loader();

    public interface Builder {
        Builder name(String name);

        Builder version(String version);

        Builder main(String main);

        Builder loader(Object loader);

        JsPluginManifestEntry build();
    }
}
