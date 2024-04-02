//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.js;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.plugin.js.JsPlugin;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static io.deephaven.server.plugin.js.Jackson.OBJECT_MAPPER;

@Immutable
@SimpleStyle
public abstract class JsPluginManifest {
    public static final String PLUGINS = "plugins";
    public static final String MANIFEST_JSON = "manifest.json";

    @JsonCreator
    public static JsPluginManifest of(
            @JsonProperty(value = PLUGINS, required = true) List<JsPluginManifestEntry> plugins) {
        return ImmutableJsPluginManifest.of(plugins);
    }

    public static JsPluginManifest from(List<JsPlugin> plugins) {
        return of(plugins.stream().map(JsPluginManifestEntry::from).collect(Collectors.toList()));
    }

    static JsPluginManifest read(Path manifestJson) throws IOException {
        // jackson impl does buffering internally
        try (final InputStream in = Files.newInputStream(manifestJson)) {
            return OBJECT_MAPPER.readValue(in, JsPluginManifest.class);
        }
    }

    @Parameter
    @JsonProperty(PLUGINS)
    public abstract List<JsPluginManifestEntry> plugins();
}
