/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.List;

@Immutable
@SimpleStyle
abstract class JsPluginManifest {
    public static final String PLUGINS = "plugins";

    @JsonCreator
    public static JsPluginManifest of(
            @JsonProperty(value = PLUGINS, required = true) List<JsPluginManifestEntry> plugins) {
        return ImmutableJsPluginManifest.of(plugins);
    }

    @Parameter
    @JsonProperty(PLUGINS)
    public abstract List<JsPluginManifestEntry> plugins();
}
