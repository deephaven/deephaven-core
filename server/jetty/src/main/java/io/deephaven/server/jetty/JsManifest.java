package io.deephaven.server.jetty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.List;

@Immutable
@SimpleStyle
abstract class JsManifest {
    public static final String PLUGINS = "plugins";

    @JsonCreator
    public static JsManifest of(
            @JsonProperty(value = PLUGINS, required = true) List<JsPlugin> plugins) {
        return ImmutableJsManifest.of(plugins);
    }

    @Parameter
    @JsonProperty(PLUGINS)
    public abstract List<JsPlugin> plugins();
}
