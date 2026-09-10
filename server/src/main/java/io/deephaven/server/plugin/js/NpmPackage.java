//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.js;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.deephaven.server.plugin.js.Jackson.OBJECT_MAPPER;

@Immutable
@BuildableStyle
abstract class NpmPackage {

    static final String LOADER = "loader";

    public static Builder builder() {
        return ImmutableNpmPackage.builder();
    }

    @JsonCreator
    public static NpmPackage of(
            @JsonProperty(value = JsPluginNpmPackageRegistration.NAME, required = true) String name,
            @JsonProperty(value = JsPluginNpmPackageRegistration.VERSION, required = true) String version,
            @JsonProperty(value = JsPluginNpmPackageRegistration.MAIN, required = true) String main,
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

    public static NpmPackage read(Path packageJson) throws IOException {
        // jackson impl does buffering internally
        try (final InputStream in = Files.newInputStream(packageJson)) {
            return OBJECT_MAPPER.readValue(in, NpmPackage.class);
        }
    }

    @JsonProperty(JsPluginNpmPackageRegistration.NAME)
    public abstract String name();

    @JsonProperty(JsPluginNpmPackageRegistration.VERSION)
    public abstract String version();

    @JsonProperty(JsPluginNpmPackageRegistration.MAIN)
    public abstract String main();

    /**
     * The optional "loader" field, an arbitrary plugin-specific JSON value.
     */
    @Nullable
    @JsonProperty(LOADER)
    public abstract Object loader();

    public interface Builder {
        Builder name(String name);

        Builder version(String version);

        Builder main(String main);

        Builder loader(Object loader);

        NpmPackage build();
    }
}
