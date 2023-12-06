/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.js;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.deephaven.server.plugin.js.Jackson.OBJECT_MAPPER;

@Immutable
@SimpleStyle
abstract class NpmPackage {
    public static final String NAME = "name";
    public static final String VERSION = "version";
    public static final String MAIN = "main";

    @JsonCreator
    public static NpmPackage of(
            @JsonProperty(value = NAME, required = true) String name,
            @JsonProperty(value = VERSION, required = true) String version,
            @JsonProperty(value = MAIN, required = true) String main) {
        return ImmutableNpmPackage.of(name, version, main);
    }

    public static NpmPackage read(Path packageJson) throws IOException {
        // jackson impl does buffering internally
        try (final InputStream in = Files.newInputStream(packageJson)) {
            return OBJECT_MAPPER.readValue(in, NpmPackage.class);
        }
    }

    @Parameter
    @JsonProperty(NAME)
    public abstract String name();

    @Parameter
    @JsonProperty(VERSION)
    public abstract String version();

    @Parameter
    @JsonProperty(MAIN)
    public abstract String main();

    // todo: files?
    // todo: deephaven object?
}
