/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.js;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.nio.file.Path;

/**
 * A manifest-based js plugin sourced from a {@value MANIFEST_JSON} file.
 */
@Immutable
@SimpleStyle
public abstract class JsPluginManifestPath extends JsPluginBase {

    public static final String MANIFEST_JSON = "manifest.json";

    /**
     * Creates a manifest-based js plugin from {@code manifestRoot}.
     *
     * @param manifestRoot the manifest root directory path
     * @return the manifest-based js plugin
     */
    public static JsPluginManifestPath of(Path manifestRoot) {
        return ImmutableJsPluginManifestPath.of(manifestRoot);
    }

    /**
     * The manifest root path directory path.
     * 
     * @return the manifest root directory path
     */
    @Parameter
    public abstract Path path();

    /**
     * The {@value MANIFEST_JSON} file path, relative to {@link #path()}. Equivalent to
     * {@code path().resolve(MANIFEST_JSON)}.
     * 
     * @return the manifest json file path
     */
    public final Path manifestJson() {
        return path().resolve(MANIFEST_JSON);
    }

    /**
     * Equivalent to {@code JsPluginPackagePath.of(path().resolve(name))}.
     *
     * @param name the package name
     * @return the package path
     */
    public final JsPluginPackagePath packagePath(String name) {
        return JsPluginPackagePath.of(path().resolve(name));
    }
}
