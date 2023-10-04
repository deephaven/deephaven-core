/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.js;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.nio.file.Path;

/**
 * A package-based js plugin sourced from a {@value PACKAGE_JSON} file.
 */
@Immutable
@SimpleStyle
public abstract class JsPluginPackagePath extends JsPluginBase {
    public static final String PACKAGE_JSON = "package.json";

    /**
     * Creates a package-based js plugin from {@code packageRoot}.
     *
     * @param packageRoot the package root directory path
     * @return the package-based js plugin
     */
    public static JsPluginPackagePath of(Path packageRoot) {
        return ImmutableJsPluginPackagePath.of(packageRoot);
    }

    /**
     * The package root directory path.
     * 
     * @return the package root directory path
     */
    @Parameter
    public abstract Path path();

    /**
     * The {@value PACKAGE_JSON} file path. Equivalent to {@code path().resolve(PACKAGE_JSON)}.
     * 
     * @return the package json file path
     */
    public final Path packageJson() {
        return path().resolve(PACKAGE_JSON);
    }
}
