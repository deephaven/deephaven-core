/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.js;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.nio.file.Path;

/**
 * A js plugin package path.
 */
@Immutable
@SimpleStyle
public abstract class JsPluginPackagePath extends JsPluginBase {
    public static final String PACKAGE_JSON = "package.json";

    public static JsPluginPackagePath of(Path packageRoot) {
        return ImmutableJsPluginPackagePath.of(packageRoot);
    }

    /**
     * The package root path.
     * 
     * @return the package root path
     */
    @Parameter
    public abstract Path path();

    /**
     * The {@value PACKAGE_JSON} path. Equivalent to {@code path().resolve(PACKAGE_JSON)}.
     * 
     * @return the package json path
     */
    public final Path packageJson() {
        return path().resolve(PACKAGE_JSON);
    }

    /**
     * The distribution path. Equivalent to {@code path().resolve(main).getParent()}.
     *
     * @param main the main file name
     * @return the distribution path
     */
    public final Path distributionPath(String main) {
        return path().resolve(main).getParent();
    }

    @Override
    public final <T> T walk(JsPlugin.Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
