/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.js;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.PluginBase;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A JS plugin is a {@link Plugin} that allows adding javascript code under the server's URL path "js-plugins/". See
 * <a href="https://github.com/deephaven/deephaven-plugins#js-plugins">deephaven-plugins#js-plugins</a> for more details
 * about the underlying construction for JS plugins.
 */
@Immutable
@BuildableStyle
public abstract class JsPlugin extends PluginBase {

    public static Builder builder() {
        return ImmutableJsPlugin.builder();
    }

    /**
     * The JS plugin name. The JS plugin contents will be served via the URL path "js-plugins/{name}/", as well as
     * included as the "name" field for the manifest entry in "js-plugins/manifest.json".
     *
     * @return the name
     */
    public abstract String name();

    /**
     * The JS plugin version. Will be included as the "version" field for the manifest entry in
     * "js-plugins/manifest.json".
     *
     * @return the version
     */
    public abstract String version();

    /**
     * The main JS file path, specified relative to {@link #rootPath()}. The main JS file must exist
     * ({@code Files.isRegularFile(root().resolve(main()))}) and must be included in {@link #paths()}. Will be included
     * as the "main" field for the manifest entry in "js-plugins/manifest.json".
     *
     *
     * @return the main JS file path
     */
    public abstract Path main();

    /**
     * The root directory path of the resources to serve. The root must exist ({@code Files.isDirectory(root())}).
     *
     * @return the root
     */
    public abstract Path rootPath();

    /**
     * The paths to serve, specified relative to {@link #rootPath()}. The resources will be served via the URL path
     * "js-plugins/{name}/{pathRelativeToRoot}". By default, is {@link Paths#all()}.
     *
     * @return the paths
     */
    @Default
    public Paths paths() {
        return Paths.all();
    }

    @Override
    public final <T, V extends Plugin.Visitor<T>> T walk(V visitor) {
        return visitor.visit(this);
    }

    @Check
    final void checkRootPath() {
        if (!Files.isDirectory(rootPath())) {
            throw new IllegalArgumentException(String.format("rootPath ('%s') must exist and be a directory", rootPath()));
        }
    }

    @Check
    final void checkMain() {
        final Path mainPath = rootPath().resolve(main());
        if (!Files.isRegularFile(mainPath)) {
            throw new IllegalArgumentException(String.format("main ('%s') must exist and be a regular file", mainPath));
        }
    }

    @Check
    final void checkPaths() {
        if (!(paths() instanceof PathsInternal)) {
            throw new IllegalArgumentException("Must construct one of the approved Paths");
        }
        final Path relativeMain = rootPath().relativize(rootPath().resolve(main()));
        if (!((PathsInternal) paths()).matches(relativeMain)) {
            throw new IllegalArgumentException(String.format("main ('%s') is not in paths", relativeMain));
        }
    }

    public interface Builder {
        Builder name(String name);

        Builder version(String version);

        Builder main(Path main);

        Builder rootPath(Path rootPath);

        Builder paths(Paths paths);

        JsPlugin build();
    }
}
