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
 * A JS plugin is a {@link Plugin} that allows for custom javascript and related content to be served, see
 * {@link io.deephaven.plugin.js}.
 *
 * <p>
 * For example, if the following JS plugin was the only JS plugin installed
 *
 * <pre>
 * JsPlugin.builder()
 *         .name("foo")
 *         .version("1.0.0")
 *         .main(Path.of("dist/index.js"))
 *         .path(Path.of("/path-to/my-plugin"))
 *         .build()
 * </pre>
 *
 * the manifest served at "js-plugins/manifest.json" would be equivalent to
 *
 * <pre>
 * {
 *   "plugins": [
 *     {
 *       "name": "foo",
 *       "version": "1.0.0",
 *       "main": "dist/index.js"
 *     }
 *   ]
 * }
 * </pre>
 *
 * and the file "/path-to/my-plugin/dist/index.js" would be served at "js-plugins/foo/dist/index.js". All other files of
 * the form "/path-to/my-plugin/{somePath}" will be served at "js-plugins/foo/{somePath}".
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
     * The main JS file path, specified relative to {@link #path()}. The main JS file must exist
     * ({@code Files.isRegularFile(root().resolve(main()))}) and must be included in {@link #paths()}. Will be included
     * as the "main" field for the manifest entry in "js-plugins/manifest.json".
     *
     * @return the main JS file path
     */
    public abstract Path main();

    /**
     * The directory path of the resources to serve. The resources will be served via the URL path
     * "js-plugins/{name}/{relativeToPath}". The path must exist ({@code Files.isDirectory(path())}).
     *
     * @return the path
     */
    public abstract Path path();

    /**
     * The subset of resources from {@link #path()} to serve. Production installations should preferably be packaged
     * with the exact resources necessary (and thus served with {@link Paths#all()}). During development, other subsets
     * may be useful if {@link #path()} contains content unrelated to the JS content. By default, is
     * {@link Paths#all()}.
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
    final void checkPath() {
        if (!Files.isDirectory(path())) {
            throw new IllegalArgumentException(String.format("path ('%s') must exist and be a directory", path()));
        }
    }

    @Check
    final void checkMain() {
        final Path mainPath = path().resolve(main());
        if (!Files.isRegularFile(mainPath)) {
            throw new IllegalArgumentException(String.format("main ('%s') must exist and be a regular file", mainPath));
        }
    }

    @Check
    final void checkPaths() {
        if (!(paths() instanceof PathsInternal)) {
            throw new IllegalArgumentException("Must construct one of the approved Paths");
        }
        final Path relativeMain = path().relativize(path().resolve(main()));
        if (!((PathsInternal) paths()).matches(relativeMain)) {
            throw new IllegalArgumentException(String.format("main ('%s') is not in paths", relativeMain));
        }
    }

    public interface Builder {
        Builder name(String name);

        Builder version(String version);

        Builder main(Path main);

        Builder path(Path path);

        Builder paths(Paths paths);

        JsPlugin build();
    }
}
