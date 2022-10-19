/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.plugin.type.JsPlugin;
import io.deephaven.plugin.type.JsPluginBase;
import io.deephaven.plugin.type.JsPluginInfo;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

/**
 * A {@link JsPlugin} implementation sourced from a distribution directory.
 */
public final class JsPluginDistribution extends JsPluginBase {

    private static final String PACKAGE_JSON = "package.json";

    /**
     * Creates a new js plugin distribution.
     *
     * <p>
     * Note: unlike {@link #fromPackageJsonDistribution(Path)}, {@code distributionDir} does not need to contain
     * {@value PACKAGE_JSON}.
     *
     * @param distributionDir the distribution directory
     * @param info the plugin info
     */
    public static JsPluginDistribution of(Path distributionDir, JsPluginInfo info) {
        return new JsPluginDistribution(distributionDir, info);
    }

    /**
     * Creates a new js plugin distribution. Assumes that {@value PACKAGE_JSON} exists in {@code distributionDir}. The
     * {@value JsPluginInfo#NAME}, {@value JsPluginInfo#VERSION}, and {@value JsPluginInfo#MAIN} from
     * {@value PACKAGE_JSON} will be used.
     *
     * @param distributionDir the distribution directory
     * @return the js plugin distribution
     * @throws IOException if an I/O exception occurs
     * @see <a href="https://github.com/deephaven/js-plugin-template">js-plugin-template</a>
     */
    public static JsPluginDistribution fromPackageJsonDistribution(Path distributionDir) throws IOException {
        final Path packageJson = distributionDir.resolve(PACKAGE_JSON);
        try (final InputStream in = new BufferedInputStream(Files.newInputStream(packageJson))) {
            final JsPluginInfo pluginInfo = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readValue(in, JsPluginInfo.class);
            // Note: we could provide an option, or do it by default, to only copy over main, or main.parent, if we
            // wanted to minimize the amount of data we re-expose.
            return new JsPluginDistribution(distributionDir, pluginInfo);
        }
    }

    private final Path distributionDir;
    private final JsPluginInfo info;

    private JsPluginDistribution(Path distributionDir, JsPluginInfo info) {
        this.distributionDir = Objects.requireNonNull(distributionDir);
        this.info = Objects.requireNonNull(info);
    }

    @Override
    public JsPluginInfo info() {
        return info;
    }

    @Override
    public void copyTo(Path destination) throws IOException {
        copyRecursive(distributionDir, destination);
    }

    private static void copyRecursive(Path src, Path dst) throws IOException {
        Files.createDirectories(dst.getParent());
        Files.walkFileTree(src, new CopyRecursiveVisitor(src, dst));
    }

    private static class CopyRecursiveVisitor extends SimpleFileVisitor<Path> {
        private final Path src;
        private final Path dst;

        public CopyRecursiveVisitor(Path src, Path dst) {
            this.src = Objects.requireNonNull(src);
            this.dst = Objects.requireNonNull(dst);
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            // Note: toString() necessary for src/dst that don't share the same root FS
            Files.copy(dir, dst.resolve(src.relativize(dir).toString()), StandardCopyOption.COPY_ATTRIBUTES);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            // Note: toString() necessary for src/dst that don't share the same root FS
            Files.copy(file, dst.resolve(src.relativize(file).toString()), StandardCopyOption.COPY_ATTRIBUTES);
            return FileVisitResult.CONTINUE;
        }
    }
}
