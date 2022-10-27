/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plugin.type;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.PluginBase;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

public abstract class ContentPluginBase extends PluginBase implements ContentPlugin {

    @Override
    public final <T, V extends Plugin.Visitor<T>> T walk(V visitor) {
        return visitor.visit(this);
    }

    protected static void copyRecursive(Path src, Path dst) throws IOException {
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
