/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.function.Predicate;

class CopyHelper {
    static void copyRecursive(Path src, Path dst, Predicate<Path> includeDir, Predicate<Path> includeFile)
            throws IOException {
        Files.createDirectories(dst.getParent());
        Files.walkFileTree(src, new CopyRecursiveVisitor(src, dst, includeDir, includeFile));
    }

    private static class CopyRecursiveVisitor extends SimpleFileVisitor<Path> {
        private final Path src;
        private final Path dst;
        private final Predicate<Path> includeDir;
        private final Predicate<Path> includeFile;

        public CopyRecursiveVisitor(Path src, Path dst, Predicate<Path> includeDir, Predicate<Path> includeFile) {
            this.src = Objects.requireNonNull(src);
            this.dst = Objects.requireNonNull(dst);
            this.includeDir = Objects.requireNonNull(includeDir);
            this.includeFile = Objects.requireNonNull(includeFile);
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            if (!includeDir.test(dir)) {
                return FileVisitResult.SKIP_SUBTREE;
            }
            // Note: toString() necessary for src/dst that don't share the same root FS
            Files.copy(dir, dst.resolve(src.relativize(dir).toString()), StandardCopyOption.COPY_ATTRIBUTES);
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            if (!includeFile.test(file)) {
                return FileVisitResult.CONTINUE;
            }
            // Note: toString() necessary for src/dst that don't share the same root FS
            Files.copy(file, dst.resolve(src.relativize(file).toString()), StandardCopyOption.COPY_ATTRIBUTES);
            return FileVisitResult.CONTINUE;
        }
    }
}
