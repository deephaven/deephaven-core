//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

class CopyHelper {
    static void copyRecursive(Path src, Path dst, PathMatcher pathMatcher) throws IOException {
        copyRecursive(src, dst, pathMatcher, d -> true);
    }

    static void copyRecursive(Path src, Path dst, PathMatcher pathMatcher, PathMatcher dirMatcher) throws IOException {
        Files.createDirectories(dst.getParent());
        Files.walkFileTree(src, new CopyRecursiveVisitor(src, dst, pathMatcher, dirMatcher));
    }

    private static class CopyRecursiveVisitor extends SimpleFileVisitor<Path> {
        private final Path src;
        private final Path dst;
        private final PathMatcher pathMatcher;
        private final PathMatcher dirMatcher;

        public CopyRecursiveVisitor(Path src, Path dst, PathMatcher pathMatcher, PathMatcher dirMatcher) {
            this.src = Objects.requireNonNull(src);
            this.dst = Objects.requireNonNull(dst);
            this.pathMatcher = Objects.requireNonNull(pathMatcher);
            this.dirMatcher = Objects.requireNonNull(dirMatcher);
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            final Path relativeDir = src.relativize(dir);
            if (dirMatcher.matches(relativeDir) || pathMatcher.matches(relativeDir)) {
                // Note: toString() necessary for src/dst that don't share the same root FS
                Files.copy(dir, dst.resolve(relativeDir.toString()), StandardCopyOption.COPY_ATTRIBUTES);
                return FileVisitResult.CONTINUE;
            }
            return FileVisitResult.SKIP_SUBTREE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            final Path relativeFile = src.relativize(file);
            if (pathMatcher.matches(relativeFile)) {
                // Note: toString() necessary for src/dst that don't share the same root FS
                Files.copy(file, dst.resolve(relativeFile.toString()), StandardCopyOption.COPY_ATTRIBUTES);
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            if (exc != null) {
                throw exc;
            }
            final Path relativeDir = src.relativize(dir);
            if (!pathMatcher.matches(relativeDir)) {
                // If the specific dir does not match as a path (even if it _did_ match as a directory), we
                // "optimistically" try and delete it; if the directory is not empty (b/c some subpath matched and was
                // copied), the delete will fail. (We could have an alternative impl that keeps track w/ a stack if any
                // subpaths matched.)
                try {
                    Files.delete(dir);
                } catch (DirectoryNotEmptyException e) {
                    // ignore
                }
            }
            return FileVisitResult.CONTINUE;
        }
    }
}
