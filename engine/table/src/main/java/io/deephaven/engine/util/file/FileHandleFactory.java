/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util.file;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;

/**
 * Factory interface for producing {@link FileHandle}s.
 */
public interface FileHandleFactory {

    /**
     * Create a new {@link FileHandle} with the specified set of {@link OpenOption}s.
     *
     * @param file The {@link File} to open
     * @param openOptions The {@link OpenOption}s to use
     * @return The new file handle
     */
    @NotNull
    FileHandle makeHandle(@NotNull final File file, @NotNull final OpenOption... openOptions) throws IOException;

    @FunctionalInterface
    interface FileToHandleFunction {

        @NotNull
        FileHandle invoke(@NotNull final File file) throws IOException;
    }

    static FileToHandleFunction toReadOnlyHandleCreator(@NotNull final FileHandleFactory fileHandleFactory) {
        return (final File file) -> fileHandleFactory.makeHandle(file, OpenOptionsHelper.READ_ONLY_OPEN_OPTIONS);
    }

    static FileToHandleFunction toReadWriteCreateHandleCreator(@NotNull final FileHandleFactory fileHandleFactory) {
        return (final File file) -> fileHandleFactory.makeHandle(file,
                OpenOptionsHelper.READ_WRITE_CREATE_OPEN_OPTIONS);
    }

    static FileToHandleFunction toWriteAppendCreateHandleCreator(@NotNull final FileHandleFactory fileHandleFactory) {
        return (final File file) -> fileHandleFactory.makeHandle(file,
                OpenOptionsHelper.WRITE_APPEND_CREATE_OPEN_OPTIONS);
    }

    static FileToHandleFunction toWriteTruncateCreateHandleCreator(@NotNull final FileHandleFactory fileHandleFactory) {
        return (final File file) -> fileHandleFactory.makeHandle(file,
                OpenOptionsHelper.WRITE_TRUNCATE_CREATE_OPEN_OPTIONS);
    }

    final class OpenOptionsHelper {

        /**
         * Open the file for reading only. Fail if the file doesn't already exist.
         */
        private static final OpenOption[] READ_ONLY_OPEN_OPTIONS = new OpenOption[] {StandardOpenOption.READ};

        /**
         * Open the file for reading or writing. Create the file iff it doesn't already exist.
         */
        private static final OpenOption[] READ_WRITE_CREATE_OPEN_OPTIONS =
                new OpenOption[] {StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE};

        /**
         * Open the file for writing. If it already exists, append to it, else create it.
         */
        private static final OpenOption[] WRITE_APPEND_CREATE_OPEN_OPTIONS =
                new OpenOption[] {StandardOpenOption.WRITE, StandardOpenOption.APPEND, StandardOpenOption.CREATE};

        /**
         * Open the file for writing. If it already exists truncate it, else create it.
         */
        private static final OpenOption[] WRITE_TRUNCATE_CREATE_OPEN_OPTIONS = new OpenOption[] {
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.CREATE};
    }
}
