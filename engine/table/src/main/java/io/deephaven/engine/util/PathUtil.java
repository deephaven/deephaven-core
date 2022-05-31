package io.deephaven.engine.util;

import io.deephaven.io.InputStreamFactory;
import io.deephaven.io.streams.SevenZipInputStream;
import io.deephaven.io.streams.SevenZipInputStream.Behavior;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipInputStream;

public class PathUtil {
    /**
     * Opens a file, returning an input stream. Paths that end in ".tar.zip", ".tar.bz2", ".tar.gz", ".tar.7z",
     * ".tar.zst", ".zip", ".bz2", ".gz", ".7z", ".zst", or ".tar" will have appropriate decompression applied. The
     * returned stream may or may not be buffered.
     *
     * @param path the path
     * @return the input stream, potentially decompressed
     * @throws IOException if an I/O exception occurs
     * @see Files#newInputStream(Path, OpenOption...)
     */
    public static InputStream open(Path path) throws IOException {
        final String fileName = path.getFileName().toString();
        if (fileName.endsWith(".zip")) {
            final ZipInputStream in = new ZipInputStream(Files.newInputStream(path));
            in.getNextEntry();
            return fileName.endsWith(".tar.zip") ? untar(in) : in;
        }
        if (fileName.endsWith(".bz2")) {
            final BZip2CompressorInputStream in = new BZip2CompressorInputStream(Files.newInputStream(path));
            return fileName.endsWith(".tar.bz2") ? untar(in) : in;
        }
        if (fileName.endsWith(".gz")) {
            final GZIPInputStream in = new GZIPInputStream(Files.newInputStream(path));
            return fileName.endsWith(".tar.gz") ? untar(in) : in;
        }
        if (fileName.endsWith(".7z")) {
            final SevenZipInputStream in = new SevenZipInputStream(new InputStreamFactory() {
                @Override
                public InputStream createInputStream() throws IOException {
                    return Files.newInputStream(path);
                }

                @Override
                public String getDescription() {
                    return path.toString();
                }
            });
            in.getNextEntry(Behavior.SKIP_WHEN_NO_STREAM);
            return fileName.endsWith(".tar.7z") ? untar(in) : in;
        }
        if (fileName.endsWith(".zst")) {
            final ZstdCompressorInputStream in = new ZstdCompressorInputStream(Files.newInputStream(path));
            return fileName.endsWith(".tar.zst") ? untar(in) : in;
        }
        if (fileName.endsWith(".tar")) {
            return untar(Files.newInputStream(path));
        }
        return Files.newInputStream(path);
    }

    private static TarArchiveInputStream untar(InputStream in) throws IOException {
        final TarArchiveInputStream tarInputStream = new TarArchiveInputStream(in);
        tarInputStream.getNextEntry();
        return tarInputStream;
    }
}
