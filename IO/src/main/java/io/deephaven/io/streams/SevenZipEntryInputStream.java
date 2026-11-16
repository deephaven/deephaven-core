//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.streams;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 * An {@link InputStream} over the first entry with data in a 7z archive. Closing the stream closes the archive.
 */
public final class SevenZipEntryInputStream extends InputStream {
    /** The first six bytes of every 7z archive, little-endian, as {@code CompressedFileUtil.getMagic6} reads them. */
    public static final long SIGNATURE_AS_LONG = 0x1C27AFBC7A37L;

    private final SevenZFile archive;
    private final String entryName;

    private SevenZipEntryInputStream(final SevenZFile archive, final String entryName) {
        this.archive = archive;
        this.entryName = entryName;
    }

    /**
     * Positions {@code archive} at its first entry that has data, skipping directories and empty files, and returns a
     * stream over that entry. The stream owns {@code archive}, which is also closed if this method throws.
     *
     * @param archive the archive
     * @param description describes the archive in exception messages
     * @return a stream over the first entry with data
     * @throws FileNotFoundException if no entry has data
     * @throws IOException if the archive cannot be read
     */
    public static SevenZipEntryInputStream openFirstEntry(final SevenZFile archive, final String description)
            throws IOException {
        try {
            SevenZArchiveEntry entry;
            do {
                entry = archive.getNextEntry();
            } while (entry != null && !entry.hasStream());
            if (entry == null) {
                throw new FileNotFoundException("No 7z entries with data in " + description + ".");
            }
            return new SevenZipEntryInputStream(archive, entry.getName());
        } catch (IOException | RuntimeException e) {
            archive.close();
            throw e;
        }
    }

    /**
     * @return the name of the entry this stream reads
     */
    public String getEntryName() {
        return entryName;
    }

    @Override
    public int read() throws IOException {
        return archive.read();
    }

    @Override
    public int read(final byte[] bytes, final int offset, final int length) throws IOException {
        return archive.read(bytes, offset, length);
    }

    @Override
    public void close() throws IOException {
        archive.close();
    }
}
