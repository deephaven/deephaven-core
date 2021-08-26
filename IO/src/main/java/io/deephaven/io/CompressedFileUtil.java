/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io;

import io.deephaven.io.streams.MultiFileInputStream;
import io.deephaven.io.streams.SevenZipInputStream;
import io.deephaven.base.verify.Require;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarConstants;
import org.apache.commons.compress.archivers.tar.TarUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.*;

public class CompressedFileUtil {

    public static final long UINT_TO_LONG = 0xFFFFFFFFL;
    public static final int BZIP2_MAGIC = ((int) 'B') | (((int) 'Z') << 8) | (((int) 'h') << 16);
    public static final String[] COMPRESSION_EXTENSIONS =
            new String[] {".gz", ".tgz", ".tar.gz", ".zip", ".bz2", ".7z"};
    private static final int TAR_CHECKSUM_OFFSET = TarConstants.NAMELEN + TarConstants.MODELEN + TarConstants.UIDLEN
            + TarConstants.GIDLEN + TarConstants.SIZELEN + TarConstants.MODTIMELEN;

    /**
     * Unzip the file
     * 
     * @param file The file to bunzip2
     */
    public static int bunzip2(File file) throws IOException {
        List<String> list = new ArrayList<String>();
        list.add("bzip2");
        list.add("-d");
        list.add(file.getPath());
        try {
            ProcessExecutor executor = exec(list, ".", "out.log", "err.log");
            executor.waitFor();
            return executor.exitValue();
        } catch (InterruptedException ignored) {
        }
        return -1;
    }

    /**
     * Files need to be in the same directory, otherwise Tar complains about windows paths
     *
     * @param archiveName
     * @param baseDir the directory where the input files are located
     * @param inputFiles
     * @throws IOException
     */
    public static void bzip2(String archiveName, String baseDir, String[] inputFiles) throws IOException {
        if (inputFiles.length == 0) {
            throw new IllegalArgumentException("Need at least one input file.");
        }

        ArrayList<String> tarCmdList = new ArrayList<String>();
        tarCmdList.add("tar");
        tarCmdList.add("cvjf");
        tarCmdList.add(archiveName);

        for (int i = 0; i < inputFiles.length; i++) {
            File input = new File(baseDir + "/" + inputFiles[i]);
            if (!input.exists()) {
                throw new IOException("Input file " + input.getAbsolutePath() + " doesn't exist.");
            }
            tarCmdList.add(inputFiles[i]);
        }

        // System.out.println(tarCmdList);
        ProcessExecutor p = exec(tarCmdList, baseDir, "tar.out", "tar.err");
        try {
            p.waitFor();
        } catch (InterruptedException ignored) {
        }
        if (p.exitValue() != 0) {
            throw new RuntimeException("TAR failed: " + tarCmdList);
        }
    }

    public static void unbzip2(File archive) throws IOException {
        if (!archive.exists()) {
            throw new IllegalArgumentException("File " + archive.getAbsolutePath() + " doesn't exist.");
        }
        String workDir = archive.getParent();
        ArrayList<String> tarCmdList = new ArrayList<String>();
        tarCmdList.add("tar");
        tarCmdList.add("xvjof");
        tarCmdList.add(archive.getName());
        // System.out.println(tarCmd);
        ProcessExecutor p = exec(tarCmdList, workDir, "tar.out", "tar.err");
        try {
            p.waitFor();
        } catch (InterruptedException ignored) {
        }
        if (p.exitValue() != 0) {
            throw new RuntimeException("Tar decompression ailed: " + tarCmdList);
        }

        (new File(archive.getName())).delete();
    }

    public static void zipFile(File inputFile, File zipFilePath) throws IOException {
        // Wrap a FileOutputStream around a ZipOutputStream
        // to store the zip stream to a file. Note that this is
        // not absolutely necessary
        FileOutputStream fileOutputStream = new FileOutputStream(zipFilePath);
        ZipOutputStream zipOutputStream = new ZipOutputStream(fileOutputStream);

        // a ZipEntry represents a file entry in the zip archive
        // We name the ZipEntry after the original file's name
        ZipEntry zipEntry = new ZipEntry(inputFile.getName());
        zipOutputStream.putNextEntry(zipEntry);

        FileInputStream fileInputStream = new FileInputStream(inputFile);
        byte[] buf = new byte[1024];
        int bytesRead;

        // Read the input file by chucks of 1024 bytes
        // and write the read bytes to the zip stream
        while ((bytesRead = fileInputStream.read(buf)) > 0) {
            zipOutputStream.write(buf, 0, bytesRead);
        }

        // close ZipEntry to store the stream to the file
        zipOutputStream.closeEntry();

        zipOutputStream.close();
        fileOutputStream.close();
    }

    /**
     * Execute another system process
     *
     * @param commandList array, see ProcessBuilder
     * @param stdoutFile redirect output if not null
     * @param stderrFile redirect output if not null
     */
    private static ProcessExecutor exec(List commandList, String workDir, String stdoutFile, String stderrFile) {
        return new ProcessExecutor(commandList, workDir, stdoutFile, stderrFile);
    }

    public static InputStream openPossiblyCompressedFiles(String[] fileNames) throws IOException {
        List<MultiFileInputStream.DecoratedInputStream> inputStreams = new ArrayList<>();
        for (String currentFileName : fileNames) {
            inputStreams.add(new MultiFileInputStream.DecoratedInputStream(currentFileName,
                    openPossiblyCompressedFile(currentFileName)));
        }
        return new MultiFileInputStream(
                inputStreams.toArray(new MultiFileInputStream.DecoratedInputStream[inputStreams.size()]));
    }

    /**
     * If the given file doesn't exist, looks to see if a compressed version of the file exists. If no file is found,
     * returns the original file name.
     */
    public static String addCompressionExtensionIfNeeded(String sFileName) {
        File file = new File(sFileName);
        if (!file.exists()) {
            for (String sExtension : COMPRESSION_EXTENSIONS) {
                File altFile = new File(file.toString() + sExtension);
                if (altFile.exists()) {
                    file = altFile;
                    break;
                }
            }
        }
        return file.toString();
    }

    /**
     * Open the file, automatically determining if it has been zipped, GZipped, BZip2'd or not. The returned input
     * stream will be buffered. See also {@link #addCompressionExtensionIfNeeded}.
     */
    public static InputStream openPossiblyCompressedFile(String sFileName, Boolean useMicrosPcapEmulation)
            throws IOException {
        if (useMicrosPcapEmulation == null) {
            return openPossiblyCompressedFile(new FileInputStreamFactory(sFileName));
        }

        InputStream inputStream = openPossiblyCompressedFile(sFileName, null);
        final boolean markSupported = inputStream.markSupported();
        if (markSupported)
            inputStream.mark(4);

        final int magic4 = getMagic4(inputStream, getMagic2(inputStream));

        if (markSupported) {
            inputStream.reset();
        } else {
            inputStream.close();
            inputStream = openPossiblyCompressedFile(sFileName, null);
        }

        return inputStream;
    }

    public static InputStream openPossiblyCompressedFile(String sFileName) throws IOException {
        return openPossiblyCompressedFile(sFileName, true);
    }

    public static InputStream openPossiblyCompressedFile(String sFileName, int bufferedSize,
            Boolean useMicrosPcapEmulation) throws IOException {
        if (useMicrosPcapEmulation == null) {
            return openPossiblyCompressedFile(new FileInputStreamFactory(sFileName, bufferedSize));
        }

        InputStream inputStream = openPossiblyCompressedFile(sFileName, bufferedSize, null);
        final boolean markSupported = inputStream.markSupported();
        if (markSupported)
            inputStream.mark(4);

        final int magic4 = getMagic4(inputStream, getMagic2(inputStream));

        if (markSupported) {
            inputStream.reset();
        } else {
            inputStream.close();
            inputStream = openPossiblyCompressedFile(sFileName, bufferedSize, null);
        }

        return inputStream;
    }

    public static InputStream openPossiblyCompressedFile(String sFileName, int bufferedSize) throws IOException {
        return openPossiblyCompressedFile(sFileName, bufferedSize, true);
    }

    /**
     * Open the file, automatically determining if it has been zipped, GZipped, BZip2'd or not.
     */
    public static InputStream openPossiblyCompressedFile(InputStreamFactory inputStreamFactory) throws IOException {
        InputStreamFactory decompressedInputStreamFactory =
                createInputStreamFactoryForPossiblyCompressedStream(inputStreamFactory);
        decompressedInputStreamFactory =
                createInputStreamFactoryForPossiblyTarredStream(decompressedInputStreamFactory);
        InputStream inputStream = decompressedInputStreamFactory.createInputStream();
        return inputStream;
    }

    // ----------------------------------------------------------------
    public static InputStreamFactory createInputStreamFactoryForPossiblyCompressedStream(
            final InputStreamFactory inputStreamFactory) throws IOException {
        // Read in the header
        final InputStream testStream = inputStreamFactory.createInputStream();
        int nMagic2 = getMagic2(testStream);
        int nMagic4 = getMagic4(testStream, nMagic2);
        long nMagic6 = getMagic6(testStream, nMagic4);
        testStream.close();

        InputStreamFactory decompressedInputStreamFactory;
        if (BZIP2_MAGIC == (nMagic4 & 0xFFFFFF)) {
            // open as bzip2'd text
            decompressedInputStreamFactory = new InputStreamFactory() {
                public InputStream createInputStream() throws IOException {
                    return new BZip2CompressorInputStream(inputStreamFactory.createInputStream());
                }

                public String getDescription() {
                    return "bzip2'd " + inputStreamFactory.getDescription();
                }
            };

        } else if (GZIPInputStream.GZIP_MAGIC == nMagic2) {
            // open as gzipped text
            decompressedInputStreamFactory = new InputStreamFactory() {
                public InputStream createInputStream() throws IOException {
                    return new GZIPInputStream(inputStreamFactory.createInputStream());
                }

                public String getDescription() {
                    return "gzipped " + inputStreamFactory.getDescription();
                }
            };

        } else if (ZipInputStream.LOCSIG == nMagic4) {
            // open as zipped text
            decompressedInputStreamFactory = new InputStreamFactory() {
                private String m_sSubFileName;

                public InputStream createInputStream() throws IOException {
                    ZipInputStream zipInputStream = new ZipInputStream(inputStreamFactory.createInputStream());
                    ZipEntry zipEntry = zipInputStream.getNextEntry();
                    if (null == zipEntry) {
                        throw new FileNotFoundException(
                                "No zip entries in " + inputStreamFactory.getDescription() + ".");
                    }
                    m_sSubFileName = zipEntry.getName();
                    return zipInputStream;
                }

                public String getDescription() {
                    return "zipped " + inputStreamFactory.getDescription()
                            + (null == m_sSubFileName ? "" : " (sub-file \"" + m_sSubFileName + "\")");
                }
            };

        } else if (SevenZipInputStream.SIGNATURE_AS_LONG == nMagic6) {
            // open as 7zipped text
            decompressedInputStreamFactory = new InputStreamFactory() {
                private String m_sSubFileName;

                public InputStream createInputStream() throws IOException {
                    SevenZipInputStream zipInputStream = new SevenZipInputStream(inputStreamFactory);
                    SevenZipInputStream.Entry entry =
                            zipInputStream.getNextEntry(SevenZipInputStream.Behavior.SKIP_WHEN_NO_STREAM);
                    if (null == entry) {
                        throw new FileNotFoundException(
                                "No zip entries in " + inputStreamFactory.getDescription() + ".");
                    }
                    m_sSubFileName = entry.getName();
                    return zipInputStream;
                }

                public String getDescription() {
                    return "7zipped " + inputStreamFactory.getDescription()
                            + (null == m_sSubFileName ? "" : " (sub-file \"" + m_sSubFileName + "\")");
                }
            };
        } else {
            // open as plaintext
            decompressedInputStreamFactory = new InputStreamFactory() {
                public InputStream createInputStream() throws IOException {
                    return inputStreamFactory.createInputStream();
                }

                public String getDescription() {
                    return "plaintext " + inputStreamFactory.getDescription();
                }
            };
        }
        return decompressedInputStreamFactory;
    }

    public static long getMagic6(InputStream testStream, int nMagic4) throws IOException {
        return ((testStream.read() & UINT_TO_LONG) << 32) | ((testStream.read() & UINT_TO_LONG) << 40)
                | (nMagic4 & UINT_TO_LONG);
    }

    public static int getMagic4(InputStream testStream, int nMagic2) throws IOException {
        return (testStream.read() << 16) | (testStream.read() << 24) | nMagic2;
    }

    public static int getMagic2(InputStream testStream) throws IOException {
        return testStream.read() | (testStream.read() << 8);
    }

    // ----------------------------------------------------------------
    public static InputStreamFactory createInputStreamFactoryForPossiblyTarredStream(
            final InputStreamFactory inputStreamFactory) throws IOException {
        // Read in the header
        byte[] header = new byte[TarConstants.DEFAULT_RCDSIZE];
        InputStream testStream = inputStreamFactory.createInputStream();
        try {
            for (int nOffset = 0; nOffset < header.length;) {
                int nBytesRead = testStream.read(header, nOffset, header.length - nOffset);
                if (-1 == nBytesRead) {
                    return inputStreamFactory;
                }
                nOffset += nBytesRead;
            }
        } finally {
            testStream.close();
        }

        // see if the checksum is correct
        long nExpectedChecksum;
        try {
            nExpectedChecksum = TarUtils.parseOctal(header, TAR_CHECKSUM_OFFSET, TarConstants.CHKSUMLEN);
        } catch (final IllegalArgumentException ignored) {
            return inputStreamFactory;
        }
        for (int nOffset = 0; nOffset < TarConstants.CHKSUMLEN; nOffset++) {
            header[TAR_CHECKSUM_OFFSET + nOffset] = (byte) ' ';
        }
        long nActualChecksum = TarUtils.computeCheckSum(header);
        if (nExpectedChecksum != nActualChecksum) {
            return inputStreamFactory;
        }

        // looks like tar!
        return new InputStreamFactory() {
            private String m_sSubFileName;

            public InputStream createInputStream() throws IOException {
                TarArchiveInputStream tarInputStream =
                        new TarArchiveInputStream(inputStreamFactory.createInputStream());
                TarArchiveEntry tarEntry = tarInputStream.getNextTarEntry();
                if (null == tarEntry) {
                    throw new FileNotFoundException("No tar entries in " + inputStreamFactory.getDescription() + ".");
                }
                m_sSubFileName = tarEntry.getName();
                return tarInputStream;
            }

            public String getDescription() {
                return "tarred " + inputStreamFactory.getDescription()
                        + (null == m_sSubFileName ? "" : " (sub-file \"" + m_sSubFileName + "\")");
            }
        };

    }

    private static class ProcessExecutor {
        ProcessBuilder builder;
        Process p = null;
        FilePipe outPipe, errPipe;

        ProcessExecutor(List cmdList, String workDir, String stdoutFile, String stderrFile) {
            builder = new ProcessBuilder(cmdList);
            if (workDir != null) {
                builder.directory(new File(workDir));
            }

            try {
                p = builder.start();
                outPipe = new FilePipe(stdoutFile, p.getInputStream());
                errPipe = new FilePipe(stderrFile, p.getErrorStream());
                outPipe.start();
                errPipe.start();
            } catch (Exception e) {
                throw new RuntimeException("Error while executing native command: " + cmdList.get(0), e);
            }
        }

        public void waitFor() throws InterruptedException, IOException {
            try {
                p.waitFor();
            } finally {
                outPipe.close();
                errPipe.close();
            }
        }

        public int exitValue() {
            return p.exitValue();
        }
    }

    public static class FilePipe extends Thread {
        BufferedInputStream reader;
        FileWriter writer;
        boolean done = false;

        public FilePipe(String fileName, InputStream inStream) throws IOException {
            super("WFileUtil.FilePipe->" + fileName);

            reader = new BufferedInputStream(inStream);
            if (fileName != null) {
                writer = new FileWriter(fileName);
            }
            // setDaemon(true);
        }

        public void run() {
            while (!done) {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException("Error while writing to pipe.", e);
                }
                try {
                    // we need a very quick sleep, otherwise the executed process is being slowed down by java
                    Thread.sleep(50);
                } catch (InterruptedException x) {
                    return;
                }
            }
        }

        public synchronized void flush() throws IOException {
            if (!done) {
                int avail = reader.available();
                if (avail > 0) {
                    byte[] buffer = new byte[avail];
                    int length = reader.read(buffer);
                    if (length == -1) {
                        done = true;
                    } else if (writer != null) {
                        writer.write(new String(buffer));
                    }
                }
            }
        }

        public synchronized void close() throws IOException {
            if (!done) {
                flush();
                done = true;
                reader.close();
                if (writer != null) {
                    writer.close();
                }
            }
        }
    }

    // ----------------------------------------------------------------
    public static class FileInputStreamFactory implements InputStreamFactory {

        private final String m_sFileName;
        private final int m_bufferedSize;

        public FileInputStreamFactory(String sFileName) {
            this(sFileName, 8192); // BufferedInputStream.defaultBufferSize
        }

        public FileInputStreamFactory(String sFileName, int bufferedSize) {
            Require.nonempty(sFileName, "sFileName");
            m_sFileName = sFileName;
            m_bufferedSize = bufferedSize;
        }

        public InputStream createInputStream() throws IOException {
            return new BufferedInputStream(new FileInputStream(m_sFileName), m_bufferedSize);
        }

        public String getDescription() {
            return "file \"" + m_sFileName + "\"";
        }
    }
}
