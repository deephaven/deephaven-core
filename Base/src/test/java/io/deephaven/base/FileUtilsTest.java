//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.DosFileAttributeView;
import java.util.UUID;

import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

public class FileUtilsTest extends TestCase {

    /**
     * Create a randomized directory-structure and delete it recursively
     *
     * @throws IOException
     */
    public void testRecursiveDelete() throws IOException {
        final Path tmpRoot = Files.createTempDirectory("testRecursiveDelete");
        makeRandomDirectoryStructure(tmpRoot);

        FileUtils.deleteRecursively(tmpRoot.toFile());
        Assert.assertFalse(tmpRoot.toFile().exists());
    }

    /**
     * Create a randomized directory-structure and delete it recursively in a way safe for NFS. Before deletion, each
     * File is moved to a temporary-file, which is then deleted. If on NFS, the files may be left, but as temporary
     * ".nfs..." files, which will be cleaned up by the NFS server when all handles to them are closed
     *
     * @throws IOException
     */
    public void testRecursiveDeleteNFS() throws IOException {
        final Path tmpRoot = Files.createTempDirectory("testRecursiveDeleteNFS");
        makeRandomDirectoryStructure(tmpRoot);

        FileUtils.deleteRecursivelyOnNFS(tmpRoot.toFile());
        Assert.assertFalse(tmpRoot.toFile().exists());
    }

    private static void makeRandomDirectoryStructure(@NotNull final Path rootPath) throws IOException {
        final Path tmpFile = Files.createTempFile(rootPath, "tmp", ".tmp");
        final Path tmpDir = Files.createTempDirectory(rootPath, "dir");
        Files.createTempFile(tmpDir, "tmp", ".tmp");

        final String fileName = String.format("%s.lnk", UUID.randomUUID());
        final Path slinkPath = Paths.get(tmpDir.toString(), fileName);

        try {
            Files.createSymbolicLink(slinkPath, tmpFile);
        } catch (final UnsupportedOperationException uoe) {
            // no problem; we can't try deleting a soft-link because the concept does not exist on this platform
        }

        final Path hiddenFile = Files.createTempFile(tmpDir, ".hid", ".tmp");

        // the following possibly does nothing (like if we're on *nix system)
        final DosFileAttributeView dosView =
                Files.getFileAttributeView(hiddenFile, DosFileAttributeView.class, LinkOption.NOFOLLOW_LINKS);
        dosView.setHidden(true);
    }

    public void testConvertToFileURI() throws IOException {
        final File currentDir = new File("").getAbsoluteFile();
        fileUriTestHelper(currentDir.toString(), true, currentDir.toURI().toString());

        final File someFile = new File(currentDir, "tempFile");
        fileUriTestHelper(someFile.getPath(), false, someFile.toURI().toString());

        // Check if trailing slash gets added for a directory
        final String expectedDirURI = "file:" + currentDir.getPath() + "/path/to/directory/";
        fileUriTestHelper(currentDir.getPath() + "/path/to/directory", true, expectedDirURI);

        // Check if multiple slashes get normalized
        fileUriTestHelper(currentDir.getPath() + "////path///to////directory////", true, expectedDirURI);

        // Check if multiple slashes in the beginning get normalized
        fileUriTestHelper("////" + currentDir.getPath() + "/path/to/directory", true, expectedDirURI);

        // Check for bad inputs for files with trailing slashes
        final String expectedFileURI = someFile.toURI().toString();
        fileUriTestHelper(someFile.getPath() + "/", false, expectedFileURI);
        Assert.assertEquals(expectedFileURI,
                FileUtils.convertToURI("file:" + someFile.getPath() + "/", false).toString());
    }

    private static void fileUriTestHelper(final String filePath, final boolean isDirectory,
            final String expectedURIString) {
        Assert.assertEquals(expectedURIString, FileUtils.convertToURI(filePath, isDirectory).toString());
        Assert.assertEquals(expectedURIString, FileUtils.convertToURI(new File(filePath), isDirectory).toString());
        Assert.assertEquals(expectedURIString, FileUtils.convertToURI(Path.of(filePath), isDirectory).toString());
    }

    public void testConvertToS3URI() throws URISyntaxException {
        Assert.assertEquals("s3://bucket/key", FileUtils.convertToURI("s3://bucket/key", false).toString());

        // Check if trailing slash gets added for a directory
        Assert.assertEquals("s3://bucket/key/".toString(), FileUtils.convertToURI("s3://bucket/key", true).toString());

        // Check if multiple slashes get normalized
        Assert.assertEquals("s3://bucket/key/", FileUtils.convertToURI("s3://bucket///key///", true).toString());

        // Check if trailing slash gets added to bucket root
        Assert.assertEquals("s3://bucket/", FileUtils.convertToURI("s3://bucket", true).toString());

        try {
            FileUtils.convertToURI("", false);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }

        try {
            FileUtils.convertToURI("s3://bucket/key/", false);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
    }
}
