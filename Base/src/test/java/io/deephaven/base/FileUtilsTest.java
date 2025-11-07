//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base;

import java.io.File;
import java.io.IOException;
import java.net.URI;
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
        if (dosView != null) {
            dosView.setHidden(true);
        }
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

    /**
     * Before the {@link FileUtils#startsWithScheme(String)} check, this was "successfully" parsing via the
     * file-fallback path, to logic that looked something like:
     *
     * <pre>{@code
     * // this is a valid relative File location
     * final File relativeFile = new File("s3://bucket/path with spaces/cool");
     * final URI uri = convertToURI(relativeFile, true);
     * }</pre>
     */
    public void testInvalidURIPaths() {
        badUriString("s3://bucket/path with spaces/cool", false,
                "Failed to convert to URI: 's3://bucket/path with spaces/cool'");
        badUriString("s3://bucket/path with spaces/cool/", true,
                "Failed to convert to URI: 's3://bucket/path with spaces/cool/'");
        badUriString("file:/bad/file uri", false, "Failed to convert to URI: 'file:/bad/file uri'");
        badUriString("file:/bad/file uri/", true, "Failed to convert to URI: 'file:/bad/file uri/'");
    }

    public void testStartsWithScheme() {
        Assert.assertTrue(FileUtils.startsWithScheme("myScheme:something"));
        Assert.assertTrue(FileUtils.startsWithScheme("dh+plain://localhost"));
        Assert.assertTrue(FileUtils.startsWithScheme("x-y://localhost"));
        Assert.assertTrue(FileUtils.startsWithScheme("x.y://localhost"));
        Assert.assertTrue(FileUtils.startsWithScheme("justScheme:"));
        Assert.assertTrue(FileUtils.startsWithScheme("s3://my/bucket"));
        Assert.assertTrue(
                FileUtils.startsWithScheme("scheme: anything after scheme is okay, doesn't have to be a real URI"));

        Assert.assertFalse(FileUtils.startsWithScheme("3://scheme-must-start-with-alpha"));
        Assert.assertFalse(FileUtils.startsWithScheme("nocolon"));
        Assert.assertFalse(FileUtils.startsWithScheme("/file/path"));
        Assert.assertFalse(FileUtils.startsWithScheme("/file/path:something"));
    }

    public void testGoodFileURIs() {
        assertEqualUriString(URI.create("file:/"), FileUtils.convertToURI("file:/", false));
        assertEqualUriString(URI.create("file:/"), FileUtils.convertToURI("file:/", true));
        assertEqualUriString(URI.create("file:/foo"), FileUtils.convertToURI("file:/foo/", false));
        assertEqualUriString(URI.create("file:/foo/"), FileUtils.convertToURI("file:/foo", true));

        assertEqualUriString(URI.create("file:/"), FileUtils.convertToURI("file:///", false));
        assertEqualUriString(URI.create("file:/"), FileUtils.convertToURI("file:///", true));
        assertEqualUriString(URI.create("file:/foo"), FileUtils.convertToURI("file:///foo/", false));
        assertEqualUriString(URI.create("file:/foo/"), FileUtils.convertToURI("file:///foo", true));
    }

    public void testFileSourceWithNoScheme() {
        checkFileSource("my/foo-fake-name");
        checkFileSource("my/foo-fake-name/");
        checkFileSource("/my/foo-fake-name");
        checkFileSource("/my/foo-fake-name/");
        // These are invalid URIs as-is, but work with File
        checkFileSource("/my/foo fake name with spaces");
        checkFileSource("/my/foo fake name with spaces/");
    }

    public void testBadFileURIs() {
        // If the user explicitly passes a file: scheme, we should not be more lenient than new File(uri)
        badFileURI(URI.create("file:foo"), "URI is not hierarchical");
        badFileURI(URI.create("file://foo"), "URI has an authority component");
        badFileURI(URI.create("file:///foo#frag"), "URI has a fragment component");
        badFileURI(URI.create("file:///foo?frag"), "URI has a query component");
    }

    private static void checkFileSource(final String pathname) {
        assertFalse("FileUtils.hasScheme(pathname)", FileUtils.startsWithScheme(pathname));
        final File file = new File(pathname);
        // File.toURI is weird in that it does a filesystem operation to check if the directory exists. Since we don't
        // want to go around creating random files from the working directory just to satisfy File.toURI code path
        // behavior, we'll use paths we are pretty sure don't exist and assert as such.
        assertFalse(file.exists());
        assertEqualUriString(file.toURI(), FileUtils.convertToURI(pathname, false));
        assertEqualUriString(FileUtils.convertToURI(file, false), FileUtils.convertToURI(pathname, false));
        assertEqualUriString(FileUtils.convertToURI(file, true), FileUtils.convertToURI(pathname, true));
    }

    private static void badFileURI(URI uri, String expectedError) {
        assertEquals("file", uri.getScheme());
        try {
            new File(uri);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(expectedError, e.getMessage());
        }
        badUriString(uri.toString(), false, expectedError);
        badUriString(uri.toString(), true, expectedError);
    }

    private static void badUriString(String uriWithScheme, boolean isDirectory, String expectedError) {
        assertTrue("FileUtils.hasScheme(uriWithScheme)", FileUtils.startsWithScheme(uriWithScheme));
        try {
            FileUtils.convertToURI(uriWithScheme, isDirectory);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(expectedError, e.getMessage());
        }
    }

    private static void assertEqualUriString(URI expected, URI actual) {
        // Two URIs can be equal but have _different_ toString representations; this is a stricter equality check.
        // ie URI.create("file:///myfile.txt") vs URI.create("file:/myfile.txt")
        Assert.assertEquals(expected.toString(), actual.toString());
    }
}
