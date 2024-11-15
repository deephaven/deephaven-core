//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;

import junit.framework.TestCase;
import org.junit.Assert;

public class FileUtilsTest extends TestCase {

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
