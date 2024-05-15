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
import org.junit.rules.TemporaryFolder;

public class FileUtilsTest extends TestCase {

    public void testConvertToFileURI() throws IOException {
        final TemporaryFolder folder = new TemporaryFolder();
        folder.create();

        final File someDir = folder.newFolder();
        fileUriTestHelper(someDir.getPath(), true, someDir.toURI().toString());

        final File someFile = folder.newFile();
        fileUriTestHelper(someFile.getPath(), false, someFile.toURI().toString());

        // Check if trailing slash gets added for a directory
        final String expectedURI = "file:" + someDir.getPath() + "/path/to/directory/";
        fileUriTestHelper(someDir.getPath() + "/path/to/directory", true, expectedURI);

        // Check if multiple slashes get normalized
        fileUriTestHelper(someDir.getPath() + "////path///to////directory", true, expectedURI);

        // Check if multiple slashes in the beginning get normalized
        fileUriTestHelper("////" + someDir.getPath() + "/path/to/directory", true, expectedURI);
    }

    private static void fileUriTestHelper(final String filePath, final boolean isDirectory,
            final String expctedURIString) {
        Assert.assertEquals(expctedURIString, FileUtils.convertToURI(filePath, isDirectory).toString());
        Assert.assertEquals(expctedURIString, FileUtils.convertToURI(new File(filePath), isDirectory).toString());
        Assert.assertEquals(expctedURIString, FileUtils.convertToURI(Path.of(filePath), isDirectory).toString());
    }

    public void testConvertToS3URI() throws URISyntaxException {
        assertEquals("s3:/bucket/key", FileUtils.convertToURI("s3:/bucket/key", false).toString());

        // Check if trailing slash gets added for a directory
        assertEquals("s3:/bucket/key/".toString(), FileUtils.convertToURI("s3:/bucket/key", true).toString());

        // Check if multiple slashes get normalized
        assertEquals("s3:/bucket/key/", FileUtils.convertToURI("s3:////bucket///key///", true).toString());

        try {
            FileUtils.convertToURI("", false);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
        }
    }
}
