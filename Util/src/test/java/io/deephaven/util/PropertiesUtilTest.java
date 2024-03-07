//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;

public class PropertiesUtilTest {

    @Test
    public void testLoad() throws IOException {
        final File tmpFile = File.createTempFile(PropertiesUtilTest.class.getName(), "properties");
        try {
            Files.write(tmpFile.toPath(), Arrays.asList("hello=world", "goodbye=moon"));
            final Properties properties = PropertiesUtil.load(tmpFile.getPath());
            Assert.assertEquals("world", properties.getProperty("hello"));
            Assert.assertEquals("moon", properties.getProperty("goodbye"));
        } finally {
            tmpFile.delete();
        }
    }
}
