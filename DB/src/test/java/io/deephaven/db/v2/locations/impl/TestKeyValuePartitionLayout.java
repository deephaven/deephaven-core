package io.deephaven.db.v2.locations.impl;

import io.deephaven.base.FileUtils;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.local.FileTableLocationKey;
import io.deephaven.db.v2.locations.local.KeyValuePartitionLayout;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Unit tests for {@link TestKeyValuePartitionLayout}.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class TestKeyValuePartitionLayout {

    private File dataDirectory;

    @Before
    public void setUp() throws IOException {
        dataDirectory = Files.createTempDirectory(Paths.get(""), "TestChunkedRegionedOperations-").toFile();
        dataDirectory.deleteOnExit();
    }

    @After
    public void tearDown() {
        FileUtils.deleteRecursively(dataDirectory);
    }

    @Test
    public void testFlat() throws IOException {
        final File file1 = new File(dataDirectory, "file1");
        final File file2 = new File(dataDirectory, "file2");
        Files.write(file1.toPath(), "Hello world!".getBytes());
        Files.write(file2.toPath(), "Goodbye cruel world!".getBytes());

        final RecordingLocationKeyFinder<FileTableLocationKey> recorder = new RecordingLocationKeyFinder<>();
        new KeyValuePartitionLayout<>(dataDirectory, path -> true, (path, partitions) -> new FileTableLocationKey(path.toFile(), 0, partitions), 0).findKeys(recorder);
        final List<FileTableLocationKey> results = recorder.getRecordedKeys().stream().sorted().collect(Collectors.toList());

        TestCase.assertEquals(2, results.size());

        TestCase.assertEquals(file1.getAbsoluteFile(), results.get(0).getFile());
        TestCase.assertEquals(file2.getAbsoluteFile(), results.get(1).getFile());

        TestCase.assertTrue(results.get(0).getPartitionKeys().isEmpty());
        TestCase.assertTrue(results.get(1).getPartitionKeys().isEmpty());
    }

    @Test
    public void testOneLevel() throws IOException {
        final File file1 = new File(dataDirectory, "Country=US" + File.separator + "file1");
        final File file2 = new File(dataDirectory, "Country=France" + File.separator + "file2");
        file1.getParentFile().mkdirs();
        file2.getParentFile().mkdirs();
        Files.write(file1.toPath(), "Hello world!".getBytes());
        Files.write(file2.toPath(), "Goodbye cruel world!".getBytes());

        final RecordingLocationKeyFinder<FileTableLocationKey> recorder = new RecordingLocationKeyFinder<>();
        new KeyValuePartitionLayout<>(dataDirectory, path -> true, (path, partitions) -> new FileTableLocationKey(path.toFile(), 0, partitions), 1).findKeys(recorder);
        final List<FileTableLocationKey> results = recorder.getRecordedKeys().stream().sorted().collect(Collectors.toList());

        TestCase.assertEquals(2, results.size());

        TestCase.assertEquals(file2.getAbsoluteFile(), results.get(0).getFile());
        TestCase.assertEquals(file1.getAbsoluteFile(), results.get(1).getFile());

        TestCase.assertEquals(1, results.get(0).getPartitionKeys().size());
        TestCase.assertEquals(1, results.get(1).getPartitionKeys().size());

        TestCase.assertEquals("France", results.get(0).getPartitionValue("Country"));
        TestCase.assertEquals("US", results.get(1).getPartitionValue("Country"));
    }

    @Test
    public void testThreeLevels() throws IOException {
        final File file1 = new File(dataDirectory, "Country=US" + File.separator + "State=New York" + File.separator + "City=New York" + File.separator + "file1");
        final File file2 = new File(dataDirectory, "Country=France" + File.separator + "State=Grand Est" + File.separator + "City=Reims" + File.separator + "file2");
        final File file3 = new File(dataDirectory, "Country=France" + File.separator + "State=Grand Est" + File.separator + "City=Strasbourg" + File.separator + "file3");
        file1.getParentFile().mkdirs();
        file2.getParentFile().mkdirs();
        file3.getParentFile().mkdirs();
        Files.write(file1.toPath(), "Hello world!".getBytes());
        Files.write(file2.toPath(), "Goodbye cruel world!".getBytes());
        Files.write(file3.toPath(), "Oui!".getBytes());

        final RecordingLocationKeyFinder<FileTableLocationKey> recorder = new RecordingLocationKeyFinder<>();
        new KeyValuePartitionLayout<>(dataDirectory, path -> true, (path, partitions) -> new FileTableLocationKey(path.toFile(), 0, partitions), 3).findKeys(recorder);
        final List<FileTableLocationKey> results = recorder.getRecordedKeys().stream().sorted().collect(Collectors.toList());

        TestCase.assertEquals(3, results.size());

        TestCase.assertEquals(file2.getAbsoluteFile(), results.get(0).getFile());
        TestCase.assertEquals(file3.getAbsoluteFile(), results.get(1).getFile());
        TestCase.assertEquals(file1.getAbsoluteFile(), results.get(2).getFile());

        TestCase.assertEquals(3, results.get(0).getPartitionKeys().size());
        TestCase.assertEquals(3, results.get(1).getPartitionKeys().size());
        TestCase.assertEquals(3, results.get(2).getPartitionKeys().size());

        TestCase.assertEquals("France", results.get(0).getPartitionValue("Country"));
        TestCase.assertEquals("France", results.get(1).getPartitionValue("Country"));
        TestCase.assertEquals("US", results.get(2).getPartitionValue("Country"));

        TestCase.assertEquals("Grand Est", results.get(0).getPartitionValue("State"));
        TestCase.assertEquals("Grand Est", results.get(1).getPartitionValue("State"));
        TestCase.assertEquals("New York", results.get(2).getPartitionValue("State"));

        TestCase.assertEquals("Reims", results.get(0).getPartitionValue("City"));
        TestCase.assertEquals("Strasbourg", results.get(1).getPartitionValue("City"));
        TestCase.assertEquals("New York", results.get(2).getPartitionValue("City"));
    }

    @Test
    public void testTypesAndNameLegalization() throws IOException {
        final File file1 = new File(dataDirectory, "A=2" + File.separator + "B 1=3.14" + File.separator + " C=true" + File.separator + "file1");
        final File file2 = new File(dataDirectory, "A=1" + File.separator + "B 1=7.0" + File.separator + " C=false" + File.separator + "file2");
        final File file3 = new File(dataDirectory, "A=1" + File.separator + "B 1=100" + File.separator + " C=false" + File.separator + "file3");
        file1.getParentFile().mkdirs();
        file2.getParentFile().mkdirs();
        file3.getParentFile().mkdirs();
        Files.write(file1.toPath(), "Hello world!".getBytes());
        Files.write(file2.toPath(), "Goodbye cruel world!".getBytes());
        Files.write(file3.toPath(), "Oui!".getBytes());

        final RecordingLocationKeyFinder<FileTableLocationKey> recorder = new RecordingLocationKeyFinder<>();
        new KeyValuePartitionLayout<>(dataDirectory, path -> true, (path, partitions) -> new FileTableLocationKey(path.toFile(), 0, partitions), 3).findKeys(recorder);
        final List<FileTableLocationKey> results = recorder.getRecordedKeys().stream().sorted().collect(Collectors.toList());

        TestCase.assertEquals(3, results.size());

        TestCase.assertEquals(file2.getAbsoluteFile(), results.get(0).getFile());
        TestCase.assertEquals(file3.getAbsoluteFile(), results.get(1).getFile());
        TestCase.assertEquals(file1.getAbsoluteFile(), results.get(2).getFile());

        TestCase.assertEquals(3, results.get(0).getPartitionKeys().size());
        TestCase.assertEquals(3, results.get(1).getPartitionKeys().size());
        TestCase.assertEquals(3, results.get(2).getPartitionKeys().size());

        TestCase.assertEquals((Integer) 1, results.get(0).getPartitionValue("A"));
        TestCase.assertEquals((Integer) 1, results.get(1).getPartitionValue("A"));
        TestCase.assertEquals((Integer) 2, results.get(2).getPartitionValue("A"));

        TestCase.assertEquals(7.0, results.get(0).getPartitionValue("B1"));
        TestCase.assertEquals(100.0, results.get(1).getPartitionValue("B1"));
        TestCase.assertEquals(3.14, results.get(2).getPartitionValue("B1"));

        TestCase.assertEquals(Boolean.FALSE, results.get(0).getPartitionValue("C"));
        TestCase.assertEquals(Boolean.FALSE, results.get(1).getPartitionValue("C"));
        TestCase.assertEquals(Boolean.TRUE, results.get(2).getPartitionValue("C"));
    }

    @Test
    public void testMaxDepthEmpty() throws IOException {
        final File file1 = new File(dataDirectory, "Country=US" + File.separator + "State=New York" + File.separator + "City=New York" + File.separator + "Dummy=Nowhere" + File.separator + "file1");
        final File file2 = new File(dataDirectory, "Country=France" + File.separator + "State=Grand Est" + File.separator + "City=Reims" + File.separator + "Dummy=Nowhere" + File.separator + "file2");
        final File file3 = new File(dataDirectory, "Country=France" + File.separator + "State=Grand Est" + File.separator + "City=Strasbourg" + File.separator + "Dummy=Nowhere" + File.separator + "file3");
        file1.getParentFile().mkdirs();
        file2.getParentFile().mkdirs();
        file3.getParentFile().mkdirs();
        Files.write(file1.toPath(), "Hello world!".getBytes());
        Files.write(file2.toPath(), "Goodbye cruel world!".getBytes());
        Files.write(file3.toPath(), "Oui!".getBytes());

        final RecordingLocationKeyFinder<FileTableLocationKey> recorder = new RecordingLocationKeyFinder<>();
        new KeyValuePartitionLayout<>(dataDirectory, path -> true, (path, partitions) -> new FileTableLocationKey(path.toFile(), 0, partitions), 3).findKeys(recorder);
        final List<FileTableLocationKey> results = recorder.getRecordedKeys().stream().sorted().collect(Collectors.toList());

        TestCase.assertTrue(results.isEmpty());
    }

    @Test
    public void testMaxDepth() throws IOException {
        final File file1 = new File(dataDirectory, "Country=US" + File.separator + "State=New York" + File.separator + "City=New York" + File.separator + "file1");
        final File file2 = new File(dataDirectory, "Country=France" + File.separator + "State=Grand Est" + File.separator + "City=Reims" + File.separator + "file2");
        final File file3 = new File(dataDirectory, "Country=France" + File.separator + "State=Grand Est" + File.separator + "City=Strasbourg" + File.separator + "file3");
        final File file4 = new File(dataDirectory, "Country=France" + File.separator + "State=Grand Est" + File.separator + "City=Strasbourg" + File.separator + "Dummy=Nowhere" + File.separator + "file4");
        file1.getParentFile().mkdirs();
        file2.getParentFile().mkdirs();
        file3.getParentFile().mkdirs();
        file4.getParentFile().mkdirs();
        Files.write(file1.toPath(), "Hello world!".getBytes());
        Files.write(file2.toPath(), "Goodbye cruel world!".getBytes());
        Files.write(file3.toPath(), "Oui!".getBytes());
        Files.write(file4.toPath(), "Non!".getBytes());

        final RecordingLocationKeyFinder<FileTableLocationKey> recorder = new RecordingLocationKeyFinder<>();
        new KeyValuePartitionLayout<>(dataDirectory, path -> true, (path, partitions) -> new FileTableLocationKey(path.toFile(), 0, partitions), 3).findKeys(recorder);
        final List<FileTableLocationKey> results = recorder.getRecordedKeys().stream().sorted().collect(Collectors.toList());

        TestCase.assertEquals(3, results.size());

        TestCase.assertEquals(file2.getAbsoluteFile(), results.get(0).getFile());
        TestCase.assertEquals(file3.getAbsoluteFile(), results.get(1).getFile());
        TestCase.assertEquals(file1.getAbsoluteFile(), results.get(2).getFile());
    }

    @Test
    public void testMismatch() throws IOException {
        final File file1 = new File(dataDirectory, "Country=US" + File.separator + "State=New York" + File.separator + "City=New York" + File.separator + "file1");
        final File file2 = new File(dataDirectory, "Country=France" + File.separator + "Region=Grand Est" + File.separator + "City=Reims" + File.separator + "file2");
        final File file3 = new File(dataDirectory, "Country=France" + File.separator + "Region=Grand Est" + File.separator + "City=Strasbourg" + File.separator + "file3");
        file1.getParentFile().mkdirs();
        file2.getParentFile().mkdirs();
        file3.getParentFile().mkdirs();
        Files.write(file1.toPath(), "Hello world!".getBytes());
        Files.write(file2.toPath(), "Goodbye cruel world!".getBytes());
        Files.write(file3.toPath(), "Oui!".getBytes());

        try {
            new KeyValuePartitionLayout<>(dataDirectory, path -> true, (path, partitions) -> new FileTableLocationKey(path.toFile(), 0, partitions), 3).findKeys(ftlk -> {});
            TestCase.fail("Expected exception");
        } catch (TableDataException expected) {
        }
    }
}
