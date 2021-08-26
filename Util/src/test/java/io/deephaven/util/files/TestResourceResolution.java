package io.deephaven.util.files;

import io.deephaven.base.FileUtils;
import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.OSUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class TestResourceResolution extends BaseArrayTestCase {

    private static final String TEST_DIR1 = "TestResourceResolution/TestDir1";
    private static final String TEST_DIR2 = "TestResourceResolution/TestDir2";
    private static final String TEST_DIR_BASE = "TestResourceResolution";

    // ResourceResolution should be resilient to Linux/Windows directories and duplicate separators
    private static final String TEST_WILDCARD_LINUX = "Test*/TestDir?/*";
    private static final String TEST_WILDCARD_WINDOWS = "Test*\\TestDir?\\*";
    private static final String TEST_WILDCARD_LINUX_DUPS = "Test*///TestDir?/*";
    private static final String TEST_WILDCARD_WINDOWS_DUPS = "Test*\\TestDir?\\\\\\*";
    private static final String SUFFIX = ".tester";
    private static final String ZIP_FILE = TEST_DIR2 + File.separator + "zipTest.jar";
    private static final String ZIP_ENTRY1 = "testZip1" + SUFFIX;
    private static final String ZIP_ENTRY2 = "testZip2" + SUFFIX;
    private static final String TEST_FILE1_NAME = "testFoo1" + SUFFIX;
    private static final String TEST_FILE2_NAME = "testBar1" + SUFFIX;
    private static final String TEST_FILE1 = TEST_DIR1 + File.separator + TEST_FILE1_NAME;
    private static final String TEST_FILE2 = TEST_DIR2 + File.separator + TEST_FILE2_NAME;

    private File dir1;
    private File dir2;
    private File dirBase;

    private Map<String, URL> filenameToUrlMap;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FileUtils.deleteRecursively(new File(TEST_DIR1));
        FileUtils.deleteRecursively(new File(TEST_DIR2));
        dir1 = new File(TEST_DIR1);
        assertTrue(dir1.mkdirs());
        dir2 = new File(TEST_DIR2);
        assertTrue(dir2.mkdirs());
        dirBase = new File(TEST_DIR_BASE);

        assertTrue(new File(TEST_FILE1).createNewFile());
        assertTrue(new File(TEST_FILE2).createNewFile());

        final String zipTestString = "Test String";
        final byte[] data = zipTestString.getBytes();

        final File zipFile = new File(ZIP_FILE);
        final ZipOutputStream zipOut = new ZipOutputStream(new FileOutputStream(zipFile));

        final ZipEntry e1 = new ZipEntry(ZIP_ENTRY1);
        zipOut.putNextEntry(e1);
        zipOut.write(data, 0, data.length);

        final ZipEntry e2 = new ZipEntry(ZIP_ENTRY2);
        zipOut.putNextEntry(e2);
        zipOut.write(data, 0, data.length);

        zipOut.closeEntry();
        zipOut.close();

        filenameToUrlMap = new HashMap<>();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        new File(TEST_FILE1).delete();
        new File(TEST_FILE2).delete();
        new File(ZIP_FILE).delete();

        dir1.delete();
        dir2.delete();
        dirBase.delete();
    }

    public void testRelativeOneDirNoWildcard() throws IOException {
        ResourceResolution resourceResolution =
            new ResourceResolution(Configuration.getInstance(), null, TEST_DIR2);
        resourceResolution.findResources(SUFFIX, this::resourceFound);
        assertTrue(filenameToUrlMap.size() == 3);
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE2_NAME));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY1));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY2));
        validateFilenameMap();
    }

    public void testAbsoluteTwoDirsNoWildcard() throws IOException {
        final String dirName1 = dir1.getAbsolutePath();
        final String dirName2 = dir2.getAbsolutePath();

        ResourceResolution resourceResolution =
            new ResourceResolution(Configuration.getInstance(), null, dirName1, dirName2);
        resourceResolution.findResources(SUFFIX, this::resourceFound);
        assertTrue(filenameToUrlMap.size() == 4);
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE1_NAME));
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE2_NAME));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY1));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY2));
        validateFilenameMap();
    }

    public void testRelativeTopDirNoWildcard() throws IOException {
        ResourceResolution resourceResolution =
            new ResourceResolution(Configuration.getInstance(), null, TEST_DIR_BASE);
        resourceResolution.findResources(SUFFIX, this::resourceFound);
        assertTrue(filenameToUrlMap.size() == 4);
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE1_NAME));
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE2_NAME));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY1));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY2));
        validateFilenameMap();
    }

    public void testAbsoluteTopDirNoWildcard() throws IOException {
        final String dirName = dirBase.getAbsolutePath();

        ResourceResolution resourceResolution =
            new ResourceResolution(Configuration.getInstance(), null, dirName);
        resourceResolution.findResources(SUFFIX, this::resourceFound);
        assertTrue(filenameToUrlMap.size() == 4);
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE1_NAME));
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE2_NAME));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY1));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY2));
        validateFilenameMap();
    }

    public void testRelativeWildcardLinux() throws IOException {
        ResourceResolution resourceResolution =
            new ResourceResolution(Configuration.getInstance(), null, TEST_WILDCARD_LINUX);
        resourceResolution.findResources(SUFFIX, this::resourceFound);
        assertTrue(filenameToUrlMap.size() == 4);
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE1_NAME));
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE2_NAME));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY1));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY2));
        validateFilenameMap();
    }

    public void testAbsoluteWildcardLinux() throws IOException {
        final File dirBase = new File(TEST_WILDCARD_LINUX);
        final String dirName = dirBase.getAbsolutePath();

        ResourceResolution resourceResolution =
            new ResourceResolution(Configuration.getInstance(), null, dirName);
        resourceResolution.findResources(SUFFIX, this::resourceFound);
        assertTrue(filenameToUrlMap.size() == 4);
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE1_NAME));
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE2_NAME));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY1));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY2));
        validateFilenameMap();
    }

    public void testRelativeWildcardWindows() throws IOException {
        ResourceResolution resourceResolution =
            new ResourceResolution(Configuration.getInstance(), null, TEST_WILDCARD_WINDOWS);
        resourceResolution.findResources(SUFFIX, this::resourceFound);
        assertTrue(filenameToUrlMap.size() == 4);
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE1_NAME));
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE2_NAME));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY1));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY2));
        validateFilenameMap();
    }

    public void testAbsoluteWildcardWindows() throws IOException {
        final File dirBase = new File(TEST_WILDCARD_WINDOWS);
        final String dirName = dirBase.getAbsolutePath();

        ResourceResolution resourceResolution =
            new ResourceResolution(Configuration.getInstance(), null, dirName);
        resourceResolution.findResources(SUFFIX, this::resourceFound);
        assertTrue(filenameToUrlMap.size() == 4);
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE1_NAME));
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE2_NAME));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY1));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY2));
        validateFilenameMap();
    }

    public void testRelativeWildcardLinuxDups() throws IOException {
        ResourceResolution resourceResolution =
            new ResourceResolution(Configuration.getInstance(), null, TEST_WILDCARD_LINUX_DUPS);
        resourceResolution.findResources(SUFFIX, this::resourceFound);
        assertTrue(filenameToUrlMap.size() == 4);
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE1_NAME));
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE2_NAME));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY1));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY2));
        validateFilenameMap();
    }

    public void testAbsoluteWildcardWindowsDups() throws IOException {
        final File dirBase = new File(TEST_WILDCARD_WINDOWS_DUPS);
        final String dirName = dirBase.getAbsolutePath();

        ResourceResolution resourceResolution =
            new ResourceResolution(Configuration.getInstance(), null, dirName);
        resourceResolution.findResources(SUFFIX, this::resourceFound);
        assertTrue(filenameToUrlMap.size() == 4);
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE1_NAME));
        assertTrue(filenameToUrlMap.containsKey(TEST_FILE2_NAME));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY1));
        assertTrue(filenameToUrlMap.containsKey(ZIP_ENTRY2));
        validateFilenameMap();
    }

    public void testUNC() {
        ResourceResolution resourceResolution =
            new ResourceResolution(Configuration.getInstance(), null, "") {
                @Override
                public String normalize(String resourcePath) {
                    return super.normalize(resourcePath);
                }
            };

        final Map<String, String> answers = new HashMap<>();
        final String answer1 = String.join(File.separator, "", "WindowsDC", "path", "to", "");
        final String answer2;
        // separators are collapsed to singles (e.g. /WIndowsDC/path/to) unless it is a UNC path on
        // Windows (\WindowsDC\path\to)
        if (OSUtil.runningWindows()) {
            // The normalize method checks for a Windows file separator type and adds an extra slash
            // for UNC paths
            answer2 = File.separator + answer1;
        } else {
            // For Linux and Mac, the normal normalization is done
            answer2 = answer1;
        }
        answers.put("\\\\WindowsDC\\path\\to\\file1.txt", answer2 + "file1.txt");
        answers.put("\\\\/WindowsDC\\\\\\\\\\path/\\to\\file2.txt", answer2 + "file2.txt");
        answers.put("//WindowsDC/path/to///file3.txt", answer1 + "file3.txt");
        answers.put("///WindowsDC/path/to///file4.txt", answer1 + "file4.txt");

        answers.forEach((resourcePath, answer) -> {

            final String normalizedResourcePath = resourceResolution.normalize(resourcePath);
            assertEquals(answer, normalizedResourcePath);
        });
    }

    private void validateFilenameMap() {
        for (Map.Entry entry : filenameToUrlMap.entrySet()) {
            final String file = (String) entry.getKey();
            final URL url = (URL) entry.getValue();
            assertTrue(url.toString().endsWith(file));
        }
    }

    private void resourceFound(final URL url, final String filename) {
        filenameToUrlMap.put(filename, url);
    }
}
