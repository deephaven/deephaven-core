package io.deephaven.util.files;

import io.deephaven.base.FileUtils;
import io.deephaven.base.testing.BaseArrayTestCase;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static io.deephaven.datastructures.util.CollectionUtil.ZERO_LENGTH_STRING_ARRAY;
import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

public class TestDirWatchService extends BaseArrayTestCase {
    final private static String TEST_DIR = "TestDirWatchService";

    private LinkedList<String> addedFiles;
    private LinkedList<String> modifiedFiles;
    private LinkedList<String> deletedFiles;
    private boolean exceptionOccurred = false;

    private final static String STARTSWITH_FILE = "testing.foo.bin";
    private final static String STARTSWITH_FILE2 = "testing.oof.bin";
    private final static String ENDSWITH_FILE = "trial.foo.aaaaaa";
    private final static String EXACT_MATCH_FILE = "abcde.foo.bin";
    private final static String EXACT_MATCH_FILE2 = "abcde.oof.bin";
    private final static String REGEX_MATCH_FILE = "test_file.stats.foo.file";
    private final static String FILE_NOT_FOUND_1 = "tttt.foo.bin";
    private final static String FILE_NOT_FOUND_2 = "abcdef.foo.aaaa";
    private final static String FILE_NOT_FOUND_3 = "abcdef.foo.bin";
    private final static String FILE_NOT_FOUND_4 = "stats_bad.foo.bin";
    private final static String FILE_NOT_FOUND_5 = "test_file2.stats.oof.file";
    private File dir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FileUtils.deleteRecursively(new File(TEST_DIR));
        dir = new File(TEST_DIR);
        addedFiles = new LinkedList<>();
        deletedFiles = new LinkedList<>();
        modifiedFiles = new LinkedList<>();
        assertTrue(dir.mkdirs());
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        new File(TEST_DIR + File.separator + STARTSWITH_FILE).delete();
        new File(TEST_DIR + File.separator + STARTSWITH_FILE2).delete();
        new File(TEST_DIR + File.separator + ENDSWITH_FILE).delete();
        new File(TEST_DIR + File.separator + EXACT_MATCH_FILE).delete();
        new File(TEST_DIR + File.separator + EXACT_MATCH_FILE2).delete();
        new File(TEST_DIR + File.separator + REGEX_MATCH_FILE).delete();
        new File(TEST_DIR + File.separator + FILE_NOT_FOUND_1).delete();
        new File(TEST_DIR + File.separator + FILE_NOT_FOUND_2).delete();
        new File(TEST_DIR + File.separator + FILE_NOT_FOUND_3).delete();
        new File(TEST_DIR + File.separator + FILE_NOT_FOUND_4).delete();
        new File(TEST_DIR + File.separator + FILE_NOT_FOUND_5).delete();
        dir.delete();
    }

    public void testBasicFilesJavaFileWatch() throws Exception {

        final String[] expectedResults = {STARTSWITH_FILE, STARTSWITH_FILE2, ENDSWITH_FILE, EXACT_MATCH_FILE,
                EXACT_MATCH_FILE, EXACT_MATCH_FILE2, EXACT_MATCH_FILE2, REGEX_MATCH_FILE};
        Arrays.sort(expectedResults);

        final DirWatchService service = new DirWatchService(dir.toString(),
                this::watcherExceptionOccurred,
                DirWatchService.WatchServiceType.JAVAWATCHSERVICE,
                1000,
                ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        service.start();

        service.addFileWatchAtStart(DirWatchService.makeStartsWithMatcher("test"),
                (p, w) -> checkAndUpdate(p, w, STARTSWITH_FILE, STARTSWITH_FILE2));

        service.addFileWatchAtStart(DirWatchService.makeEndsWithMatcher("aaaa"),
                (p, w) -> checkAndUpdate(p, w, ENDSWITH_FILE));

        service.addExactFileWatch(".foo.", "abcde",
                (p, w) -> checkAndUpdate(p, w, EXACT_MATCH_FILE, EXACT_MATCH_FILE2));
        service.addExactFileWatch(".oof.", "abcde",
                (p, w) -> checkAndUpdate(p, w, EXACT_MATCH_FILE, EXACT_MATCH_FILE2));

        // Add a second exact watch pair (one per separator), this should catch both the exact match files so increases
        // the expected results to 8
        service.addExactFileWatch(".foo.", "abcde",
                (p, w) -> checkAndUpdate(p, w, EXACT_MATCH_FILE, EXACT_MATCH_FILE2));
        service.addExactFileWatch(".oof.", "abcde",
                (p, w) -> checkAndUpdate(p, w, EXACT_MATCH_FILE, EXACT_MATCH_FILE2));

        service.addFileWatchAtStart(DirWatchService.makeRegexMatcher("^[A-Za-z0-9_]*\\.stats\\.foo\\..*"),
                (p, w) -> checkAndUpdate(p, w, REGEX_MATCH_FILE));

        assertTrue(new File(TEST_DIR + File.separator + STARTSWITH_FILE).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + STARTSWITH_FILE2).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + ENDSWITH_FILE).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE2).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + REGEX_MATCH_FILE).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_1).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_2).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_3).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_4).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_5).createNewFile());

        sleepUntilConditionTrue(15000, () -> addedFiles.size() == 8);
        synchronized (this) {
            assertFalse(exceptionOccurred);
            assertEquals(8, addedFiles.size());
            assertEquals(0, deletedFiles.size());
            assertEquals(0, modifiedFiles.size());
            final String[] foundFiles = addedFiles.toArray(ZERO_LENGTH_STRING_ARRAY);
            Arrays.sort(foundFiles);
            assertEquals(expectedResults, foundFiles);
        }

        assertTrue(touch(new File(TEST_DIR + File.separator + STARTSWITH_FILE)));
        assertTrue(touch(new File(TEST_DIR + File.separator + STARTSWITH_FILE2)));
        assertTrue(touch(new File(TEST_DIR + File.separator + ENDSWITH_FILE)));
        assertTrue(touch(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE)));
        assertTrue(touch(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE2)));
        assertTrue(touch(new File(TEST_DIR + File.separator + REGEX_MATCH_FILE)));
        assertTrue(touch(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_1)));
        assertTrue(touch(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_2)));
        assertTrue(touch(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_3)));
        assertTrue(touch(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_4)));
        assertTrue(touch(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_5)));

        sleepUntilConditionTrue(15000, () -> modifiedFiles.size() == 8);
        synchronized (this) {
            assertFalse(exceptionOccurred);
            assertEquals(8, addedFiles.size());
            assertEquals(0, deletedFiles.size());
            assertEquals(8, modifiedFiles.size());
            final String[] updatedFiles = modifiedFiles.toArray(ZERO_LENGTH_STRING_ARRAY);
            Arrays.sort(updatedFiles);
            assertEquals(expectedResults, updatedFiles);
        }

        assertTrue(new File(TEST_DIR + File.separator + STARTSWITH_FILE).delete());
        assertTrue(new File(TEST_DIR + File.separator + STARTSWITH_FILE2).delete());
        assertTrue(new File(TEST_DIR + File.separator + ENDSWITH_FILE).delete());
        assertTrue(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE).delete());
        assertTrue(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE2).delete());
        assertTrue(new File(TEST_DIR + File.separator + REGEX_MATCH_FILE).delete());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_1).delete());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_2).delete());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_3).delete());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_4).delete());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_5).delete());

        sleepUntilConditionTrue(15000, () -> deletedFiles.size() == 8);
        synchronized (this) {
            assertFalse(exceptionOccurred);
            assertEquals(8, addedFiles.size());
            assertEquals(8, deletedFiles.size());
            assertEquals(8, modifiedFiles.size());
            final String[] removedFiles = deletedFiles.toArray(ZERO_LENGTH_STRING_ARRAY);
            Arrays.sort(removedFiles);
            assertEquals(expectedResults, removedFiles);
        }
    }

    public void testBasicFilesPollFileWatch() throws Exception {

        final String[] expectedResults = {STARTSWITH_FILE, STARTSWITH_FILE2, ENDSWITH_FILE, EXACT_MATCH_FILE,
                EXACT_MATCH_FILE2, REGEX_MATCH_FILE};
        Arrays.sort(expectedResults);

        final DirWatchService service = new DirWatchService(dir.toString(),
                this::watcherExceptionOccurred,
                DirWatchService.WatchServiceType.POLLWATCHSERVICE,
                1000,
                ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        service.start();

        service.addFileWatchAtStart(DirWatchService.makeStartsWithMatcher("test"),
                (p, w) -> checkAndUpdate(p, w, STARTSWITH_FILE, STARTSWITH_FILE2));

        service.addFileWatchAtStart(DirWatchService.makeEndsWithMatcher("aaaa"),
                (p, w) -> checkAndUpdate(p, w, ENDSWITH_FILE));

        service.addExactFileWatch(".foo.", "abcde",
                (p, w) -> checkAndUpdate(p, w, EXACT_MATCH_FILE, EXACT_MATCH_FILE2));
        service.addExactFileWatch(".oof.", "abcde",
                (p, w) -> checkAndUpdate(p, w, EXACT_MATCH_FILE, EXACT_MATCH_FILE2));

        service.addFileWatchAtStart(DirWatchService.makeRegexMatcher("^[A-Za-z0-9_]*\\.stats\\.foo\\..*"),
                (p, w) -> checkAndUpdate(p, w, REGEX_MATCH_FILE));

        assertTrue(new File(TEST_DIR + File.separator + STARTSWITH_FILE).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + STARTSWITH_FILE2).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + ENDSWITH_FILE).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE2).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + REGEX_MATCH_FILE).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_1).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_2).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_3).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_4).createNewFile());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_5).createNewFile());

        sleepUntilConditionTrue(10000, () -> addedFiles.size() == 6);
        synchronized (this) {
            assertFalse(exceptionOccurred);
            assertEquals(6, addedFiles.size());
            assertEquals(0, deletedFiles.size());
            assertEquals(0, modifiedFiles.size());
            final String[] foundFiles = addedFiles.toArray(ZERO_LENGTH_STRING_ARRAY);
            Arrays.sort(foundFiles);
            assertEquals(expectedResults, foundFiles);
        }

        // Wait a second before touching the files to ensure the poll watch service picks them up
        sleepWithoutInterruptions(1000L);
        assertTrue(touch(new File(TEST_DIR + File.separator + STARTSWITH_FILE)));
        assertTrue(touch(new File(TEST_DIR + File.separator + STARTSWITH_FILE2)));
        assertTrue(touch(new File(TEST_DIR + File.separator + ENDSWITH_FILE)));
        assertTrue(touch(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE)));
        assertTrue(touch(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE2)));
        assertTrue(touch(new File(TEST_DIR + File.separator + REGEX_MATCH_FILE)));
        assertTrue(touch(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_1)));
        assertTrue(touch(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_2)));
        assertTrue(touch(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_3)));
        assertTrue(touch(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_4)));
        assertTrue(touch(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_5)));

        sleepUntilConditionTrue(10000, () -> modifiedFiles.size() == 6);
        synchronized (this) {
            assertFalse(exceptionOccurred);
            assertEquals(6, addedFiles.size());
            assertEquals(0, deletedFiles.size());
            assertEquals(6, modifiedFiles.size());
            final String[] updatedFiles = modifiedFiles.toArray(ZERO_LENGTH_STRING_ARRAY);
            Arrays.sort(updatedFiles);
            assertEquals(expectedResults, updatedFiles);
        }

        assertTrue(new File(TEST_DIR + File.separator + STARTSWITH_FILE).delete());
        assertTrue(new File(TEST_DIR + File.separator + STARTSWITH_FILE2).delete());
        assertTrue(new File(TEST_DIR + File.separator + ENDSWITH_FILE).delete());
        assertTrue(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE).delete());
        assertTrue(new File(TEST_DIR + File.separator + EXACT_MATCH_FILE2).delete());
        assertTrue(new File(TEST_DIR + File.separator + REGEX_MATCH_FILE).delete());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_1).delete());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_2).delete());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_3).delete());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_4).delete());
        assertTrue(new File(TEST_DIR + File.separator + FILE_NOT_FOUND_5).delete());

        sleepUntilConditionTrue(10000, () -> deletedFiles.size() == 6);
        synchronized (this) {
            assertFalse(exceptionOccurred);
            assertEquals(6, addedFiles.size());
            assertEquals(6, deletedFiles.size());

            // On Mac sometimes the .delete() modifies a file before deleting it, resulting in a second entry for a
            // file. Remove duplicates from this list before checking.
            final List<String> modifiedFilesNoDuplicates = modifiedFiles.stream()
                    .distinct()
                    .collect(Collectors.toList());
            assertEquals(6, modifiedFilesNoDuplicates.size());
            final String[] removedFiles = deletedFiles.toArray(new String[0]);
            Arrays.sort(removedFiles);
            assertEquals(expectedResults, removedFiles);
        }
    }

    public void testStopJava() throws Exception {
        final DirWatchService service = new DirWatchService(dir.toString(),
                this::watcherExceptionOccurred,
                DirWatchService.WatchServiceType.JAVAWATCHSERVICE,
                1000,
                ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        service.start();
        assertFalse(exceptionOccurred);

        // A second start should generate an exception
        try {
            service.start();
            exceptionOccurred = false;
        } catch (IllegalStateException ignored) {
            exceptionOccurred = true;
        }
        assertTrue(exceptionOccurred);

        exceptionOccurred = false;
        service.stop();
        sleepWithoutInterruptions(2000);
        assertFalse(exceptionOccurred);

        // Should now be able to start again
        service.start();
        assertFalse(exceptionOccurred);
    }

    public void testStopPoll() throws Exception {
        final DirWatchService service = new DirWatchService(dir.toString(),
                this::watcherExceptionOccurred,
                DirWatchService.WatchServiceType.POLLWATCHSERVICE,
                1000,
                ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
        service.start();
        assertFalse(exceptionOccurred);

        // A second start should generate an exception
        try {
            service.start();
            exceptionOccurred = false;
        } catch (IllegalStateException ignored) {
            exceptionOccurred = true;
        }
        assertTrue(exceptionOccurred);

        exceptionOccurred = false;
        service.stop();
        sleepWithoutInterruptions(2000);
        assertFalse(exceptionOccurred);

        // Should now be able to start again
        service.start();
        assertFalse(exceptionOccurred);
    }

    private synchronized void checkAndUpdate(Path p, WatchEvent.Kind k, String... fn) {
        final String s = p.getFileName().toString();
        notify();
        for (final String fileName : fn) {
            if (s.equals(fileName)) {
                if (k == ENTRY_CREATE) {
                    addedFiles.add(s);
                    return;
                } else if (k == ENTRY_DELETE) {
                    deletedFiles.add(s);
                    return;
                } else if (k == ENTRY_MODIFY) {
                    modifiedFiles.add(s);
                    return;
                }
            }
        }
    }

    private static boolean touch(File file) {
        long timestamp = System.currentTimeMillis();
        return file.setLastModified(timestamp);
    }

    @SuppressWarnings("unused")
    private synchronized void watcherExceptionOccurred(
            final DirWatchService.ExceptionConsumerParameter consumerParameter) {
        exceptionOccurred = true;
    }

    @SuppressWarnings("SameParameterValue")
    private static void sleepWithoutInterruptions(final long t) {
        try {
            Thread.sleep(t);
        } catch (InterruptedException ignored) {
        }
    }

    private void sleepUntilConditionTrue(final long maxSleepMillis, BooleanSupplier testProc) {
        long currentTime = System.currentTimeMillis();
        final long endTime = currentTime + maxSleepMillis;
        while (currentTime < endTime) {
            synchronized (this) {
                if (testProc.getAsBoolean()) {
                    break;
                }
                final long sleepMillis = endTime - currentTime;
                try {
                    wait(sleepMillis);
                } catch (InterruptedException ignored) {
                }
            }
            currentTime = System.currentTimeMillis();
        }
    }
}
