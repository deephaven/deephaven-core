package io.deephaven.util.files;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Utilities to assist with file and directory operations.
 *
 * See MatcherType for details on the matcher types supported.
 *
 */
public class DirWatchService {

    /**
     * The preferred watcher type is the built-in Java one as it's more efficient, but it doesn't catch all new files
     */
    public enum WatchServiceType {
        /** The built-in Java classes are efficient on Linux but don't catch remotely-created NFS files */
        JAVAWATCHSERVICE("JavaWatchService"),
        /** A simple poll loop watcher */
        POLLWATCHSERVICE("PollWatchService");

        private final String name;

        // Provide an array of names for users
        private final static String[] names;
        static {
            final WatchServiceType[] serviceTypes = values();
            names = new String[serviceTypes.length];
            int i = 0;
            for (WatchServiceType serviceType : serviceTypes) {
                names[i++] = serviceType.getName();
            }
        }

        WatchServiceType(final String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public static String[] getNames() {
            return names;
        }
    }

    public static class ExceptionConsumerParameter {
        private final Exception exception;
        private final boolean watchServiceTerminated;
        private final Path path;

        private ExceptionConsumerParameter(final Exception exception, final boolean watchServiceTerminated,
                final Path path) {
            this.exception = exception;
            this.watchServiceTerminated = watchServiceTerminated;
            this.path = path;
        }

        public Exception getException() {
            return exception;
        }

        public boolean watchServiceTerminated() {
            return watchServiceTerminated;
        }

        public Path getPath() {
            return path;
        }
    }

    /**
     * Class to store the consumers and matcher types for use in the Maps used to track the files being watched for
     */
    private class FileWatcher {

        private final List<BiConsumer<Path, WatchEvent.Kind>> consumers;

        FileWatcher(@NotNull final BiConsumer<Path, WatchEvent.Kind> consumer) {
            consumers = new LinkedList<>();
            addConsumer(consumer);
        }

        void addConsumer(@NotNull final BiConsumer<Path, WatchEvent.Kind> consumer) {
            consumers.add(consumer);
        }

        /**
         * Wrap the call to the user's consumer in a try block, as these are called on the watcher thread and we don't
         * want consumer exceptions to kill that thread. At some point we may want to consider moving this call into its
         * own thread so that the file watches don't hang up on long-lived consume operations.
         */
        void consume(final Path p, final WatchEvent.Kind k) {
            for (BiConsumer<Path, WatchEvent.Kind> consumer : consumers) {
                try {
                    consumer.accept(p, k);
                } catch (Exception e) {
                    callExceptionConsumer(e, false);
                }
            }
        }
    }

    /**
     * An "exact-match" file watcher, keyed by the token that must appear before the separator (supplied on
     * registration).
     */
    private class ExactMatchFileWatcher extends FileWatcher {

        private final String tokenToMatch;

        private ExactMatchFileWatcher(@NotNull final BiConsumer<Path, WatchEvent.Kind> consumer,
                @NotNull final String tokenToMatch) {
            super(consumer);
            this.tokenToMatch = tokenToMatch;
        }
    }

    private static final KeyedObjectKey<String, ExactMatchFileWatcher> EXACT_MATCH_KEY =
            new KeyedObjectKey.Basic<String, ExactMatchFileWatcher>() {

                @Override
                public String getKey(@NotNull final ExactMatchFileWatcher value) {
                    return value.tokenToMatch;
                }
            };

    private class SeparatorToExactMatchWatchersPair {

        private final String fileSeparator;
        private final KeyedObjectHashMap<String, ExactMatchFileWatcher> exactMatchWatchers;

        private SeparatorToExactMatchWatchersPair(String fileSeparator) {
            this.fileSeparator = fileSeparator;
            exactMatchWatchers = new KeyedObjectHashMap<>(EXACT_MATCH_KEY);
        }

        private void addWatcher(@NotNull final String filePattern,
                @NotNull final BiConsumer<Path, WatchEvent.Kind> consumer) {
            exactMatchWatchers.compute(filePattern, (k, oldFileWatcher) -> {
                // TODO: If we didn't need to take a consumer in the constructor, we could make this simpler and use
                // putIfAbsent or computeIfAbsent.
                if (oldFileWatcher != null) {
                    oldFileWatcher.addConsumer(consumer);
                    return oldFileWatcher;
                } else {
                    return new ExactMatchFileWatcher(consumer, filePattern);
                }
            });
        }
    }

    /**
     * A file watcher that evaluates whole file names and accepts suitable files.
     */
    private class FilteringFileWatcher extends FileWatcher {

        private final Predicate<String> matcher;

        private FilteringFileWatcher(@NotNull final BiConsumer<Path, WatchEvent.Kind> consumer,
                @NotNull final Predicate<String> matcher) {
            super(consumer);
            this.matcher = matcher;
        }

        private boolean matches(@NotNull final String fileName) {
            return matcher.test(fileName);
        }
    }

    /** Class to define behavior for different file watching mechanisms. */
    private interface WatcherInterface {
        /** Start the file watching interface (i.e. start watching for files) */
        void start() throws Exception;

        /** Stop the file watcher (i.e. stop watching for files) */
        void stop() throws Exception;
    }

    /** WatcherInterface implementation for an implementation that uses the Java WatchService */
    private class WatcherInterfaceJavaImpl implements WatcherInterface {
        private volatile Thread watcherThread;
        private volatile boolean stopRequested;
        private volatile WatchService watcher;
        private final String dirToWatch;
        private final Path dir;
        private final WatchEvent.Kind[] watchEventKinds;

        private WatcherInterfaceJavaImpl(final String dirToWatch, WatchEvent.Kind... kinds) {
            this.dirToWatch = dirToWatch;
            dir = Paths.get(dirToWatch);
            stopRequested = false;
            watchEventKinds = kinds;
        }

        @Override
        public synchronized void start() throws IOException {
            if (watcherThread != null) {
                throw new IllegalStateException("Trying to start a running DirWatchService!");
            }

            watcher = FileSystems.getDefault().newWatchService();
            dir.register(watcher, watchEventKinds);

            watcherThread = new Thread(this::runJavaFileWatcher, "DirWatchService" + "-" + dirToWatch);
            watcherThread.setDaemon(true);
            watcherThread.start();
        }

        @Override
        public synchronized void stop() throws IOException {
            if (watcherThread == null) {
                throw new IllegalStateException("Trying to stop an already-stopped DirWatchService!");
            }

            stopRequested = true;
            final Thread t = watcherThread;
            if (t != null) {
                t.interrupt();
                while (t.isAlive()) {
                    try {
                        t.join();
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            watcher.close();
            watcher = null;
            watcherThread = null;
            stopRequested = false;
        }

        /** Run the DirWatchService */
        private void runJavaFileWatcher() {
            while (!stopRequested) {
                final WatchKey watchKey;
                try {
                    watchKey = watcher.take();
                } catch (InterruptedException e) {
                    if (!stopRequested) {
                        callExceptionConsumer(e, false);
                    }
                    continue;
                } catch (ClosedWatchServiceException e) {
                    callExceptionConsumer(e, true);
                    return;
                }

                for (WatchEvent<?> event : watchKey.pollEvents()) {
                    final WatchEvent.Kind kind = event.kind();
                    // An overflow event means that too many file events have happened and we've lost some - best to
                    // indicate a problem
                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        callExceptionConsumer(new RuntimeException(
                                "Overflow event received in DirWatchService, file events possibly lost"), true);
                        return;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> watchEvent = (WatchEvent<Path>) event;
                    // The watchEvent only contains the relative path, we want the full path
                    final Path filePath = watchEvent.context();
                    final String fileName = filePath.toString();
                    handleFileEvent(fileName, kind);
                }

                if (!watchKey.reset()) {
                    callExceptionConsumer(new RuntimeException("Error resetting watch key in directory watch service"),
                            true);
                    return;
                }
            }
        }
    }

    /** WatcherInterface implementation for an implementation that uses the Apache polling service */
    private class WatcherInterfacePollImpl implements WatcherInterface {
        private final FileAlterationMonitor fileAlterationMonitor;
        private boolean fileAlterationMonitorStarted;

        private WatcherInterfacePollImpl(long pollIntervalMillis, final String dirToWatch, WatchEvent.Kind... kinds) {
            final FileAlterationObserver fileAlterationObserver = new FileAlterationObserver(new File(dirToWatch));

            /*
             * Convert the events to and from the Java standard - it would be nice to have all these in one listener...
             * A switch statement won't work here as these values might look like enums or constants but aren't
             */
            for (WatchEvent.Kind kind : kinds) {
                if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                    fileAlterationObserver.addListener(new FileAlterationListenerAdaptor() {
                        @Override
                        public void onFileCreate(File file) {
                            handlePollFileEvent(file, StandardWatchEventKinds.ENTRY_CREATE);
                        }
                    });
                } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                    fileAlterationObserver.addListener(new FileAlterationListenerAdaptor() {
                        @Override
                        public void onFileChange(File file) {
                            handlePollFileEvent(file, StandardWatchEventKinds.ENTRY_MODIFY);
                        }
                    });
                } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                    fileAlterationObserver.addListener(new FileAlterationListenerAdaptor() {
                        @Override
                        public void onFileDelete(File file) {
                            handlePollFileEvent(file, StandardWatchEventKinds.ENTRY_DELETE);
                        }
                    });
                } else {
                    throw new IllegalArgumentException(
                            "DirWatchService passed unsupported watch event kind: " + kind.name());
                }
            }

            fileAlterationMonitor = new FileAlterationMonitor(pollIntervalMillis, fileAlterationObserver);
            fileAlterationMonitorStarted = false;
        }

        @Override
        public synchronized void start() throws Exception {
            if (fileAlterationMonitorStarted) {
                throw new IllegalStateException("Trying to start an already-started DirWatchService!");
            }
            fileAlterationMonitor.start();
            fileAlterationMonitorStarted = true;
        }

        @Override
        public synchronized void stop() throws Exception {
            if (fileAlterationMonitorStarted) {
                fileAlterationMonitor.stop();
                fileAlterationMonitorStarted = false;
            }
        }

        private void handlePollFileEvent(final File file, final WatchEvent.Kind kind) {
            final String fileName = file.getName();
            handleFileEvent(fileName, kind);
        }
    }

    /** For exact-match hash comparisons. */
    private final List<SeparatorToExactMatchWatchersPair> separatorToExactMatchWatchers;
    /** For in-order non-exact-match comparisons. */
    private final Deque<FilteringFileWatcher> filteringFileWatchers;

    private final Consumer<ExceptionConsumerParameter> exceptionConsumer;
    private final Path keyDir;

    private final WatcherInterface watcherImpl;

    /**
     * Constructor to create a directory watch service. This initializes the instance but doesn't add any watch
     * patterns, and doesn't start the watch thread.
     *
     * @param dirToWatch Directory to watch for changes
     * @param exceptionConsumer Consumer to accept exceptions if they occur. Even if the watch service has terminated,
     *        stop() should be called before restarting it. The exceptionConsumer must accept two arguments - the
     *        Exception generated, and a boolean which specifies whether the WatchService has terminated as a result of
     *        the exception.
     * @param watchServiceType the watch service type, from the WatchServiceType enum
     * @param pollIntervalMillis for a poll service, the interval between polls
     * @param kinds The kinds of events that may need to be watched from java.nio.file.StandardWatchEventKinds, valid
     *        options are: ENTRY_CREATE, ENTRY_DELETE, and ENTRY_MODIFY
     */
    @SuppressWarnings("WeakerAccess")
    public DirWatchService(@NotNull final String dirToWatch,
            @NotNull final Consumer<ExceptionConsumerParameter> exceptionConsumer,
            @NotNull final WatchServiceType watchServiceType,
            final long pollIntervalMillis,
            @NotNull final WatchEvent.Kind... kinds) {
        separatorToExactMatchWatchers = new ArrayList<>();
        filteringFileWatchers = new ArrayDeque<>();

        this.exceptionConsumer = exceptionConsumer;

        keyDir = Paths.get(dirToWatch);

        switch (watchServiceType) {
            case POLLWATCHSERVICE:
                watcherImpl = new WatcherInterfacePollImpl(pollIntervalMillis, dirToWatch, kinds);
                break;

            case JAVAWATCHSERVICE:
                watcherImpl = new WatcherInterfaceJavaImpl(dirToWatch, kinds);
                break;

            default:
                throw new IllegalArgumentException("Unknown watch service type: " + watchServiceType);
        }
    }

    // If the exception consumer generates an exception, wrap it; this will terminate the watch service.
    private void callExceptionConsumer(final Exception e, final Boolean watchServiceTerminated) {
        try {
            exceptionConsumer.accept(new ExceptionConsumerParameter(e, watchServiceTerminated, keyDir));
        } catch (Exception e2) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds an exact match file pattern to watch for; equivalent to adding with the EXACT_MATCH_WITH_SEPARATOR
     * MatcherType
     * 
     * @param filePattern The exact file pattern to watch for (i.e. the part before the separator must match this)
     * @param consumer The consumer to be called when the pattern is matched
     */
    @SuppressWarnings("WeakerAccess")
    public synchronized void addExactFileWatch(@NotNull final String separator,
            @NotNull final String filePattern,
            @NotNull final BiConsumer<Path, WatchEvent.Kind> consumer) {
        final SeparatorToExactMatchWatchersPair pair = separatorToExactMatchWatchers.stream()
                .filter((p) -> p.fileSeparator.equals(separator))
                .findFirst()
                .orElseGet(() -> {
                    final SeparatorToExactMatchWatchersPair p = new SeparatorToExactMatchWatchersPair(separator);
                    separatorToExactMatchWatchers.add(p);
                    return p;
                });
        pair.addWatcher(filePattern, consumer);
    }

    /**
     * Adds a file pattern to watch for at the end of the ordered watch list
     *
     * @param matcher The filtering predicate, returns true for files that should be consumed
     * @param consumer Consumer to be called with the file and event type
     */
    @SuppressWarnings("unused")
    public synchronized void addFileWatchAtEnd(@NotNull final Predicate<String> matcher,
            @NotNull final BiConsumer<Path, WatchEvent.Kind> consumer) {
        filteringFileWatchers.addLast(new FilteringFileWatcher(consumer, matcher));
    }

    /**
     * Adds a file pattern to watch for at the start of the ordered watch list
     *
     * @param matcher The filtering predicate, returns true for files that should be consumed
     * @param consumer Consumer to be called with the file and event type
     */
    public synchronized void addFileWatchAtStart(@NotNull final Predicate<String> matcher,
            @NotNull final BiConsumer<Path, WatchEvent.Kind> consumer) {
        filteringFileWatchers.addFirst(new FilteringFileWatcher(consumer, matcher));
    }

    /**
     * Request that the fileWatcher thread stop. This will not return until the thread has stopped.
     */
    public void stop() throws Exception {
        watcherImpl.stop();
    }

    /**
     * Starts the watch service thread. Even if it's initially empty the service should start as it could get files
     * later.
     * 
     * @throws IOException from the Java watch service
     */
    public void start() throws Exception {
        watcherImpl.start();
    }

    /**
     * Check the exact matcher list for any matchers and call the consumer if one is found. Returns true if a match was
     * found.
     */
    private synchronized FileWatcher checkExactMatches(final String fileName) {
        for (final SeparatorToExactMatchWatchersPair pair : separatorToExactMatchWatchers) {
            final String fileSeparator = pair.fileSeparator;
            final Map<String, ExactMatchFileWatcher> exactMatchWatchers = pair.exactMatchWatchers;

            final int fileSeparatorIndex = fileName.indexOf(fileSeparator);
            if (fileSeparatorIndex > 0) {
                final String fileNameBeforeSeparator = fileName.substring(0, fileSeparatorIndex); // Must be non-zero
                                                                                                  // length because of
                                                                                                  // the above check
                final FileWatcher fileWatcher = exactMatchWatchers.get(fileNameBeforeSeparator);
                if (fileWatcher != null) {
                    return fileWatcher;
                }
            }
        }
        return null;
    }

    /**
     * Check the pattern match list and call the first one found, if any. Returns true if a match was found.
     */
    private synchronized FileWatcher checkPatternMatches(final String fileName) {
        for (FilteringFileWatcher fileWatcher : filteringFileWatchers) {
            if (fileWatcher.matches(fileName)) {
                return fileWatcher;
            }
        }
        return null;
    }

    private void handleFileEvent(final String fileName, final WatchEvent.Kind kind) {
        FileWatcher foundFileWatcher;
        synchronized (this) {
            // The first check is against the set of hash entries for the exact match - this requires a separator
            foundFileWatcher = checkExactMatches(fileName);

            // If an exact match wasn't found, then check for pattern matches
            if (foundFileWatcher == null) {
                foundFileWatcher = checkPatternMatches(fileName);
            }
        }

        // Consume after releasing the lock, to reduce potential lock contention
        if (foundFileWatcher != null) {
            Path fullPath = keyDir.resolve(fileName);
            foundFileWatcher.consume(fullPath, kind);
        }
    }

    @SuppressWarnings("WeakerAccess")
    public static Predicate<String> makeRegexMatcher(@NotNull final String regex) {
        final Pattern pattern = Pattern.compile(regex);
        return fileName -> pattern.matcher(fileName).matches();
    }

    @SuppressWarnings("WeakerAccess")
    public static Predicate<String> makeStartsWithMatcher(@NotNull final String prefix) {
        return fileName -> fileName.startsWith(prefix);
    }

    @SuppressWarnings("WeakerAccess")
    public static Predicate<String> makeEndsWithMatcher(@NotNull final String suffix) {
        return fileName -> fileName.endsWith(suffix);
    }
}
