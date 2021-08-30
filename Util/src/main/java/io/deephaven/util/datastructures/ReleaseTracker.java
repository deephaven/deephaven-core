package io.deephaven.util.datastructures;

import io.deephaven.configuration.Configuration;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.*;

/**
 * Instrumentation tool for detecting missing resource releases.
 */
public interface ReleaseTracker<RESOURCE_TYPE> {
    boolean CAPTURE_STACK_TRACES = Configuration.getInstance().getBooleanForClassWithDefault(ReleaseTracker.class,
            "captureStackTraces", false);

    void reportAcquire(@NotNull final RESOURCE_TYPE resource);

    void reportRelease(@NotNull final RESOURCE_TYPE resource);

    void check();

    interface Factory {
        ReleaseTracker makeReleaseTracker();

        boolean isMyType(Class type);
    }

    Factory strictReleaseTrackerFactory = new Factory() {
        @Override
        public ReleaseTracker makeReleaseTracker() {
            return new StrictReleaseTracker();
        }

        @Override
        public boolean isMyType(final Class type) {
            return type == StrictReleaseTracker.class;
        }
    };

    Factory weakReleaseTrackerFactory = new Factory() {
        @Override
        public ReleaseTracker makeReleaseTracker() {
            return new WeakReleaseTracker();
        }

        @Override
        public boolean isMyType(final Class type) {
            return type == WeakReleaseTracker.class;
        }
    };

    class StrictReleaseTracker<RESOURCE_TYPE> implements ReleaseTracker<RESOURCE_TYPE> {
        private static final StackTraceElement[] ZERO_ELEMENT_STACK_TRACE_ARRAY = new StackTraceElement[0];

        private final Map<RESOURCE_TYPE, StackTraceElement[]> lastAcquireMap = new HashMap<>();

        private static final class LastAcquireAndReleaseInfo {

            private final StackTraceElement[] lastAcquire;
            private final StackTraceElement[] lastRelease;

            private LastAcquireAndReleaseInfo(final StackTraceElement[] lastAcquire,
                    final StackTraceElement[] lastRelease) {
                this.lastAcquire = lastAcquire;
                this.lastRelease = lastRelease;
            }
        }

        private final Map<RESOURCE_TYPE, LastAcquireAndReleaseInfo> lastAcquireAndReleaseMap = new WeakHashMap<>();

        public final void reportAcquire(@NotNull final RESOURCE_TYPE resource) {
            final StackTraceElement[] stackTrace =
                    CAPTURE_STACK_TRACES ? Thread.currentThread().getStackTrace() : ZERO_ELEMENT_STACK_TRACE_ARRAY;
            synchronized (this) {
                final StackTraceElement[] prev = lastAcquireMap.put(resource, stackTrace);
                if (prev != null) {
                    throw new AlreadyAcquiredException(stackTrace, prev);
                }
                lastAcquireAndReleaseMap.remove(resource);
            }
        }

        public final void reportRelease(@NotNull final RESOURCE_TYPE resource) {
            final StackTraceElement[] stackTrace =
                    CAPTURE_STACK_TRACES ? Thread.currentThread().getStackTrace() : ZERO_ELEMENT_STACK_TRACE_ARRAY;
            synchronized (this) {
                final StackTraceElement[] prev = lastAcquireMap.remove(resource);
                if (prev != null) {
                    lastAcquireAndReleaseMap.put(resource, new LastAcquireAndReleaseInfo(prev, stackTrace));
                    return;
                }
                final LastAcquireAndReleaseInfo lastAcquireAndRelease = lastAcquireAndReleaseMap.get(resource);
                if (lastAcquireAndRelease != null) {
                    throw new AlreadyReleasedException(stackTrace, lastAcquireAndRelease.lastAcquire,
                            lastAcquireAndRelease.lastRelease);
                }
                throw new UnmatchedAcquireException(stackTrace);
            }
        }

        public final void check() {
            synchronized (this) {
                final int leakedCount = lastAcquireMap.size();
                if (leakedCount > 0) {
                    final LeakedException leakedException;
                    if (CAPTURE_STACK_TRACES) {
                        leakedException = new LeakedException(lastAcquireMap.values());
                    } else {
                        leakedException = new LeakedException(leakedCount);
                    }
                    lastAcquireMap.clear();
                    throw leakedException;
                }
            }
        }
    }

    class WeakReleaseTracker<RESOURCE_TYPE> implements ReleaseTracker<RESOURCE_TYPE> {
        private final Map<RESOURCE_TYPE, Cookie> outstandingCookies = Collections.synchronizedMap(new WeakHashMap<>());
        private final ReferenceQueue<RESOURCE_TYPE> collectedCookies = new ReferenceQueue<>();

        public final void check() {
            System.gc();
            Cookie cookie;
            // noinspection unchecked
            while ((cookie = (Cookie) collectedCookies.poll()) != null) {
                cookie.collected();
            }
            synchronized (outstandingCookies) {
                outstandingCookies.values().forEach(Cookie::presentOnCheck);
                outstandingCookies.clear();
            }
        }

        public final void reportAcquire(@NotNull final RESOURCE_TYPE resource) {
            outstandingCookies.put(resource, new Cookie(resource));
        }

        public final void reportRelease(@NotNull final RESOURCE_TYPE resource) {
            outstandingCookies.remove(resource).released();
        }

        private final class Cookie extends WeakReference<RESOURCE_TYPE> {

            private UnmatchedAcquireException pendingAcquireException = new UnmatchedAcquireException();
            private boolean released = false;

            private Cookie(@NotNull final RESOURCE_TYPE referent) {
                super(referent, collectedCookies);
            }

            private synchronized void released() {
                released = true;
                pendingAcquireException = null;
            }

            private synchronized void collected() {
                if (!released) {
                    throw new LeakedException(pendingAcquireException);
                }
            }

            private synchronized void presentOnCheck() {
                try {
                    throw new MissedReleaseException(pendingAcquireException);
                } finally {
                    released();
                }
            }
        }
    }

    class UnmatchedAcquireException extends RuntimeException {

        private UnmatchedAcquireException() {
            super("Resource lastAcquireMap without release");
        }

        private static String builder(@NotNull final StackTraceElement[] release) {
            final StringBuilder sb = new StringBuilder("Release with no prior acquire:\n");
            append(sb, "    ", release);
            return sb.toString();
        }

        private UnmatchedAcquireException(@NotNull final StackTraceElement[] release) {
            super(builder(release));
        }
    }

    class MissedReleaseException extends RuntimeException {
        private MissedReleaseException(@NotNull final UnmatchedAcquireException cause) {
            super("Missed release for resource", cause);
        }
    }

    static void append(final StringBuilder sb, final String prefix, final StackTraceElement[] es) {
        for (int i = 1; i < es.length; ++i) {
            sb.append(prefix).append(es[i].toString()).append("\n");
        }
    }

    class LeakedException extends RuntimeException {

        private LeakedException(@NotNull final UnmatchedAcquireException cause) {
            super("Leaked resource", cause);
        }

        private static String build(@NotNull final Collection<StackTraceElement[]> leaks) {
            final long maxUniqueTraces = 100;

            final StringBuilder stackTrace = new StringBuilder();
            final LinkedHashMap<String, Long> dupDetector = new LinkedHashMap<>();

            for (StackTraceElement[] leak : leaks) {
                stackTrace.setLength(0);
                append(stackTrace, "        ", leak);
                final String stackTraceString = stackTrace.toString();
                dupDetector.put(stackTraceString, 1 + dupDetector.getOrDefault(stackTraceString, 0L));
            }

            final StringBuilder sb = new StringBuilder(
                    "Leaked " + leaks.size() + " resources (" + dupDetector.size() + " unique traces):\n");
            final MutableInt i = new MutableInt();
            dupDetector.entrySet().stream().limit(maxUniqueTraces).forEach(entry -> {
                sb.append("    Leak #").append(i.intValue());
                if (entry.getValue() > 0L) {
                    sb.append(", detected " + entry.getValue() + " times, was acquired:\n");
                } else {
                    sb.append(" was acquired:\n");
                }
                i.increment();;
                sb.append(entry.getKey());
            });

            if (dupDetector.size() > maxUniqueTraces) {
                sb.append("    [ ... ]");
            }

            return sb.toString();
        }

        private LeakedException(@NotNull final Collection<StackTraceElement[]> leaked) {
            super(build(leaked));
        }

        private LeakedException(final long numLeaks) {
            super("Leaked " + numLeaks + " resources. Enable `ReleaseTracker.captureStackTraces` to further debug.");
        }
    }

    class AlreadyAcquiredException extends RuntimeException {

        private static String build(
                @NotNull final StackTraceElement[] newAcquire,
                @NotNull final StackTraceElement[] existingAcquire) {
            if (newAcquire.length == 0) {
                return "Already acquired resource is being re-acquired without intervening release. Enable `ReleaseTracker.captureStackTraces` to further debug.";
            }

            final StringBuilder sb =
                    new StringBuilder("Already acquired resource is being re-acquired without intervening release:\n");
            sb.append("    New acquire:\n");
            append(sb, "        ", newAcquire);
            sb.append("    Existing acquire:\n");
            append(sb, "        ", existingAcquire);
            return sb.toString();
        }

        private AlreadyAcquiredException(
                @NotNull final StackTraceElement[] newAcquire,
                @NotNull final StackTraceElement[] existingAcquire) {
            super(build(newAcquire, existingAcquire));
        }
    }

    class AlreadyReleasedException extends RuntimeException {

        private static String build(@NotNull final StackTraceElement[] newRelease,
                @NotNull final StackTraceElement[] lastAcquire,
                @NotNull final StackTraceElement[] lastRelease) {
            if (newRelease.length == 0) {
                return "Already released resource is being re-released. Enable `ReleaseTracker.captureStackTraces` to further debug.";
            }
            final StringBuilder sb = new StringBuilder("Already released resource is being re-released:\n");
            sb.append("    New release:\n");
            append(sb, "        ", newRelease);
            sb.append("    Last release:\n");
            append(sb, "        ", lastRelease);
            sb.append("    Last acquire:\n");
            append(sb, "        ", lastAcquire);
            return sb.toString();
        }

        private AlreadyReleasedException(
                @NotNull final StackTraceElement[] newRelease,
                @NotNull final StackTraceElement[] lastAcquire,
                @NotNull final StackTraceElement[] lastRelease) {
            super(build(newRelease, lastAcquire, lastRelease));
        }
    }
}
