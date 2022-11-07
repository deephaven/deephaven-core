package io.deephaven.app;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.qst.type.Type;
import io.deephaven.stream.StreamConsumer;
import io.deephaven.stream.StreamPublisher;
import io.deephaven.stream.StreamToTableAdapter;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.lang.Thread.State;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class ThreadPublisher implements StreamPublisher {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofTime("Timestamp"),
            ColumnDefinition.ofLong("Id"),
            ColumnDefinition.ofString("Name"),
            ColumnDefinition.of("State", Type.ofCustom(State.class)),
            ColumnDefinition.ofLong("BlockedCount"),
            ColumnDefinition.ofLong("BlockedDuration"),
            ColumnDefinition.ofLong("WaitedCount"),
            ColumnDefinition.ofLong("WaitedDuration"),
            ColumnDefinition.ofBoolean("IsInNative"),
            ColumnDefinition.ofBoolean("IsDaemon"),
            ColumnDefinition.ofInt("Priority"),
            ColumnDefinition.ofLong("UserTime"),
            ColumnDefinition.ofLong("SystemTime"),
            ColumnDefinition.ofLong("AllocatedBytes"));

    private static final int CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public static TableDefinition definition() {
        return DEFINITION;
    }

    private final Map<Long, long[]> map;
    private WritableChunk<Values>[] chunks;
    private StreamConsumer consumer;

    public ThreadPublisher() {
        map = new HashMap<>();
        // noinspection unchecked
        chunks = StreamToTableAdapter.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
        init(true, true, true);
    }

    private static void init(boolean contentionMonitoring, boolean cpuTime, boolean allocatedMemory) {
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        if (contentionMonitoring && threadMXBean.isThreadContentionMonitoringSupported()) {
            threadMXBean.setThreadContentionMonitoringEnabled(true);
        }
        if (cpuTime && threadMXBean.isThreadCpuTimeSupported()) {
            threadMXBean.setThreadCpuTimeEnabled(true);
        }
        if (allocatedMemory
                && (threadMXBean instanceof com.sun.management.ThreadMXBean)
                && ((com.sun.management.ThreadMXBean) threadMXBean).isThreadAllocatedMemorySupported()) {
            ((com.sun.management.ThreadMXBean) threadMXBean).setThreadAllocatedMemoryEnabled(true);
        }
    }

    @Override
    public void register(@NotNull StreamConsumer consumer) {
        if (this.consumer != null) {
            throw new IllegalStateException("Can not register multiple StreamConsumers.");
        }
        this.consumer = Objects.requireNonNull(consumer);
    }

    @Override
    public synchronized void flush() {
        if (chunks[0].size() == 0) {
            return;
        }
        flushInternal();
    }

    private void flushInternal() {
        consumer.accept(chunks);
        // noinspection unchecked
        chunks = StreamToTableAdapter.makeChunksForDefinition(DEFINITION, CHUNK_SIZE);
    }

    public synchronized void measure() {
        final ThreadMXBean jvm = ManagementFactory.getThreadMXBean();
        final long nowNanos = System.currentTimeMillis() * 1_000_000;
        final ThreadInfo[] threadInfos = jvm.dumpAllThreads(false, false, 0);
        // Add all of the extra info ASAP since it's not concurrent with dumpAllThreads
        addExtras(jvm, threadInfos);
        for (ThreadInfo threadInfo : threadInfos) {
            final long id = threadInfo.getThreadId();
            final long[] previous = Objects.requireNonNull(map.get(id));
            final String name = threadInfo.getThreadName();
            final State state = threadInfo.getThreadState();
            final long blockedTime = threadInfo.getBlockedTime(); // -1
            final long blockedCount = threadInfo.getBlockedCount();
            final long waitedTime = threadInfo.getWaitedTime(); // -1
            final long waitedCount = threadInfo.getWaitedCount();
            final boolean isInNative = threadInfo.isInNative();
            final boolean isDaemon = threadInfo.isDaemon();
            final int priority = threadInfo.getPriority();

            chunks[0].asWritableLongChunk().add(nowNanos);
            chunks[1].asWritableLongChunk().add(id);
            chunks[2].<String>asWritableObjectChunk().add(name);
            chunks[3].<State>asWritableObjectChunk().add(state);
            chunks[4].asWritableLongChunk().add(blockedCount - previous[1]);
            chunks[5].asWritableLongChunk().add(diff(previous[0], blockedTime));
            chunks[6].asWritableLongChunk().add(waitedCount - previous[3]);
            chunks[7].asWritableLongChunk().add(diff(previous[2], waitedTime));
            // todo: how to do boolean better?
            chunks[8].asWritableByteChunk().add(isInNative ? (byte) 1 : (byte) 0);
            chunks[9].asWritableByteChunk().add(isDaemon ? (byte) 1 : (byte) 0);
            chunks[10].asWritableIntChunk().add(priority);

            previous[0] = blockedTime;
            previous[1] = blockedCount;
            previous[2] = waitedTime;
            previous[3] = waitedCount;

            if (chunks[0].size() == CHUNK_SIZE) {
                flushInternal();
            }
        }

        if (map.size() != threadInfos.length) {
            // Remove dead threads
            final Set<Long> liveIds = Stream.of(threadInfos)
                    .map(ThreadInfo::getThreadId)
                    .collect(Collectors.toSet());
            map.keySet()
                    .removeIf(k -> !liveIds.contains(k));
        }
    }

    private void addExtras(ThreadMXBean jvm, ThreadInfo[] threadInfos) {
        if (jvm instanceof com.sun.management.ThreadMXBean) {
            addComSunExtras((com.sun.management.ThreadMXBean) jvm, threadInfos);
        } else {
            addJavaLangExtras(jvm, threadInfos);
        }
    }

    private void addJavaLangExtras(ThreadMXBean jvm, ThreadInfo[] threadInfos) {
        if (jvm.isThreadCpuTimeSupported() && jvm.isThreadCpuTimeEnabled()) {
            for (ThreadInfo threadInfo : threadInfos) {
                final long id = threadInfo.getThreadId();
                final long userTime = jvm.getThreadUserTime(id);
                final long cpuTime = jvm.getThreadCpuTime(id);
                addExtras(id, userTime, cpuTime, -1);
            }
            return;
        }
        for (ThreadInfo threadInfo : threadInfos) {
            final long id = threadInfo.getThreadId();
            addExtras(id, -1, -1, -1);
        }
    }

    private void addComSunExtras(com.sun.management.ThreadMXBean sun, ThreadInfo[] threadInfos) {
        final long[] threadIds = Stream.of(threadInfos).mapToLong(ThreadInfo::getThreadId).toArray();
        final long[] userTimes;
        final long[] cpuTimes;
        final long[] allocatedBytes;
        if (sun.isThreadCpuTimeSupported() && sun.isThreadCpuTimeEnabled()) {
            userTimes = sun.getThreadUserTime(threadIds);
            cpuTimes = sun.getThreadCpuTime(threadIds);
        } else {
            userTimes = null;
            cpuTimes = null;
        }
        if (sun.isThreadAllocatedMemorySupported() && sun.isThreadAllocatedMemoryEnabled()) {
            allocatedBytes = sun.getThreadAllocatedBytes(threadIds);
        } else {
            allocatedBytes = null;
        }
        for (int i = 0; i < threadIds.length; ++i) {
            addExtras(
                    threadIds[i],
                    userTimes == null ? -1 : userTimes[i],
                    cpuTimes == null ? -1 : cpuTimes[i],
                    allocatedBytes == null ? -1 : allocatedBytes[i]);
        }
    }

    private void addExtras(long id, long userTime, long cpuTime, long allocatedBytes) {
        final long systemTime = cpuTime == -1 || userTime == -1 ? -1 : cpuTime - userTime;
        final long[] previous = map.computeIfAbsent(id, k -> new long[7]);
        chunks[11].asWritableLongChunk().add(diff(previous[4], userTime));
        chunks[12].asWritableLongChunk().add(diff(previous[5], systemTime));
        chunks[13].asWritableLongChunk().add(diff(previous[6], allocatedBytes));
        previous[4] = userTime;
        previous[5] = systemTime;
        previous[6] = allocatedBytes;
    }

    private static long diff(long previous, long current) {
        return previous == -1 || current == -1 ? QueryConstants.NULL_LONG : current - previous;
    }
}
