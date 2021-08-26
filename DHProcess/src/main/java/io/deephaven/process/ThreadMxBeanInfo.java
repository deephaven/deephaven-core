package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import java.lang.management.ThreadMXBean;
import org.immutables.value.Value;

@Value.Immutable
@ProcessStyle
public abstract class ThreadMxBeanInfo implements PropertySet {

    private static final String CURRENT_THREAD_CPU_TIME_SUPPORTED = "current-thread-cpu-time-supported";
    private static final String OBJECT_MONITOR_USAGE_SUPPORTED = "object-monitor-usage-supported";
    private static final String SYNCHRONIZER_USAGE_SUPPORTED = "synchronizer-usage-supported";
    private static final String THREAD_CONTENTION_MONITORING_SUPPORTED = "thread-contention-monitoring-supported";
    private static final String THREAD_CPU_TIME_SUPPORTED = "thread-cpu-time-supported";

    public static ThreadMxBeanInfo of(ThreadMXBean bean) {
        return ImmutableThreadMxBeanInfo.builder()
                .isCurrentThreadCpuTimeSupported(bean.isCurrentThreadCpuTimeSupported())
                .isObjectMonitorUsageSupported(bean.isObjectMonitorUsageSupported())
                .isSynchronizerUsageSupported(bean.isSynchronizerUsageSupported())
                .isThreadContentionMonitoringSupported(bean.isThreadContentionMonitoringSupported())
                .isThreadCpuTimeSupported(bean.isThreadCpuTimeSupported())
                .build();
    }

    @Value.Parameter
    public abstract boolean isCurrentThreadCpuTimeSupported();

    @Value.Parameter
    public abstract boolean isObjectMonitorUsageSupported();

    @Value.Parameter
    public abstract boolean isSynchronizerUsageSupported();

    @Value.Parameter
    public abstract boolean isThreadContentionMonitoringSupported();

    @Value.Parameter
    public abstract boolean isThreadCpuTimeSupported();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visit(CURRENT_THREAD_CPU_TIME_SUPPORTED, isCurrentThreadCpuTimeSupported());
        visitor.visit(OBJECT_MONITOR_USAGE_SUPPORTED, isObjectMonitorUsageSupported());
        visitor.visit(SYNCHRONIZER_USAGE_SUPPORTED, isSynchronizerUsageSupported());
        visitor.visit(THREAD_CONTENTION_MONITORING_SUPPORTED, isThreadContentionMonitoringSupported());
        visitor.visit(THREAD_CPU_TIME_SUPPORTED, isThreadCpuTimeSupported());
    }
}
