/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@ProcessStyle
public abstract class ProcessInfo implements PropertySet {

    private static final int VERSION = 3;

    private static final String META_ID = "meta.id";
    private static final String META_VERSION = "meta.version";

    private static final String RUNTIME_MX = "runtime-mx";
    private static final String ENVIRONMENT = "env-var";
    private static final String THREAD_MX = "thread-mx";
    private static final String MEMORY_MX = "memory-mx";
    private static final String MEMORY_POOL_MX = "memory-pool-mx";
    private static final String APP_ARGS = "app-args";
    private static final String APP_CONFIG = "app-config";
    private static final String HOST_PATH_INFO = "host-path-info";
    private static final String SYSTEM_INFO = "system-info";

    @Value.Parameter
    public abstract ProcessUniqueId getId();

    @Value.Parameter
    public abstract RuntimeMxBeanInfo getRuntimeInfo();

    @Value.Parameter
    public abstract EnvironmentVariables getEnvironmentVariables();

    @Value.Parameter
    public abstract ThreadMxBeanInfo getThreadInfo();

    @Value.Parameter
    public abstract MemoryMxBeanInfo getMemoryInfo();

    @Value.Parameter
    public abstract MemoryPoolsMxBeanInfo getMemoryPoolsInfo();

    @Value.Parameter
    public abstract ApplicationArguments getApplicationArguments();

    @Value.Parameter
    public abstract ApplicationConfig getApplicationConfig();

    @Value.Parameter
    public abstract HostPathInfo getHostPathInfo();

    @Value.Parameter
    public abstract Optional<SystemInfoOshi> getSystemInfo();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visit(META_ID, getId().value());
        visitor.visit(META_VERSION, VERSION);
        visitor.visitProperties(RUNTIME_MX, getRuntimeInfo());
        visitor.visitProperties(ENVIRONMENT, getEnvironmentVariables());
        visitor.visitProperties(THREAD_MX, getThreadInfo());
        visitor.visitProperties(MEMORY_MX, getMemoryInfo());
        visitor.visitProperties(MEMORY_POOL_MX, getMemoryPoolsInfo());
        visitor.visitProperties(APP_ARGS, getApplicationArguments());
        visitor.visitProperties(APP_CONFIG, getApplicationConfig());
        visitor.visitProperties(HOST_PATH_INFO, getHostPathInfo());
        visitor.maybeVisitProperties(SYSTEM_INFO, getSystemInfo());
    }
}
