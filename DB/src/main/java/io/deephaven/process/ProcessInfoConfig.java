package io.deephaven.process;

import io.deephaven.configuration.Configuration;
import io.deephaven.process.ImmutableProcessInfo.Builder;
import io.deephaven.properties.SplayedPath;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;

public class ProcessInfoConfig {

    /**
     * The lookup key for {@link ProcessInfo#getId()}. If not present, will default to a random id.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String PROCESS_INFO_ID_KEY = "process.info.id";

    /**
     * The lookup key to see if {@link ProcessInfo#getSystemInfo()} is enabled. If not present, will
     * default to {@link #SYSTEM_INFO_ENABLED_DEFAULT}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String PROCESS_INFO_SYSTEM_INFO_ENABLED_KEY =
        "process.info.system-info.enabled";

    /**
     * The default value to see if {@link ProcessInfo#getSystemInfo()} is enabled.
     */
    @SuppressWarnings("WeakerAccess")
    public static final boolean SYSTEM_INFO_ENABLED_DEFAULT = true;

    /**
     * The lookup key for {@link ProcessInfo#getHostPathInfo()}. If not present, will default to
     * {@link #HOST_PATH_INFO_DIR_DEFAULT}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String HOST_PATH_INFO_DIR_KEY = "process.info.host-path-info.dir";

    /**
     * The default {@link SplayedPath} root for {@link ProcessInfo#getHostPathInfo()}.
     */
    @SuppressWarnings("WeakerAccess")
    public static final String HOST_PATH_INFO_DIR_DEFAULT = "/etc/sysconfig/deephaven.d/host";

    private static final UUID STATIC_UUID = UUID.randomUUID();
    private static final boolean TRIM = true;
    private static final boolean IS_VALUE_BASED = false;

    private static volatile ProcessUniqueId thisProcessId;

    @Nullable
    public static ProcessUniqueId getThisProcessId() {
        return thisProcessId;
    }

    @Nullable
    public static String getThisProcessIdValue() {
        final ProcessUniqueId localThisProcessId = thisProcessId;
        return localThisProcessId == null ? null : localThisProcessId.value();
    }

    public static synchronized ProcessInfo createForCurrentProcess(Configuration config)
        throws IOException {
        if (thisProcessId != null) {
            throw new IllegalStateException("ProcessInfo already created with ID " + thisProcessId);
        }
        final Path path = Paths
            .get(config.getStringWithDefault(HOST_PATH_INFO_DIR_KEY, HOST_PATH_INFO_DIR_DEFAULT));
        final SplayedPath hostPathSplayed = new SplayedPath(path, TRIM, IS_VALUE_BASED);
        final Builder builder = ImmutableProcessInfo.builder()
            .id(thisProcessId = ProcessUniqueId
                .of(config.getStringWithDefault(PROCESS_INFO_ID_KEY, STATIC_UUID.toString())))
            .runtimeInfo(RuntimeMxBeanInfo.of(ManagementFactory.getRuntimeMXBean()))
            .environmentVariables(EnvironmentVariables.of())
            .threadInfo(ThreadMxBeanInfo.of(ManagementFactory.getThreadMXBean()))
            .memoryInfo(MemoryMxBeanInfo.of(ManagementFactory.getMemoryMXBean()))
            .memoryPoolsInfo(MemoryPoolsMxBeanInfo.of(ManagementFactory.getMemoryPoolMXBeans()))
            .applicationArguments(ApplicationArguments.of(Collections.emptyList())) // blerg, todo
            .applicationConfig(ApplicationConfig.of(Collections.emptyMap())) // todo
            .hostPathInfo(_HostPathInfo.of(hostPathSplayed));
        if (config.getBooleanWithDefault(PROCESS_INFO_SYSTEM_INFO_ENABLED_KEY,
            SYSTEM_INFO_ENABLED_DEFAULT)) {
            builder.systemInfo(SystemInfoOshi.forCurrentProcess());
        }
        return builder.build();
    }
}
