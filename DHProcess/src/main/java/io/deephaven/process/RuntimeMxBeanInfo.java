package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import java.lang.management.RuntimeMXBean;
import org.immutables.value.Value;

@Value.Immutable
@ProcessStyle
public abstract class RuntimeMxBeanInfo implements PropertySet {

    private static final String SYS_PROPS = "sys-props";
    private static final String JVM_ARGS = "jvm-args";
    private static final String MANAGEMENT_SPEC_VERSION = "management-spec-version";
    private static final String BOOT_CLASS_PATH_SUPPORTED = "boot-class-path-supported";
    private static final String START_TIME = "start-time";

    public static RuntimeMxBeanInfo of(RuntimeMXBean bean) {
        return ImmutableRuntimeMxBeanInfo.builder()
                .systemProperties(_SystemProperties.of(bean))
                .jvmArguments(_JvmArguments.of(bean))
                .managementSpecVersion(bean.getManagementSpecVersion())
                .isBootClassPathSupported(bean.isBootClassPathSupported())
                .startTime(bean.getStartTime())
                .build();
    }

    @Value.Parameter
    public abstract SystemProperties getSystemProperties();

    @Value.Parameter
    public abstract JvmArguments getJvmArguments();

    @Value.Parameter
    public abstract String getManagementSpecVersion();

    @Value.Parameter
    public abstract boolean isBootClassPathSupported();

    @Value.Parameter
    public abstract long getStartTime();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visitProperties(SYS_PROPS, getSystemProperties());
        visitor.visitProperties(JVM_ARGS, getJvmArguments());
        visitor.visit(MANAGEMENT_SPEC_VERSION, getManagementSpecVersion());
        visitor.visit(BOOT_CLASS_PATH_SUPPORTED, isBootClassPathSupported());
        visitor.visit(START_TIME, getStartTime());
    }
}
