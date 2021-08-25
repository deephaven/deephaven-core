package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import java.util.Optional;
import org.immutables.value.Value;
import oshi.software.os.OperatingSystem.OSVersionInfo;

@Value.Immutable
@ProcessStyle
public abstract class OperatingSystemVersionOshi implements PropertySet {

    private static final String VERSION = "version";
    private static final String NAME = "name";
    private static final String BUILD = "build";

    /**
     * Gets the operating system version.
     *
     * @return The version, if any.
     */
    @Value.Parameter
    public abstract Optional<String> getVersion();

    /**
     * Gets the operating system codename.
     *
     * @return The code name, if any.
     */
    @Value.Parameter
    public abstract Optional<String> getCodeName();

    /**
     * Gets the operating system build number.
     *
     * @return The build number, if any.
     */
    @Value.Parameter
    public abstract Optional<String> getBuildNumber();

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.maybeVisit(VERSION, getVersion());
        visitor.maybeVisit(NAME, getCodeName());
        visitor.maybeVisit(BUILD, getBuildNumber());
    }

    public static OperatingSystemVersionOshi from(OSVersionInfo info) {
        return ImmutableOperatingSystemVersionOshi.builder()
                .version(Optional.ofNullable(info.getVersion()))
                .codeName(Optional.ofNullable(info.getCodeName()))
                .buildNumber(Optional.ofNullable(info.getBuildNumber()))
                .build();
    }
}
