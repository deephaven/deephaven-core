package io.deephaven.demo.deploy;

import io.deephaven.demo.NameConstants;
import io.deephaven.demo.NameGen;
import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;

import java.util.Objects;

/**
 * DomainMapping:
 * <p>
 * <p>
 * <p> A class representing a unique fully-qualified.domain.name.
 * <p>
 * <p> References the {@link IpMapping} which backs our DNS A record to a real IP address.
 */
public class DomainMapping {

    private final String name;
    private final String domainRoot;
    private volatile long mark;

    public DomainMapping(@NotNull final String name, @Nullable final String domainRoot) {
        this.name = name;
        this.domainRoot = domainRoot == null ? NameConstants.DOMAIN : domainRoot;
        NameGen.reserveName(name);
    }

    public String getName() {
        return name;
    }

    public String getDomainQualified() {
        return getName() + "." + getDomainRoot();
    }

    public String getDomainRoot() {
        return domainRoot;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DomainMapping that = (DomainMapping) o;
        return name.equals(that.name) && domainRoot.equals(that.domainRoot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, domainRoot) ^ NameGen.getLocalHash();
    }

    @Override
    public String toString() {
        return getDomainQualified();
    }

    public long getMark() {
        return mark;
    }

    public void setMark(final long mark) {
        this.mark = mark;
    }
}
