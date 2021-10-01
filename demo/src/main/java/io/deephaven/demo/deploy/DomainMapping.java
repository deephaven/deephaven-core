package io.deephaven.demo.deploy;

import io.deephaven.demo.NameConstants;

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

    public DomainMapping(final String name, final String domainRoot) {
        this.name = name;
        this.domainRoot = domainRoot == null ? NameConstants.DOMAIN : domainRoot;
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
        return Objects.hash(name, domainRoot);
    }

    @Override
    public String toString() {
        return getDomainQualified();
    }
}
