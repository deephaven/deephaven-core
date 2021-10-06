package io.deephaven.demo.deploy;

import io.smallrye.common.constraint.NotNull;
import io.smallrye.common.constraint.Nullable;

import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * IpMapping:
 * <p>
 * <p> Represents a *named* IP address, identified / hashed by {@link #getName()} only.
 * <p> This lets us create these object before we've actually allocated any IP address resources.
 */
public class IpMapping implements Comparable<IpMapping> {

    private static final Clock UTC_CLOCK = Clock.systemUTC();
    private String ip;
    private String name;
    private volatile IpState state;
    private volatile Optional<Machine> instance;
    private volatile Instant lastUsed;

    public IpMapping(@NotNull String name, @Nullable String ip) {
        this.name = name;
        if (name == null) {
            throw new NullPointerException("Name cannot be null");
        }
        this.ip = ip;
        this.state = IpState.Unverified;
        this.instance = Optional.empty();
        this.lastUsed = Instant.now(UTC_CLOCK);
    }

    @Nullable
    public String getIp() {
        return ip;
    }

    @NotNull
    public String getName() {
        return name;
    }

    @NotNull
    public IpState getState() {
        return state;
    }

    /**
     * @return a non-empty optional only when {@link #getState()} returns either:
     * {@link IpState#Running}, {@link IpState#Claimed} or {@link IpState#Released}.
     * <p>
     * <p> The optional should be empty in all other cases.
     */
    @NotNull
    public Optional<Machine> getInstance() {
        return instance;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final IpMapping that = (IpMapping) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "ReservedIp{" +
                "ip='" + ip + '\'' +
                ", name='" + name + '\'' +
                ", state=" + state +
                ", instance=" + instance.orElse(null) +
                '}';
    }

    public void setIp(final String ip) {
        this.ip = ip;
    }

    public void setInstance(final Machine node) {
        this.instance = Optional.ofNullable(node);
        if (node != null) {
            lastUsed = Instant.now(UTC_CLOCK);
        }
    }

    public void setState(@NotNull final IpState state) {
        this.state = state;
    }

    public boolean isRunningFor(final Machine node) {
        return node != null && getState() == IpState.Running && instance.orElse(null) == node;
    }

    @Override
    public int compareTo(IpMapping other) {
        if (name.equals(other.name)) {
            // only consider name, not IP. IP will get reused.
            return 0;
        }
        return lastUsed.compareTo(other.lastUsed);
    }
}
