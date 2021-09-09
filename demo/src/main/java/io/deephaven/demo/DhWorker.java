package io.deephaven.demo;

import io.smallrye.common.constraint.NotNull;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.util.Objects;

/**
 * DhWorker:
 * <p>
 * <p> Encapsulates the configuration information for a running pod + service + http route
 * <p>
 */
public class DhWorker {
    private final String userName;
    private final String podName;
    private final String serviceName;
    private final String ingressName;
    private final String dnsName;
    private boolean inUse;
    private boolean ready;
    private boolean destroyed;
    private long createdTime;

    public DhWorker(@NotNull final String userName,
                    @NotNull final String podName,
                    @NotNull final String serviceName,
                    @NotNull final String ingressName,
                    @NotNull final String dnsName) {
        this.userName = userName;
        this.podName = podName;
        this.serviceName = serviceName;
        this.ingressName = ingressName;
        this.dnsName = dnsName;
    }

    public String getDnsName() {
        return dnsName;
    }

    public String getPodName() {
        return podName;
    }

    public String getServiceName() {
        return serviceName;
    }

    public String getUserName() {
        return userName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final DhWorker dhWorker = (DhWorker) o;
        return podName.equals(dhWorker.podName) && dnsName.equals(dhWorker.dnsName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(podName, dnsName);
    }

    public void setInUse(final boolean inUse) {
        // TODO: force this to actually update kubernetes metadata on the pod itself
        this.inUse = inUse;
    }

    public boolean isInUse() {
        return inUse;
    }

    public void setReady(final boolean ready) {
        this.ready = ready;
    }

    public boolean isReady() {
        return ready;
    }

    public boolean checkReady(final OkHttpClient client) {
        // now, we have an entry for the worker, make sure it exists!
        // fastest way to do this, ping the worker's /health uri...
        String uri = "https://" + getDnsName() + "/health";
        final Request req = new Request.Builder().get()
                .url(uri)
                .build();
        final Response response;
        try {
            response = client.newCall(req).execute();
        } catch (IOException e) {
            return false;
        }
        if (response.isSuccessful()) {
            setReady(true);
            return true;
        }
        return false;
    }

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(final long createdTime) {
        this.createdTime = createdTime;
    }

    public boolean isDestroyed() {
        return destroyed;
    }

    public void setDestroyed(final boolean destroyed) {
        this.destroyed = destroyed;
    }

    @Override
    public String toString() {
        return "DhWorker{" +
                "userName='" + userName + '\'' +
                ", podName='" + podName + '\'' +
                ", dnsName='" + dnsName + '\'' +
                ", ingressName='" + ingressName + '\'' +
                ", inUse=" + inUse +
                ", ready=" + ready +
                ", destroyed=" + destroyed +
                ", createdTime=" + createdTime +
                '}';
    }

    public String getIngressName() {
        return ingressName;
    }
}
