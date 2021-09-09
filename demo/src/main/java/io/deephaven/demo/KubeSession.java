package io.deephaven.demo;

import io.kubernetes.client.openapi.models.V1Ingress;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.openapi.models.V1Service;

import java.util.Objects;

/**
 * KubeSession: A group of kubernetes api objects describing a single user session.
 * <p>
 */
public class KubeSession {
    private final String userName;
    private V1Pod pod;
    private V1Service service;
    private V1Ingress ingress;

    public KubeSession(String userName) {
        this.userName = userName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final KubeSession that = (KubeSession) o;
        return userName.equals(that.userName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userName);
    }

    public String getUserName() {
        return userName;
    }

    public V1Pod getPod() {
        return pod;
    }

    public void setPod(final V1Pod pod) {
        this.pod = pod;
    }

    public V1Service getService() {
        return service;
    }

    public void setService(final V1Service service) {
        this.service = service;
    }

    public V1Ingress getIngress() {
        return ingress;
    }

    public void setIngress(final V1Ingress ingress) {
        this.ingress = ingress;
    }

    public DhWorker asWorker() {
        String podName = "unknown";
        try {
            podName = getPod().getMetadata().getName();
        } catch (Exception e) {
            System.err.println("[ERROR] Failed getting pod name for " + userName);
            e.printStackTrace();
        }
        String serviceName = "unknown";
        try {
            serviceName = getService().getMetadata().getName();
        } catch (Exception e) {
            System.err.println("[ERROR] Failed getting service name for " + userName);
            e.printStackTrace();
        }
        String ingressName = "unknown";
        try {
            ingressName = getIngress().getMetadata().getName();
        } catch (Exception e) {
            System.err.println("[ERROR] Failed getting ingress name for " + userName);
            e.printStackTrace();
        }

        return new DhWorker(
                getUserName(), podName, serviceName, ingressName,
                getUserName() + "." + NameConstants.DOMAIN

        );
    }

    public boolean isComplete() {
        return pod != null && service != null && ingress != null;
    }

    @Override
    public String toString() {
        return "KubeSession{" +
                "userName='" + userName + '\'' +
                ", pod=" + (pod == null ? null : pod.getMetadata().getName()) +
                ", service=" + (service == null ? null : service.getMetadata().getName()) +
                ", ingress=" + (ingress == null ? null : ingress.getMetadata().getName()) +
                '}';
    }
}
