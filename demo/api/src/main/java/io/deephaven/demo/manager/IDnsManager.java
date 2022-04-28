package io.deephaven.demo.manager;

import java.io.IOException;

import io.deephaven.demo.api.DomainMapping;

/**
 * IDnsManager:
 * <p>
 * <p> This interface serves to expose any DnsManager methods we use as public API
 */
public interface IDnsManager {

    void tx(DnsTransaction tx);

    interface DnsChange {
        void addRecord(DomainMapping domain, final String ip) throws IOException, InterruptedException;
        void removeRecord(DomainMapping domain, final String resolved) throws IOException, InterruptedException;
    }
    interface DnsTransaction {
        void mutate(DnsChange dns) throws Exception;
    }
}
