package io.deephaven.demo.deploy;

import org.apache.commons.io.FileUtils;
import org.jboss.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.deephaven.demo.deploy.GoogleDeploymentManager.*;

/**
 * GoogleDnsManager:
 * <p>
 * <p> Google DNS does not like concurrent operations on the same domain.
 * <p> Thus, we must collect all DNS requests made into a single file,
 * <p> and process that file in a single big (off-caller-thread) transaction.
 */
public class GoogleDnsManager {

    private static final Logger LOG = Logger.getLogger(GoogleDnsManager.class);

    private final AtomicInteger txDepth = new AtomicInteger(0);
    private final AtomicInteger txCnt = new AtomicInteger(0);
    private final AtomicReference<Thread> dnsThread;
    private final File workDir;

    public GoogleDnsManager(final File workDir) {
        this.workDir = workDir;
        dnsThread = new AtomicReference<>();
    }

    interface DnsChange {
        void addRecord(DomainMapping domain, final String ip) throws IOException, InterruptedException;
        void removeRecord(DomainMapping domain, final String resolved) throws IOException, InterruptedException;
    }
    interface DnsTransaction {
        void mutate(DnsChange dns) throws Exception;
    }

    public void tx(DnsTransaction tx) {
        int id = txDepth.getAndIncrement();
        try {
            if (id == 0) {
                txCnt.incrementAndGet();
            }
            DnsChange change = startTx(id);
            tx.mutate(change);
        } catch (Exception e) {
            LOG.error("Failed while running user callback " + tx, e);
        } finally {
            if (txDepth.decrementAndGet() == 0) {
                commitTx();
            }
        }
    }

    protected void commitTx() {
        dnsThread.updateAndGet(t-> {
            if (t != null) {
                t.start();
            }
            return null;
        });
    }

    protected File dnsDir() {
        return new File(workDir.getParentFile(), "dns_" + txCnt.get());
    }

    protected DnsChange startTx(final int depth) {

        File dnsDir = dnsDir();
        if (depth == 0) {
            // require a clean, existent DNS dir when depth == 0
            try {
                forceEmptyDir(dnsDir);
            } catch (IOException e) {
                LOG.error("Failed while cleaning dnsDir " + dnsDir, e);
            }
        }
        return new DnsChange() {
            @Override
            public void addRecord(final DomainMapping domain, final String ip) throws IOException, InterruptedException {
                ensureDnsTx(dnsDir, depth);
                dnsExec(dnsDir, Arrays.asList(
                        "gcloud", "dns", "record-sets", "transaction", "add", ip, "--project=" + getGoogleProject(),
                        "--name=" + domain.getDomainQualified() + ".", "--type=A", "--ttl=300", "--zone=" + getDnsZone()
                ));
            }

            @Override
            public void removeRecord(final DomainMapping domain, final String oldIp) throws IOException, InterruptedException {
                ensureDnsTx(dnsDir, depth);
                String dom = domain.getDomainQualified();
                Execute.ExecutionResult result = Execute.executeNoFail(
                        "gcloud", "dns", "record-sets", "list", "--project=" + getGoogleProject(),
                        "--name=" + dom + ".", "--type=A", "--zone=" + getDnsZone(), "--format=value(ttl)"
                );
                String ttl = result.out.trim();
                if (ttl.isEmpty()) {
                    LOG.warn("Tried to remove non-existent DNS record " + dom + " -> " + oldIp);
                    warnResult(result);
                    return;
                }
                LOG.infof("Removing dns entry %s w/ ip %s", dom, oldIp);
                List<String> cmdList = Arrays.asList( "gcloud", "dns", "record-sets", "transaction", "remove", oldIp,
                        "--project=" + getGoogleProject(), "--name=" + dom + ".", "--type", "A", "--ttl", ttl, "--zone=" + getDnsZone()
                );
                dnsExec(dnsDir, cmdList);
            }

            private void ensureDnsTx(final File dnsDir, final int depth) throws IOException, InterruptedException {
                dnsThread.updateAndGet(is-> {
                    if (is == null) {
                        assert depth == 0 : "Created dnsThread when depth != 0 (instead: " + depth + ")";
                        return new Thread("DNS Update Thread") {
                            @Override
                            public void run() {
                                // actually commit the DNS transaction.
                                try {
                                    executeTx(dnsDir);
                                } catch (IOException e) {
                                    LOG.error("Failed to properly cleanup after completing DNS transaction", e);
                                }
                            }
                        };
                    }
                    assert depth != 0 : "Reused dnsThread when depth == 0";
                    return is;
                });
                if (!dnsDir.isDirectory()) {
                    if (!dnsDir.mkdirs()) {
                        throw new IllegalStateException("Unable to create dns working dir " + dnsDir);
                    }
                }
                File transaction = new File(dnsDir, "transaction.yaml");
                synchronized (txDepth) {
                    if (transaction.isFile()) {
                        return;
                    }
                    dnsExec(dnsDir, Arrays.asList(
                            "gcloud", "dns", "--project", getGoogleProject(), "record-sets", "transaction", "start", "--zone=" + getDnsZone()
                    ));
                }
            }
        };
    }

    private void forceEmptyDir(final File dnsDir) throws IOException {
        if (dnsDir.isDirectory()) {
            final File[] files = dnsDir.listFiles();
            if (files != null) {
                LOG.warn("Found leftover files in dnsDir: " + dnsDir + " (use info logging to see file contents)");
                if (LOG.isInfoEnabled()) {
                    for (File file : files) {
                        LOG.info("Leftover DNS file " + file.getAbsolutePath());
                        LOG.info(FileUtils.readFileToString(file, StandardCharsets.UTF_8));
                        if (!file.delete()) {
                            LOG.warn("Unable to delete file " + file + "; isFile? " + file.isFile());
                        }
                    }

                }
            }
        } else {
            if (!dnsDir.mkdirs()) {
                LOG.warn("Unable to mkdirs on " + dnsDir);
            }
        }
    }

    private void executeTx(final File dnsDir) throws IOException {
        if (!dnsDir.isDirectory()) {
            return;
        }
        if (!new File(dnsDir, "transaction.yaml").isFile()) {
            LOG.error("NO transaction.yaml FOUND IN " + dnsDir);
            return;
        }
        try {
            // we don't use dnsExec here, b/c this is the final operation on any transaction,
            // and we'll be deleting all our yaml once we're done, so no need to lock.
            // i.e. this can only be called AFTER all invocations of DnsManager.tx() have completed.
            Execute.executeNoFail(Arrays.asList(
                    "gcloud", "dns", "--project", getGoogleProject(), "record-sets", "transaction", "execute", "--zone=" + getDnsZone()
            ), new HashMap<>(), dnsDir, null, null, null);
        } catch (Exception e) {
            LOG.error("Failed to update DNS; listing all dns-related files and their contents, for debugging:");
            File[] files = dnsDir.listFiles();
            if (files != null) {

                for (File file : files) {
                    LOG.error("");
                    LOG.error("");
                    LOG.error(file.getAbsolutePath());
                    LOG.error("->");
                    LOG.error(FileUtils.readFileToString(file, StandardCharsets.UTF_8.name()));
                }
            }
            LOG.error("Done listing dns files; printing error", e);
        } finally {
            FileUtils.deleteDirectory(dnsDir);
        }
    }

    private void dnsExec(final File dnsDir, final List<String> cmdList) throws IOException, InterruptedException {
        synchronized (txDepth) {
            Execute.executeNoFail(cmdList, new HashMap<>(), dnsDir, null, null, null);
        }
    }
}
