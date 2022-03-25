package io.deephaven.demo.deploy;

import io.deephaven.demo.ClusterController;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * GcloudApiRequest:
 * <p>
 * <p>
 */
abstract class GcloudApiRequest {

    private static final Logger LOG = Logger.getLogger(GcloudApiRequest.class);

    private final String host;
    private final List<BiConsumer<Execute.ExecutionResult, Throwable>> doneList;
    private int tries;
    private boolean started;

    public GcloudApiRequest(final String host, final BiConsumer<Execute.ExecutionResult, Throwable> onDone) {
        this.host = host;
        tries = getMaxTries();
        doneList = new ArrayList<>();
        doneList.add(onDone);
        doneList.add(onDone);
    }

    public String getHost() {
        return host;
    }

    public boolean isStarted() {
        return started;
    }

    public void addDoneCallback(final BiConsumer<Execute.ExecutionResult, Throwable> callback) {
        doneList.add(callback);
    }

    public void setStarted(final boolean started) {
        this.started = started;
    }

    protected abstract Object getSynchroObject();

    public void schedule(final String threadName) {
        ClusterController.setTimer(threadName, () -> {
            final String[] args;
            synchronized (getSynchroObject()) {
                // once we've started working, any more label requests will need to be queued into a new instance
                setStarted(true);
                args = getArgs();
            }
            tryCommit(args);
        });
    }

    protected abstract String[] getArgs();

    private void tryCommit(String[] args) {
        try {
            final Execute.ExecutionResult result = doRequest(args);
            succeed(args, result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(args, e);
        } catch (IOException e) {
            fail(args, e);
        } catch (Throwable t) {
            fail(args, t);
            if (t instanceof Error) {
                throw t;
            }
        }
    }

    protected Execute.ExecutionResult doRequest(final String... args) throws IOException, InterruptedException {
        return GoogleDeploymentManager.gcloud(true, args);
    }

    private void succeed(final String[] args, final Execute.ExecutionResult result) {
        final BiConsumer<Execute.ExecutionResult, Throwable>[] items;
        if (result.code == 0) {
            synchronized (doneList) {
                //noinspection unchecked
                items = doneList.toArray(new BiConsumer[0]);
                doneList.clear();
            }
            for (BiConsumer<Execute.ExecutionResult, Throwable> callback : items) {
                try {
                    callback.accept(result, null);
                } catch (Throwable t) {
                    LOG.warnf(t, "Response callback threw exception after processing execution result %s", result);
                }
            }

        } else {
            LOG.warnf("Request had non-zero code %s", result.code);
            GoogleDeploymentManager.warnResult(result);
            fail(args, new IllegalStateException(String.format(
                    "Response had code %s", result.code
            )));
        }
    }

    private void fail(final String[] args, final Throwable e) {
        final BiConsumer<Execute.ExecutionResult, Throwable>[] items;
        // in case we get called more than once, be picky about clearing out callbacks.
        if (tries == 0) {
            // make sure any last-minute threads get their request in before we close up shop
            synchronized (doneList) {
                //noinspection unchecked
                items = doneList.toArray(new BiConsumer[0]);
                doneList.clear();
            }
            for (BiConsumer<Execute.ExecutionResult, Throwable> item : items) {
                item.accept(null, e);
            }
        } else {
            final String title = "Add labels to " + host;
            if (tries > 0) {
                ClusterController.setTimer(title, () -> tryCommit(args));
            }
            tries--;
            LOG.infof("Attempting %s job again (tries remaining: %s)", title, tries);
        }
    }

    private int getMaxTries() {
        return 4;
    }
}
