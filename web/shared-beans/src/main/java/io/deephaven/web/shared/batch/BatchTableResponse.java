package io.deephaven.web.shared.batch;

import io.deephaven.web.shared.data.TableHandle;
import io.deephaven.web.shared.fu.JsFunction;

import java.io.Serializable;
import java.util.Arrays;

/**
 * A response object for batch requests; only contains failure messages, since successful results will be pushed
 * directly the client as they come in (rather than waiting until entire batch is complete).
 *
 * Seems like we could do {@code Callback<Void, BatchFailures>} instead...
 */
public class BatchTableResponse implements Serializable {
    // not using a constant TableHandle[0], as arrays are fully mutable in JS
    private TableHandle[] success = new TableHandle[0];

    private TableHandle[] failedTableHandles;
    private String[] failureMessages;

    public TableHandle[] getSuccess() {
        return success;
    }

    public void setSuccess(TableHandle[] success) {
        this.success = success;
    }

    public TableHandle[] getFailedTableHandles() {
        return failedTableHandles;
    }

    public void setFailedTableHandles(TableHandle[] failedTableHandles) {
        this.failedTableHandles = failedTableHandles;
    }

    public String[] getFailureMessages() {
        return failureMessages;
    }

    public void setFailureMessages(String[] failureMessages) {
        this.failureMessages = failureMessages;
    }

    @Override
    public String toString() {
        return "BatchTableResponse{" +
                "success=" + Arrays.toString(success) +
                ", failedTableHandles=" + Arrays.toString(failedTableHandles) +
                ", failureMessages=" + Arrays.toString(failureMessages) +
                '}';
    }

    public boolean hasFailures(TableHandle handle) {
        return hasFailures(handle::equals);
    }

    public boolean hasFailures(JsFunction<TableHandle, Boolean> table) {
        for (TableHandle handle : failedTableHandles) {
            if (table.apply(handle)) {
                return true;
            }
        }
        return false;

    }

    public String getFailure(TableHandle handle) {
        for (int i = 0; i < failedTableHandles.length; i++) {
            if (handle.equals(failedTableHandles[i])) {
                return failureMessages[i];
            }
        }
        return null;
    }

    public void addSuccess(TableHandle newId) {
        assert Arrays.stream(success).noneMatch(newId::equals);
        // this is terrible and all, but we want to avoid all java collections in DTOs.
        // we can have a response builder w/ a proper collection ...later.
        success = Arrays.copyOf(success, success.length + 1);

        success[success.length - 1] = newId;
    }
}
