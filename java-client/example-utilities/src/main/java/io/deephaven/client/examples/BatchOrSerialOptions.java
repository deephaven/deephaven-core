//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.TableHandleManager;
import io.deephaven.client.impl.TableService;
import picocli.CommandLine.Option;

/**
 * Whether table operations are sent as one batch ({@code --batch}) or one at a time ({@code --serial}).
 */
public class BatchOrSerialOptions {

    @Option(names = {"-b", "--batch"}, required = true, description = "Batch mode")
    boolean batch;

    @Option(names = {"-s", "--serial"}, required = true, description = "Serial mode")
    boolean serial;

    public boolean isBatch() {
        return batch;
    }

    /**
     * The manager for the given mode, or the service's own default when {@code mode} is null, as picocli leaves an
     * absent argument group.
     */
    public static TableHandleManager manager(BatchOrSerialOptions mode, TableService tableService) {
        if (mode == null) {
            return tableService;
        }
        return mode.batch ? tableService.batch() : tableService.serial();
    }
}
