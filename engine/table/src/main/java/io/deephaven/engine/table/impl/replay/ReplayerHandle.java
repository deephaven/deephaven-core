/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.replay;

import java.io.Serializable;

/**
 * A handle to a replayer.
 */
public interface ReplayerHandle extends Serializable {
    /**
     * Gets the replayer.
     *
     * @return replayer
     */
    Replayer getReplayer();

    /**
     * A handle to a replayer, where the replayer and handle exist in the same JVM instance.
     */
    public static class Local implements ReplayerHandle {
        private final Replayer replayer;

        /**
         * Create a new handle.
         *
         * @param replayer replayer
         */
        public Local(Replayer replayer) {
            this.replayer = replayer;
        }

        @Override
        public Replayer getReplayer() {
            return replayer;
        }
    }
}
