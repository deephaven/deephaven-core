package io.deephaven.plugin;

/**
 * The registration interface for plugins.
 */
public interface Registration {

    /**
     * The registration entrypoint.
     *
     * @param callback the callback.
     */
    void registerInto(Callback callback);

    interface Callback {

        /**
         * Registers {@code plugin}.
         *
         * @param plugin the plugin
         */
        void register(Plugin plugin);
    }
}
