package io.deephaven.plugin;

/**
 * The registration interface for plugins.
 */
public interface Registration {

    /**
     * The registration entrypoint.
     *
     * <p>
     * May be called multiple times.
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
