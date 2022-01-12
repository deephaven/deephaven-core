package io.deephaven.plugin;

/**
 * The registration interface for plugins.
 */
public interface Registration {

    /**
     * Initialize the registration.
     *
     * <p>
     * Should not be called more than once.
     */
    void init();

    /**
     * The registration entrypoint.
     *
     * <p>
     * May be called multiple times.
     *
     * <p>
     * The registration must be {@link #init() initialized} before calling this.
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
