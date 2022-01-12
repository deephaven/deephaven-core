package io.deephaven.plugin;

/**
 * A base registration which enforces the restrictions on {@link #init()} and {@link #registerInto(Callback)}.
 */
public abstract class RegistrationBase implements Registration {

    private boolean initialized;

    protected abstract void initImpl();

    protected abstract void registerIntoImpl(Callback callback);

    @Override
    public final synchronized void init() {
        if (initialized) {
            throw new IllegalStateException("The registration is already initialized");
        }
        initialized = true;
        initImpl();
    }

    @Override
    public final synchronized void registerInto(Callback callback) {
        if (!initialized) {
            throw new IllegalStateException("The registration must be initialized first");
        }
        registerIntoImpl(callback);
    }
}
