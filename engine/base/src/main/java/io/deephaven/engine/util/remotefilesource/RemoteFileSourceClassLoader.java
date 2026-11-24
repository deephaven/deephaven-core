//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.remotefilesource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A custom ClassLoader that fetches source files from remote clients via a {@link RemoteFileSourceProvider}. This is
 * designed to support Groovy script imports where the source files are provided by remote clients.
 *
 * <p>
 * Sourcing is scoped to a single script evaluation. A client declares the resources it will serve via
 * {@link #declareRemoteSources}, and {@link #beginEvaluation()} consumes that declaration for the run that is about to
 * start. A run that was not preceded by a declaration therefore sources nothing, even while another client is
 * connected, and a declaration arriving mid-run cannot retarget the run already underway.
 *
 * <p>
 * When a resource is requested (e.g., for a Groovy import), this class loader:
 * <ol>
 * <li>Checks the evaluation's declaration to see whether the resource should be sourced remotely</li>
 * <li>Returns a custom URL with protocol "remotefile://" if it should</li>
 * <li>When that URL is opened, fetches the resource bytes from the declaring provider</li>
 * </ol>
 *
 * <p>
 * All declaration state is guarded by this loader's monitor. Resource bytes are fetched outside it, so a slow or stuck
 * provider never blocks declarations, evaluations or other lookups. The loader is registered as parallel capable, so
 * class loading locks per class name rather than on this monitor.
 */
public class RemoteFileSourceClassLoader extends ClassLoader {
    static {
        registerAsParallelCapable();
    }

    private static final long RESOURCE_TIMEOUT_SECONDS = 5;

    private static volatile RemoteFileSourceClassLoader instance;

    /**
     * The most recent declaration, awaiting the evaluation it was made for. Consumed by {@link #beginEvaluation()}.
     */
    private RemoteFileSourceDeclaration pendingDeclaration;

    /**
     * The declaration serving the evaluation currently underway, or null if that evaluation sources nothing remotely.
     */
    private RemoteFileSourceDeclaration evaluationDeclaration;

    /**
     * Constructs a new RemoteFileSourceClassLoader with the specified parent class loader.
     *
     * @param parent the parent class loader for delegation
     */
    private RemoteFileSourceClassLoader(ClassLoader parent) {
        super(parent);
    }

    /**
     * Initializes the singleton RemoteFileSourceClassLoader instance with the specified parent class loader.
     *
     * <p>
     * This method must be called exactly once before any calls to {@link #getInstance()}. The method is synchronized to
     * prevent race conditions when multiple threads attempt initialization.
     *
     * @param parent the parent class loader for delegation
     * @return the newly created singleton instance
     * @throws IllegalStateException if the instance has already been initialized
     */
    public static synchronized RemoteFileSourceClassLoader initialize(ClassLoader parent) {
        if (instance != null) {
            throw new IllegalStateException("RemoteFileSourceClassLoader is already initialized");
        }

        instance = new RemoteFileSourceClassLoader(parent);
        return instance;
    }

    /**
     * Returns the singleton instance of the RemoteFileSourceClassLoader.
     *
     * <p>
     * This method requires that {@link #initialize(ClassLoader)} has been called first.
     *
     * @return the singleton instance
     * @throws IllegalStateException if the instance has not yet been initialized via {@link #initialize(ClassLoader)}
     */
    public static RemoteFileSourceClassLoader getInstance() {
        if (instance == null) {
            throw new IllegalStateException("RemoteFileSourceClassLoader is not yet initialized");
        }
        return instance;
    }

    /**
     * Declares the resources a client will serve for its next script evaluation, replacing any declaration not yet
     * consumed. Clients declare before each run; the declaration is claimed by whichever evaluation begins next.
     *
     * @param provider the provider that will service resource requests
     * @param resourcePaths resource paths (e.g., "package/MyScript.groovy") to resolve from the provider
     * @param dirty whether the remote sources have changed and caches must be cleared
     */
    public synchronized void declareRemoteSources(final RemoteFileSourceProvider provider,
            final List<String> resourcePaths,
            final boolean dirty) {
        pendingDeclaration = new RemoteFileSourceDeclaration(provider, resourcePaths, dirty);
    }

    /**
     * Claims the pending declaration, if any, for the evaluation that is about to start, and reports whether the
     * sources it resolves against differ from the previous evaluation's. Must be called before every evaluation,
     * including those with no declaration to claim, since claiming nothing is what makes such a run resolve locally.
     *
     * <p>
     * The result is true when the claimed declaration is dirty, meaning the client's sources changed, or when a
     * different client is serving this evaluation - a client's dirty flag describes only its own sources, and says
     * nothing about classes compiled from another client's.
     *
     * @return true if compiled output from the previous evaluation must be discarded
     */
    public synchronized boolean beginEvaluation() {
        final RemoteFileSourceDeclaration claimed = pendingDeclaration;
        pendingDeclaration = null;
        // A declaration with no paths serves nothing, so it is no different from not having declared
        final RemoteFileSourceDeclaration declaration =
                claimed != null && claimed.hasConfiguredResources() ? claimed : null;
        final RemoteFileSourceProvider provider = declaration != null ? declaration.getProvider() : null;
        final RemoteFileSourceProvider previousProvider =
                evaluationDeclaration != null ? evaluationDeclaration.getProvider() : null;

        final boolean sourcingChanged = (claimed != null && claimed.isDirty()) || provider != previousProvider;
        evaluationDeclaration = declaration;

        return sourcingChanged;
    }

    /**
     * Discards any declaration the given provider has made but that no evaluation has claimed yet, for use when its
     * connection closes. A provider that has since been superseded leaves the newer declaration untouched.
     *
     * <p>
     * A declaration already claimed is deliberately left in place. An evaluation underway keeps resolving the paths it
     * claimed, so the fetches fail against the closed provider rather than silently falling through to same-named
     * classpath resources, which would let one run compile sources of mixed origin.
     *
     * @param provider the provider whose unclaimed declaration should be dropped
     */
    public synchronized void providerClosed(final RemoteFileSourceProvider provider) {
        if (pendingDeclaration != null && pendingDeclaration.getProvider() == provider) {
            pendingDeclaration = null;
        }
    }

    /**
     * Returns whether the evaluation underway claimed a declaration with resource paths.
     *
     * @return true if this evaluation sources any resources remotely, false otherwise
     */
    public synchronized boolean hasConfiguredRemoteSources() {
        return evaluationDeclaration != null;
    }

    /**
     * Gets the resource with the specified name, sourcing it remotely when this evaluation declared it.
     *
     * <p>
     * This method consults the declaration claimed for the evaluation underway. If it covers the requested resource, a
     * custom URL with protocol "remotefile://" is returned. Otherwise the request is delegated to the parent class
     * loader.
     *
     * @param name the resource name
     * @return a URL for reading the resource, or null if the resource could not be found
     */
    @Override
    public URL getResource(String name) {
        // Snapshot the declaration so that resolution and the later fetch through the returned URL agree, even if
        // the next evaluation begins in between. The fetch itself happens outside the lock.
        final RemoteFileSourceDeclaration declaration;
        synchronized (this) {
            declaration = evaluationDeclaration;
        }
        final RemoteFileSourceProvider provider =
                declaration != null && declaration.canSourceResource(name) ? declaration.getProvider() : null;

        if (provider != null) {
            try {
                return new URL(null, "remotefile://" + name, new RemoteFileURLStreamHandler(provider, name));
            } catch (MalformedURLException e) {
                // Fall through to parent if URL creation fails
            }
        }

        return super.getResource(name);
    }

    /**
     * URLStreamHandler that delegates to a RemoteFileSourceProvider to fetch resource bytes.
     */
    private static class RemoteFileURLStreamHandler extends URLStreamHandler {
        private final RemoteFileSourceProvider provider;
        private final String resourceName;

        /**
         * Constructs a new RemoteFileURLStreamHandler for the specified provider and resource.
         *
         * @param provider the provider that will source the resource
         * @param resourceName the name of the resource to fetch
         */
        RemoteFileURLStreamHandler(RemoteFileSourceProvider provider, String resourceName) {
            this.provider = provider;
            this.resourceName = resourceName;
        }

        /**
         * Opens a connection to the resource referenced by this URL.
         *
         * @param url the URL to open a connection to
         * @return a URLConnection to the specified URL
         */
        @Override
        protected URLConnection openConnection(URL url) {
            return new RemoteFileURLConnection(url, provider, resourceName);
        }
    }

    /**
     * URLConnection that fetches resource bytes from a RemoteFileSourceProvider.
     */
    private static class RemoteFileURLConnection extends URLConnection {
        private final RemoteFileSourceProvider provider;
        private final String resourceName;
        private byte[] content;

        /**
         * Constructs a new RemoteFileURLConnection for the specified URL, provider, and resource.
         *
         * @param url the URL to connect to
         * @param provider the provider that will source the resource
         * @param resourceName the name of the resource to fetch
         */
        RemoteFileURLConnection(URL url, RemoteFileSourceProvider provider, String resourceName) {
            super(url);
            this.provider = provider;
            this.resourceName = resourceName;
        }

        /**
         * Opens a connection to the resource by requesting it from the provider.
         *
         * <p>
         * This method fetches the resource bytes from the provider with a timeout of {@value #RESOURCE_TIMEOUT_SECONDS}
         * seconds. If already connected, this method does nothing.
         *
         * @throws IOException if the connection fails or times out
         */
        @Override
        public void connect() throws IOException {
            if (!connected) {
                try {
                    content = provider.requestResource(resourceName)
                            .orTimeout(RESOURCE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                            .get();
                    connected = true;
                } catch (Exception e) {
                    throw new IOException("Failed to fetch remote resource: " + resourceName, e);
                }
            }
        }

        /**
         * Returns an input stream that reads from this connection's resource.
         *
         * <p>
         * This method calls {@link #connect()} to ensure the connection is established and resource bytes are fetched
         * from the provider. Empty content is a legitimately empty file and is returned as an empty stream.
         *
         * @return an input stream that reads from the fetched resource bytes
         * @throws IOException if the connection or content download fails, including when the provider does not have
         *         the resource
         */
        @Override
        public InputStream getInputStream() throws IOException {
            connect();
            if (content == null) {
                throw new IOException("Provider returned null for remote resource: " + resourceName);
            }
            return new ByteArrayInputStream(content);
        }
    }
}
