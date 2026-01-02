//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 * A custom ClassLoader that fetches source files from remote clients via registered RemoteFileSourceProvider instances.
 * This is designed to support Groovy script imports where the source files are provided by remote clients.
 *
 * <p>When a resource is requested (e.g., for a Groovy import), this class loader:
 * <ol>
 *   <li>Checks registered providers to see if they can source the resource</li>
 *   <li>Returns a custom URL with protocol "remotefile://" if a provider can handle it</li>
 *   <li>When that URL is opened, fetches the resource bytes from the provider</li>
 * </ol>
 */
public class RemoteFileSourceClassLoader extends ClassLoader {
    private static final long RESOURCE_TIMEOUT_SECONDS = 5;

    private static volatile RemoteFileSourceClassLoader instance;
    private final CopyOnWriteArrayList<RemoteFileSourceProvider> providers = new CopyOnWriteArrayList<>();

    /**
     * Constructs a new RemoteFileSourceClassLoader with the specified parent class loader.
     *
     * @param parent the parent class loader for delegation
     */
    public RemoteFileSourceClassLoader(ClassLoader parent) {
        super(parent);
        instance = this;
    }

    /**
     * Returns the singleton instance of the RemoteFileSourceClassLoader.
     *
     * @return the singleton instance, or null if not yet initialized
     */
    public static RemoteFileSourceClassLoader getInstance() {
        return instance;
    }

    /**
     * Registers a new provider that can source remote resources.
     *
     * @param provider the provider to register
     */
    public void registerProvider(RemoteFileSourceProvider provider) {
        providers.add(provider);
    }

    /**
     * Unregisters a previously registered provider.
     *
     * @param provider the provider to unregister
     */
    public void unregisterProvider(RemoteFileSourceProvider provider) {
        providers.remove(provider);
    }

    /**
     * Finds the resource with the specified name by checking registered providers.
     *
     * <p>This method iterates through all registered providers to see if any can source the requested resource.
     * If a provider can handle the resource, a custom URL with protocol "remotefile://" is returned.
     * If no provider can handle the resource, the request is delegated to the parent class loader.
     *
     * @param name the resource name
     * @return a URL for reading the resource, or null if the resource could not be found
     */
    @Override
    protected URL findResource(String name) {
        for (RemoteFileSourceProvider provider : providers) {
            if (!provider.isActive() || !provider.canSourceResource(name)) {
                continue;
            }

            try {
                return new URL(null, "remotefile://" + name, new RemoteFileURLStreamHandler(provider, name));
            } catch (java.net.MalformedURLException e) {
                // Continue to next provider if URL creation fails
            }
        }

        return super.findResource(name);
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
         * <p>This method fetches the resource bytes from the provider with a timeout of
         * {@value #RESOURCE_TIMEOUT_SECONDS} seconds. If already connected, this method does nothing.
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
         * <p>This method ensures the connection is established before returning the stream.
         *
         * @return an input stream that reads from the resource
         * @throws IOException if the connection cannot be established or if the resource has no content
         */
        @Override
        public InputStream getInputStream() throws IOException {
            connect();
            if (content == null || content.length == 0) {
                throw new IOException("No content for resource: " + resourceName);
            }
            return new ByteArrayInputStream(content);
        }
    }
}
