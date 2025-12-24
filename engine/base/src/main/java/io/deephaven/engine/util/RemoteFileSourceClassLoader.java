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

    public RemoteFileSourceClassLoader(ClassLoader parent) {
        super(parent);
        instance = this;
    }

    public static RemoteFileSourceClassLoader getInstance() {
        return instance;
    }

    public void registerProvider(RemoteFileSourceProvider provider) {
        providers.add(provider);
    }

    public void unregisterProvider(RemoteFileSourceProvider provider) {
        providers.remove(provider);
    }

    @Override
    protected URL findResource(String name) {
        for (RemoteFileSourceProvider provider : providers) {
            if (!provider.isActive()) {
                continue;
            }
            try {
                if (provider.canSourceResource(name)) {
                    return new URL(null, "remotefile://" + name, new RemoteFileURLStreamHandler(provider, name));
                }
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

        RemoteFileURLStreamHandler(RemoteFileSourceProvider provider, String resourceName) {
            this.provider = provider;
            this.resourceName = resourceName;
        }

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

        RemoteFileURLConnection(URL url, RemoteFileSourceProvider provider, String resourceName) {
            super(url);
            this.provider = provider;
            this.resourceName = resourceName;
        }

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
