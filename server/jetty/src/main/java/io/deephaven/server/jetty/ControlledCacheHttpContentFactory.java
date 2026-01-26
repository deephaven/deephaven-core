//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;

import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.http.content.FileMappingHttpContentFactory;
import org.eclipse.jetty.http.content.HttpContent;
import org.eclipse.jetty.http.content.PreCompressedHttpContentFactory;
import org.eclipse.jetty.http.content.ResourceHttpContentFactory;
import org.eclipse.jetty.http.content.ValidatingCachingHttpContentFactory;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.util.resource.Resource;

/**
 * A custom {@link HttpContent.Factory} that creates {@link ControlledCacheHttpContent} instances.
 */
public class ControlledCacheHttpContentFactory extends ResourceHttpContentFactory {
    // Copied from ResourceHandler as of Jetty 12.1.5
    private static final int DEFAULT_BUFFER_SIZE = 32768;
    private static final boolean DEFAULT_USE_DIRECT_BUFFERS = true;

    /**
     * Creates a {@link HttpContent.Factory} using a similar methodology used in
     * {@link org.eclipse.jetty.server.handler.ResourceHandler#newHttpContentFactory(ByteBufferPool.Sized)} except that
     * we use {@link ControlledCacheHttpContentFactory} instead of {@link ResourceHttpContentFactory} as the innermost
     * factory, and we don't include the {@link org.eclipse.jetty.http.content.VirtualHttpContentFactory}.
     *
     * @param baseResource the base Resource
     * @param byteBufferPool the ByteBufferPool for {@link ValidatingCachingHttpContentFactory}
     * @param mimeTypes the MimeTypes
     * @return the wrapped {@link HttpContent.Factory}
     */
    public static HttpContent.Factory create(
            Resource baseResource,
            ByteBufferPool byteBufferPool,
            MimeTypes mimeTypes) {
        // Use ControlledCacheHttpContentFactory instead of ResourceHttpContentFactory
        ByteBufferPool.Sized sizedPool =
                new ByteBufferPool.Sized(byteBufferPool, DEFAULT_USE_DIRECT_BUFFERS, DEFAULT_BUFFER_SIZE);
        HttpContent.Factory contentFactory = new ControlledCacheHttpContentFactory(baseResource, mimeTypes, sizedPool);
        contentFactory = new FileMappingHttpContentFactory(contentFactory);
        contentFactory = new PreCompressedHttpContentFactory(contentFactory, new ArrayList<>());
        contentFactory = new ValidatingCachingHttpContentFactory(contentFactory, Duration.ofSeconds(1).toMillis(),
                sizedPool);

        return contentFactory;
    }

    private ControlledCacheHttpContentFactory(Resource baseResource, MimeTypes mimeTypes,
            ByteBufferPool.Sized sizedBufferPool) {
        super(baseResource, mimeTypes, sizedBufferPool);
    }

    @Override
    public HttpContent getContent(String pathInContext) throws IOException {
        HttpContent content = super.getContent(pathInContext);

        if (content == null) {
            return null;
        }

        if (content.getResource().isDirectory()) {
            return content;
        }

        return new ControlledCacheHttpContent(content);
    }
}
