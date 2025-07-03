//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
    /**
     * Creates a {@link HttpContent.Factory} using a similar methodology used in
     * {@link org.eclipse.jetty.server.handler.ResourceHandler#newHttpContentFactory()} except that we use
     * {@link ControlledCacheHttpContentFactory} instead of {@link ResourceHttpContentFactory} as the innermost factory,
     * and we don't include the {@link org.eclipse.jetty.http.content.VirtualHttpContentFactory}.
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
        HttpContent.Factory contentFactory = new ControlledCacheHttpContentFactory(baseResource, mimeTypes);
        contentFactory = new FileMappingHttpContentFactory(contentFactory);
        contentFactory = new PreCompressedHttpContentFactory(contentFactory, new ArrayList<>());
        contentFactory = new ValidatingCachingHttpContentFactory(contentFactory, Duration.ofSeconds(1).toMillis(),
                byteBufferPool);

        return contentFactory;
    }

    private ControlledCacheHttpContentFactory(Resource baseResource, MimeTypes mimeTypes) {
        super(baseResource, mimeTypes);
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
