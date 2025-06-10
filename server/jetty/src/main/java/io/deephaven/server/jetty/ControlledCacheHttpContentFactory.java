//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import java.io.IOException;
import java.util.List;

import org.eclipse.jetty.http.CompressedContentFormat;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.http.content.FileMappingHttpContentFactory;
import org.eclipse.jetty.http.content.HttpContent;
import org.eclipse.jetty.http.content.PreCompressedHttpContentFactory;
import org.eclipse.jetty.http.content.ResourceHttpContentFactory;
import org.eclipse.jetty.http.content.ValidatingCachingHttpContentFactory;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.util.resource.Resource;

/**
 * A custom `HttpContent.Factory` that creates `ControlledCacheHttpContent` instances.
 */
public class ControlledCacheHttpContentFactory extends ResourceHttpContentFactory {
    /**
     * Creates a `HttpContent.Factory` using a similar methodology used in `ResourceHandler.newHttpContentFactory()`
     * except that we use `ControlledCacheHttpContentFactory` instead of `ResourceHttpContentFactory` as the innermost
     * factory, and we don't include the `VirtualHttpContentFactory`.
     * 
     * @param baseResource the base Resource
     * @param byteBufferPool the ByteBufferPool for ValidatingCachingHttpContentFactory
     * @param mimeTypes the MimeTypes
     * @param preCompressedFormats formats for PreCompressedHttpContentFactory
     * @param useFileMapping whether to use FileMappingHttpContentFactory
     * @return the wrapped HttpContent.Factory
     */
    public static HttpContent.Factory create(
            Resource baseResource,
            ByteBufferPool byteBufferPool,
            MimeTypes mimeTypes,
            List<CompressedContentFormat> preCompressedFormats,
            boolean useFileMapping) {
        // Use `ControlledCacheHttpContentFactory` instead of `ResourceHttpContentFactory`
        HttpContent.Factory contentFactory = new ControlledCacheHttpContentFactory(baseResource, mimeTypes);

        if (useFileMapping) {
            contentFactory = new FileMappingHttpContentFactory(contentFactory);
        }
        contentFactory = new PreCompressedHttpContentFactory(contentFactory, preCompressedFormats);
        contentFactory = new ValidatingCachingHttpContentFactory(contentFactory,
                java.time.Duration.ofSeconds(1).toMillis(), byteBufferPool);

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
