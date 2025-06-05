//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import org.eclipse.jetty.http.content.HttpContent;
import org.eclipse.jetty.http.content.FileMappingHttpContentFactory;
import org.eclipse.jetty.http.content.PreCompressedHttpContentFactory;
import org.eclipse.jetty.http.content.VirtualHttpContentFactory;
import org.eclipse.jetty.http.content.ValidatingCachingHttpContentFactory;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.Resource;

import java.time.Duration;

public class ETagResourceHandler extends ResourceHandler {
    public ETagResourceHandler(Resource baseResource, Handler handler)
    {
        super(handler);
        setBaseResource(baseResource);
        setEtags(true);
    }

    @Override
    protected HttpContent.Factory newHttpContentFactory()
    {
        // Inject our custom, strong etag ResourceHttpContentFactory
        HttpContent.Factory contentFactory = new ETagResourceHttpContentFactory(getBaseResource(), getMimeTypes());

        if (isUseFileMapping())
            contentFactory = new FileMappingHttpContentFactory(contentFactory);
        contentFactory = new VirtualHttpContentFactory(contentFactory, getStyleSheet(), "text/css");
        contentFactory = new PreCompressedHttpContentFactory(contentFactory, getPrecompressedFormats());
        contentFactory = new ValidatingCachingHttpContentFactory(contentFactory, Duration.ofSeconds(1).toMillis(), getByteBufferPool());
        return contentFactory;
    }
}
