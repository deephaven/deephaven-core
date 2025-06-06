//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import org.eclipse.jetty.http.content.HttpContent;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.util.resource.Resource;

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
        return ETagResourceHttpContentFactory.create(
                getBaseResource(),
                getByteBufferPool(),
                getMimeTypes(),
                getStyleSheet(),
                getPrecompressedFormats(),
                isUseFileMapping()
        );
    }
}
