//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import org.eclipse.jetty.http.CompressedContentFormat;
import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.http.content.HttpContent;
import org.eclipse.jetty.http.content.FileMappingHttpContentFactory;
import org.eclipse.jetty.http.content.PreCompressedHttpContentFactory;
import org.eclipse.jetty.http.content.ResourceHttpContentFactory;
import org.eclipse.jetty.http.content.VirtualHttpContentFactory;
import org.eclipse.jetty.http.content.ValidatingCachingHttpContentFactory;
import org.eclipse.jetty.io.ByteBufferPool;
import org.eclipse.jetty.util.resource.MountedPathResource;
import org.eclipse.jetty.util.resource.Resource;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;

public class ETagResourceHttpContentFactory extends ResourceHttpContentFactory {
    private final Resource baseResource;

    public ETagResourceHttpContentFactory(Resource baseResource, MimeTypes mimeTypes) {
        super(baseResource, mimeTypes);
        this.baseResource = baseResource;
    }

    @Override
    public HttpContent getContent(String pathInContext) throws IOException {
        String containerPath = "";

        if (baseResource instanceof MountedPathResource) {
            containerPath = ((MountedPathResource) baseResource).getContainerPath().getFileName().toString();
        }
        else if (baseResource instanceof ControlledCacheResource) {
            containerPath = baseResource.getPath().toString();
        }

        String fullPath = Paths.get(containerPath, pathInContext).normalize().toString();

        HttpContent content = super.getContent(pathInContext);
        if (content == null) {
            System.out.println("[TESTING] Skipping null: " + fullPath);
            return null;
        }

        if (content.getResource().isDirectory()) {
            return content;
        }

        // Compute strong ETag based on content
        String etag = computeStrongETag(content);

        System.out.println("[TESTING] ETag created for " + fullPath + ": " + etag);

        return new HttpContent.Wrapper(content) {
            @Override
            public String getETagValue() {
                return etag;
            }
        };
    }

    private String computeStrongETag(HttpContent content) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            try (var input = content.getResource().newInputStream()) {
                int read;
                while ((read = input.read(buffer)) != -1) {
                    digest.update(buffer, 0, read);
                }
            }
            return "\"" + Base64.getEncoder().encodeToString(digest.digest()) + "\"";
        } catch (NoSuchAlgorithmException e) {
            throw new IOException("SHA-256 not supported", e);
        }
    }

    /**
     * Creates a `HttpContent.Factory` using the same methodology used in `ResourceHandler.newHttpContentFactory()`
     * except that we use `ETagResourceHttpContentFactory` instead of `ResourceHttpContentFactory` as the innermost
     * factory.
     * @param baseResource the base Resource
     * @param byteBufferPool the ByteBufferPool for ValidatingCachingHttpContentFactory
     * @param mimeTypes the MimeTypes
     * @param styleSheet the stylesheet Resource for VirtualHttpContentFactory
     * @param preCompressedFormats formats for PreCompressedHttpContentFactory
     * @param useFileMapping whether to use FileMappingHttpContentFactory
     * @return the wrapped HttpContent.Factory
     */
    public static HttpContent.Factory create(
            Resource baseResource,
            ByteBufferPool byteBufferPool,
            MimeTypes mimeTypes,
            Resource styleSheet,
            List<CompressedContentFormat> preCompressedFormats,
            boolean useFileMapping
    ) {
        // Use ETagResourceHttpContentFactory instead of ResourceHttpContentFactory
        HttpContent.Factory contentFactory = new ETagResourceHttpContentFactory(baseResource, mimeTypes);

        if (useFileMapping) {
            contentFactory = new FileMappingHttpContentFactory(contentFactory);
        }
        contentFactory = new VirtualHttpContentFactory(contentFactory, styleSheet, "text/css");
        contentFactory = new PreCompressedHttpContentFactory(contentFactory, preCompressedFormats);
        contentFactory = new ValidatingCachingHttpContentFactory(contentFactory, java.time.Duration.ofSeconds(1).toMillis(), byteBufferPool);
        return contentFactory;
    }
}
