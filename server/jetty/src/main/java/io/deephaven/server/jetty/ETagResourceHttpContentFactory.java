//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.http.content.*;
import org.eclipse.jetty.util.resource.MountedPathResource;
import org.eclipse.jetty.util.resource.Resource;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class ETagResourceHttpContentFactory extends ResourceHttpContentFactory {
    private final Resource baseResource;

    public ETagResourceHttpContentFactory(Resource baseResource, MimeTypes mimeTypes) {
        super(baseResource, mimeTypes);
        this.baseResource = baseResource;
    }

    @Override
    public HttpContent getContent(String pathInContext) throws IOException {
//        System.out.println("[TESTING] baseResource: " + baseResource.getPath());
//        pathInContext = pathInContext.replace("/js-plugins", "");
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
                System.out.println("[TESTING] Returning ETag: " + fullPath + ": " + etag);
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
}
