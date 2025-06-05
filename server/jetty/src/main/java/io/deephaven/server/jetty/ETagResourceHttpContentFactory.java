//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import org.eclipse.jetty.http.MimeTypes;
import org.eclipse.jetty.http.content.*;
import org.eclipse.jetty.util.resource.Resource;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class ETagResourceHttpContentFactory extends ResourceHttpContentFactory {
    public ETagResourceHttpContentFactory(Resource baseResource, MimeTypes mimeTypes) {
        super(baseResource, mimeTypes);
    }

    @Override
    public HttpContent getContent(String pathInContext) throws IOException {
        HttpContent content = super.getContent(pathInContext);
        if (content == null) {
            System.out.println("[TESTING] Skipping null: " + pathInContext);
            return null;
        }

        if (content.getResource().isDirectory()) {
            return content;
        }

        System.out.println("[TESTING] Computing etag for " + pathInContext);

        // Compute strong ETag based on content
        String etag = computeStrongETag(content);
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
}
