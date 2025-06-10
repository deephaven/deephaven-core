//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;

import org.eclipse.jetty.http.DateGenerator;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.content.HttpContent;

/**
 * A custom `HttpContent` implementation for finer control over caching behavior.
 * 
 * - Calculates a strong ETag based on the content's SHA-256 hash.
 * 
 * - Overrides all last modified related methods to signal that the content does not have a last modified time. This is
 * needed since we don't use last modified timestamps inside of our .jar files.
 */
public class ControlledCacheHttpContent extends HttpContent.Wrapper {

    public ControlledCacheHttpContent(HttpContent content) throws IOException {
        super(content);
        this.eTag = computeStrongETag(content);
    }

    private final String eTag;

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

    @Override
    public String getETagValue() {
        return eTag;
    }

    @Override
    public HttpField getLastModified() {
        // We don't have a last modified time, so return null so that no Last-Modified header is sent.
        return null;
    }

    @Override
    public Instant getLastModifiedInstant() {
        // Always return -1, so that we don't get the build system timestamp.
        return Instant.ofEpochMilli(-1);
    }

    @Override
    public String getLastModifiedValue() {
        Instant lm = getLastModifiedInstant();
        return DateGenerator.formatDate(lm);
    }
}
