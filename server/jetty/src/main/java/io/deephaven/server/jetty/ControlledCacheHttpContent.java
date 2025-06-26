//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;

import com.google.common.hash.Funnels;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import org.eclipse.jetty.http.DateGenerator;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.content.HttpContent;

/**
 * A custom {@link HttpContent} implementation for finer control over caching behavior.
 * <p>
 * <ul>
 * <li>Calculates a strong ETag based on the content's SHA-256 hash.</li>
 * <li>Overrides all last modified related methods to signal that the content does not have a last modified time. This
 * is needed since we don't use last modified timestamps inside of our .jar files.</li>
 * </ul>
 */
public class ControlledCacheHttpContent extends HttpContent.Wrapper {

    public ControlledCacheHttpContent(HttpContent content) throws IOException {
        super(content);
        this.eTag = computeStrongETag(content);
    }

    private final String eTag;

    private String computeStrongETag(HttpContent content) throws IOException {
        try (InputStream input = content.getResource().newInputStream()) {
            Hasher hasher = Hashing.sha256().newHasher();
            ByteStreams.copy(input, Funnels.asOutputStream(hasher));
            return "\"" + hasher.hash() + "\"";
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
