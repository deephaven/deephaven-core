//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import java.util.Set;

public final class S3Constants {
    public static final String S3_URI_SCHEME = "s3";
    public static final String S3A_URI_SCHEME = "s3a";
    public static final String S3N_URI_SCHEME = "s3n";
    public static final Set<String> S3_SCHEMES = Set.of(S3_URI_SCHEME, S3A_URI_SCHEME, S3N_URI_SCHEME);
}
